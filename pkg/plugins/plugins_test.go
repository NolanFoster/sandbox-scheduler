package plugins_test

import (
	"context"
	"testing"

	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
	"github.com/NolanFoster/sandbox-scheduler/pkg/plugins"
)

var ctx = context.Background()

func TestReachableFilter(t *testing.T) {
	f := plugins.Reachable{}
	if ok, _ := f.Filter(ctx, &framework.Request{}, &framework.Candidate{Reachable: true}); !ok {
		t.Fatal("a reachable provider must pass")
	}
	ok, reason := f.Filter(ctx, &framework.Request{}, &framework.Candidate{Reachable: false})
	if ok {
		t.Fatal("an unreachable provider must be filtered")
	}
	if reason == "" {
		t.Fatal("rejection must carry a reason")
	}
}

func TestRequiredAttributes(t *testing.T) {
	f := plugins.RequiredAttributes{}
	gpu := &framework.Candidate{Attributes: map[string]string{"gpu": "true", "region": "nyc1"}}

	if ok, _ := f.Filter(ctx, &framework.Request{Requires: map[string]string{"gpu": "true"}}, gpu); !ok {
		t.Fatal("a matching attribute must pass")
	}

	// The reason must distinguish "wrong value" from "not declared at all" —
	// they point at different fixes.
	_, missing := f.Filter(ctx, &framework.Request{Requires: map[string]string{"tpu": "true"}}, gpu)
	if missing == "" || !contains(missing, "does not declare") {
		t.Fatalf("undeclared attribute should say so, got %q", missing)
	}
	_, wrong := f.Filter(ctx, &framework.Request{Requires: map[string]string{"region": "lon1"}}, gpu)
	if !contains(wrong, "has region=nyc1") {
		t.Fatalf("mismatched value should report the actual value, got %q", wrong)
	}
}

func TestRequiredAttributesIsExactMatch(t *testing.T) {
	// Fuzzy matching in a security-relevant filter is how untrusted workloads
	// end up on a provider without isolation.
	f := plugins.RequiredAttributes{}
	c := &framework.Candidate{Attributes: map[string]string{"runtime": "gvisor-experimental"}}
	if ok, _ := f.Filter(ctx, &framework.Request{Requires: map[string]string{"runtime": "gvisor"}}, c); ok {
		t.Fatal("a prefix match must not satisfy an exact requirement")
	}
}

func TestWarmCapacitySaturates(t *testing.T) {
	s := plugins.WarmCapacity{Saturation: 5}
	cases := []struct {
		warm int
		want int64
	}{
		{0, 0}, {1, 20}, {3, 60}, {5, 100},
		// Past saturation the marginal value to *this* request is nil; scoring
		// linearly would let an over-provisioned provider dominate a dimension
		// that stopped mattering and starve the cheaper one.
		{50, 100}, {5000, 100},
	}
	for _, tc := range cases {
		got := s.Score(ctx, &framework.Request{}, &framework.Candidate{WarmCapacity: tc.warm})
		if got != tc.want {
			t.Fatalf("warm=%d scored %d, want %d", tc.warm, got, tc.want)
		}
	}
}

func TestCostScoresRelativeToTheMostExpensive(t *testing.T) {
	cands := []framework.Candidate{
		{Provider: "cheap", CostPerHour: 1},
		{Provider: "dear", CostPerHour: 4},
	}
	s := plugins.NewCost(cands)
	cheap := s.Score(ctx, &framework.Request{}, &cands[0])
	dear := s.Score(ctx, &framework.Request{}, &cands[1])
	if dear != 0 {
		t.Fatalf("the most expensive candidate should score 0, got %d", dear)
	}
	if cheap != 75 {
		t.Fatalf("cheap scored %d, want 75 (1 - 1/4)", cheap)
	}
}

func TestCostIsNeutralWhenNothingIsPriced(t *testing.T) {
	// With no pricing there is no ordering to infer. Inventing one would leak
	// an arbitrary preference into every decision.
	cands := []framework.Candidate{{Provider: "a"}, {Provider: "b"}}
	s := plugins.NewCost(cands)
	for i := range cands {
		if got := s.Score(ctx, &framework.Request{}, &cands[i]); got != framework.MaxScore {
			t.Fatalf("unpriced candidate scored %d, want neutral %d", got, framework.MaxScore)
		}
	}
}

func TestAffinityPrefersButDoesNotPin(t *testing.T) {
	s := plugins.Affinity{}
	req := &framework.Request{PreferProvider: "civo"}
	if got := s.Score(ctx, req, &framework.Candidate{Provider: "civo"}); got != framework.MaxScore {
		t.Fatalf("the preferred provider should score max, got %d", got)
	}
	if got := s.Score(ctx, req, &framework.Candidate{Provider: "gke"}); got != 0 {
		t.Fatalf("a non-preferred provider should score 0, got %d", got)
	}
}

// --- the behaviour this project was extracted from -------------------------

func TestDefaultProfilePrefersTheCheapProviderWhenItIsWarm(t *testing.T) {
	cands := []framework.Candidate{
		{Provider: "civo", Reachable: true, WarmCapacity: 3, CostPerHour: 1},
		{Provider: "gke", Reachable: true, WarmCapacity: 8, CostPerHour: 4},
	}
	got, err := plugins.DefaultProfile(cands).Schedule(ctx, &framework.Request{}, cands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// gke has more warm capacity, but warm saturates and civo is far cheaper.
	if got.Provider != "civo" {
		t.Fatalf("placed on %s, want civo:\n%s", got.Provider, got.Explain())
	}
}

func TestDefaultProfileSpillsWhenTheCheapProviderIsCold(t *testing.T) {
	cands := []framework.Candidate{
		{Provider: "civo", Reachable: true, WarmCapacity: 0, CostPerHour: 1},
		{Provider: "gke", Reachable: true, WarmCapacity: 5, CostPerHour: 4},
	}
	got, err := plugins.DefaultProfile(cands).Schedule(ctx, &framework.Request{}, cands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Provider != "gke" {
		t.Fatalf("placed on %s, want gke to absorb the spill:\n%s", got.Provider, got.Explain())
	}
}

func TestDefaultProfileStillPlacesWhenEverythingIsCold(t *testing.T) {
	// A cold start beats refusing to start a session.
	cands := []framework.Candidate{
		{Provider: "civo", Reachable: true, CostPerHour: 1},
		{Provider: "gke", Reachable: true, CostPerHour: 4},
	}
	got, err := plugins.DefaultProfile(cands).Schedule(ctx, &framework.Request{}, cands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Provider != "civo" {
		t.Fatalf("placed on %s, want the cheaper cold start:\n%s", got.Provider, got.Explain())
	}
}

func TestDefaultProfileDemotesButDoesNotExcludeUnreachable(t *testing.T) {
	// The default profile has no Reachable filter: a provider can miss
	// heartbeats while still accepting claims, so it is demoted rather than
	// disqualified, and placement never fails for want of a heartbeat.
	cands := []framework.Candidate{
		{Provider: "civo", Reachable: false, WarmCapacity: 5, CostPerHour: 1},
		{Provider: "gke", Reachable: true, WarmCapacity: 5, CostPerHour: 4},
	}
	got, err := plugins.DefaultProfile(cands).Schedule(ctx, &framework.Request{}, cands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Provider != "gke" {
		t.Fatalf("placed on %s, want gke while civo is silent:\n%s", got.Provider, got.Explain())
	}
	// ...but civo must still be a viable candidate, not filtered out.
	for _, r := range got.Results {
		if r.Provider == "civo" && r.Filtered {
			t.Fatal("an unreachable provider must be demoted, not excluded, in the default profile")
		}
	}
}

func TestDefaultProfileHonoursGpuRequirement(t *testing.T) {
	cands := []framework.Candidate{
		{Provider: "civo", Reachable: true, WarmCapacity: 5, CostPerHour: 1},
		{Provider: "modal", Reachable: true, WarmCapacity: 0, CostPerHour: 9,
			Attributes: map[string]string{"gpu": "true"}},
	}
	req := &framework.Request{Requires: map[string]string{"gpu": "true"}}
	got, err := plugins.DefaultProfile(cands).Schedule(ctx, req, cands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Cheaper and warmer, but it cannot run the workload at all.
	if got.Provider != "modal" {
		t.Fatalf("placed on %s, want modal — a hard requirement outranks every preference:\n%s",
			got.Provider, got.Explain())
	}
}

func TestDefaultProfileFailsLoudlyWhenNothingCanSatisfy(t *testing.T) {
	cands := []framework.Candidate{
		{Provider: "civo", Reachable: true, WarmCapacity: 5, CostPerHour: 1},
	}
	req := &framework.Request{Requires: map[string]string{"gpu": "true"}}
	_, err := plugins.DefaultProfile(cands).Schedule(ctx, req, cands)
	if err == nil {
		t.Fatal("expected an error when no provider meets a hard requirement")
	}
	if !contains(err.Error(), "gpu") {
		t.Fatalf("the error should name the unmet requirement, got %q", err)
	}
}

func TestAffinityReturnsAWokenSessionHomeWhenViable(t *testing.T) {
	// Equal on every other axis, a woken session should go back to where its
	// data is warm.
	cands := []framework.Candidate{
		{Provider: "civo", Reachable: true, WarmCapacity: 3, CostPerHour: 2},
		{Provider: "gke", Reachable: true, WarmCapacity: 3, CostPerHour: 2},
	}
	got, err := plugins.DefaultProfile(cands).Schedule(ctx,
		&framework.Request{PreferProvider: "gke"}, cands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Provider != "gke" {
		t.Fatalf("placed on %s, want gke by affinity:\n%s", got.Provider, got.Explain())
	}
}

func TestAffinityLosesToASubstantiallyBetterOption(t *testing.T) {
	// Affinity is a tiebreak, not a pin. A session must move rather than wait
	// on a provider with nothing warm.
	cands := []framework.Candidate{
		{Provider: "civo", Reachable: true, WarmCapacity: 5, CostPerHour: 1},
		{Provider: "gke", Reachable: true, WarmCapacity: 0, CostPerHour: 4},
	}
	got, err := plugins.DefaultProfile(cands).Schedule(ctx,
		&framework.Request{PreferProvider: "gke"}, cands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Provider != "civo" {
		t.Fatalf("placed on %s; affinity must not pin a session to a cold, costly provider:\n%s",
			got.Provider, got.Explain())
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
