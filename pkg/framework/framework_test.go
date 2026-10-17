package framework_test

import (
	"context"
	"strings"
	"testing"

	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
)

// --- test doubles ----------------------------------------------------------

type rejectAll struct{ reason string }

func (rejectAll) Name() string { return "RejectAll" }
func (r rejectAll) Filter(context.Context, *framework.Request, *framework.Candidate) (bool, string) {
	return false, r.reason
}

type rejectNamed struct{ provider string }

func (rejectNamed) Name() string { return "RejectNamed" }
func (r rejectNamed) Filter(_ context.Context, _ *framework.Request, c *framework.Candidate) (bool, string) {
	if c.Provider == r.provider {
		return false, "excluded by test"
	}
	return true, ""
}

// fixedScore returns a per-provider score, defaulting to 0.
type fixedScore struct {
	name   string
	scores map[string]int64
}

func (f fixedScore) Name() string { return f.name }
func (f fixedScore) Score(_ context.Context, _ *framework.Request, c *framework.Candidate) int64 {
	return f.scores[c.Provider]
}

func candidates(names ...string) []framework.Candidate {
	out := make([]framework.Candidate, 0, len(names))
	for _, n := range names {
		out = append(out, framework.Candidate{Provider: n, Reachable: true})
	}
	return out
}

// --- tests -----------------------------------------------------------------

func TestSchedulePicksHighestWeightedScore(t *testing.T) {
	p := &framework.Profile{
		Scorers: []framework.WeightedScorer{
			{Scorer: fixedScore{"a", map[string]int64{"civo": 100, "gke": 0}}, Weight: 1},
			{Scorer: fixedScore{"b", map[string]int64{"civo": 0, "gke": 50}}, Weight: 3},
		},
	}
	// gke: 50*3 = 150 beats civo: 100*1 = 100. Weight, not raw score, decides.
	got, err := p.Schedule(context.Background(), &framework.Request{}, candidates("civo", "gke"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Provider != "gke" || got.Score != 150 {
		t.Fatalf("got %s score %d, want gke score 150", got.Provider, got.Score)
	}
}

func TestFilteredCandidatesAreNotScored(t *testing.T) {
	p := &framework.Profile{
		Filters: []framework.Filter{rejectNamed{provider: "gke"}},
		Scorers: []framework.WeightedScorer{
			{Scorer: fixedScore{"a", map[string]int64{"civo": 1, "gke": 100}}, Weight: 1},
		},
	}
	got, err := p.Schedule(context.Background(), &framework.Request{}, candidates("civo", "gke"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Provider != "civo" {
		t.Fatalf("got %s, want civo — a filtered candidate must not win on score", got.Provider)
	}
	for _, r := range got.Results {
		if r.Provider == "gke" {
			if !r.Filtered {
				t.Fatal("gke should be marked filtered")
			}
			if len(r.Details) != 0 {
				t.Fatal("filtered candidates must carry no scores")
			}
		}
	}
}

func TestNoFeasibleCandidatesReturnsEveryReason(t *testing.T) {
	// The most common scheduler complaint is an opaque "unschedulable". The
	// error must name each provider and why it was rejected.
	p := &framework.Profile{Filters: []framework.Filter{rejectAll{reason: "nope"}}}
	_, err := p.Schedule(context.Background(), &framework.Request{}, candidates("civo", "gke"))
	if err == nil {
		t.Fatal("expected an error when everything is filtered")
	}
	var noCand *framework.ErrNoCandidates
	if !asErrNoCandidates(err, &noCand) {
		t.Fatalf("want *ErrNoCandidates, got %T", err)
	}
	if len(noCand.Results) != 2 {
		t.Fatalf("want reasons for both providers, got %d", len(noCand.Results))
	}
	msg := err.Error()
	for _, want := range []string{"civo", "gke", "nope"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error %q should mention %q", msg, want)
		}
	}
}

func TestEmptyCandidateSetIsAnError(t *testing.T) {
	p := &framework.Profile{}
	_, err := p.Schedule(context.Background(), &framework.Request{}, nil)
	if err == nil {
		t.Fatal("expected an error with no providers configured")
	}
	if !strings.Contains(err.Error(), "no providers configured") {
		t.Fatalf("unhelpful message: %v", err)
	}
}

func TestTiesBreakDeterministically(t *testing.T) {
	// An unstable tiebreak makes a fleet flap between providers on every pass.
	// That is invisible in a single decision and shows up later as churn.
	// Warm ties, because those are the ones settled by name; cold ties spread
	// load instead (see TestColdTiesWithEqualLoadSpreadAtRandom).
	p := &framework.Profile{
		Scorers: []framework.WeightedScorer{
			{Scorer: fixedScore{"a", map[string]int64{"zeta": 10, "alpha": 10}}, Weight: 1},
		},
	}
	cands := candidates("zeta", "alpha")
	for i := range cands {
		cands[i].WarmCapacity = 1
	}
	for i := 0; i < 20; i++ {
		got, err := p.Schedule(context.Background(), &framework.Request{}, cands)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Provider != "alpha" {
			t.Fatalf("iteration %d picked %s; ties must resolve to the lowest name", i, got.Provider)
		}
	}
}

func TestScoresAreClampedNotRejected(t *testing.T) {
	// A third-party scorer returning nonsense should lose its own influence,
	// not corrupt the total or break scheduling for everyone.
	p := &framework.Profile{
		Scorers: []framework.WeightedScorer{
			{Scorer: fixedScore{"huge", map[string]int64{"civo": 1_000_000}}, Weight: 1},
			{Scorer: fixedScore{"neg", map[string]int64{"gke": -500}}, Weight: 1},
		},
	}
	got, err := p.Schedule(context.Background(), &framework.Request{}, candidates("civo", "gke"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Score != framework.MaxScore {
		t.Fatalf("score %d should be clamped to %d", got.Score, framework.MaxScore)
	}
	for _, r := range got.Results {
		for _, d := range r.Details {
			if d.Raw < 0 || d.Raw > framework.MaxScore {
				t.Fatalf("raw score %d escaped clamping", d.Raw)
			}
		}
	}
}

func TestFirstFailingFilterShortCircuits(t *testing.T) {
	// Later filters must not run once one rejects, both for cost and because a
	// filter may assume earlier ones passed.
	var secondRan bool
	p := &framework.Profile{
		Filters: []framework.Filter{
			rejectAll{reason: "first"},
			filterFunc(func() { secondRan = true }),
		},
	}
	_, _ = p.Schedule(context.Background(), &framework.Request{}, candidates("civo"))
	if secondRan {
		t.Fatal("filters after a rejection must not run")
	}
}

func TestResultsAreOrderedBestFirstWithFilteredLast(t *testing.T) {
	p := &framework.Profile{
		Filters: []framework.Filter{rejectNamed{provider: "bad"}},
		Scorers: []framework.WeightedScorer{
			{Scorer: fixedScore{"a", map[string]int64{"low": 1, "high": 99}}, Weight: 1},
		},
	}
	got, err := p.Schedule(context.Background(), &framework.Request{}, candidates("low", "bad", "high"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	order := []string{got.Results[0].Provider, got.Results[1].Provider, got.Results[2].Provider}
	want := []string{"high", "low", "bad"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("results ordered %v, want %v", order, want)
		}
	}
}

func TestExplainNamesTheDecisionAndTheRejections(t *testing.T) {
	// Explain is the operator's only view into why placement went the way it
	// did; it has to cover chosen and rejected candidates alike.
	p := &framework.Profile{
		Filters: []framework.Filter{rejectNamed{provider: "gke"}},
		Scorers: []framework.WeightedScorer{
			{Scorer: fixedScore{"warm", map[string]int64{"civo": 40}}, Weight: 2},
		},
	}
	got, err := p.Schedule(context.Background(), &framework.Request{}, candidates("civo", "gke"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := got.Explain()
	for _, want := range []string{"civo", "gke", "filtered", "warm", "80"} {
		if !strings.Contains(out, want) {
			t.Fatalf("Explain() missing %q:\n%s", want, out)
		}
	}
}

// --- helpers ---------------------------------------------------------------

type filterFunc func()

func (filterFunc) Name() string { return "FilterFunc" }
func (f filterFunc) Filter(context.Context, *framework.Request, *framework.Candidate) (bool, string) {
	f()
	return true, ""
}

func asErrNoCandidates(err error, target **framework.ErrNoCandidates) bool {
	e, ok := err.(*framework.ErrNoCandidates)
	if ok {
		*target = e
	}
	return ok
}

// tiedProfile scores every candidate the same.
func tiedProfile(rnd func(int) int) *framework.Profile {
	return &framework.Profile{
		Scorers: []framework.WeightedScorer{{Scorer: fixedScore{"a", nil}, Weight: 1}},
		Rand:    rnd,
	}
}

func TestColdTiesGoToTheProviderWithFewerColdStartsInFlight(t *testing.T) {
	// Breaking on name would queue every cold start in a burst on "alpha"
	// while "zeta", just as good, provisions nothing.
	cands := candidates("alpha", "zeta")
	cands[0].InFlightCold = 4
	cands[1].InFlightCold = 1

	got, err := tiedProfile(nil).Schedule(context.Background(), &framework.Request{}, cands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Provider != "zeta" {
		t.Fatalf("picked %s; the less loaded cold provider should win the tie", got.Provider)
	}
	if got.Results[0].Provider != "zeta" {
		t.Fatalf("the winner should lead its tie in Results: %+v", got.Results)
	}
	if !strings.Contains(got.Explain(), `"zeta" has 1 cold starts in flight`) {
		t.Fatalf("the tie-break should be explained:\n%s", got.Explain())
	}
}

func TestWarmTiesStillBreakOnName(t *testing.T) {
	// A warm claim costs a provider nothing to serve, so there is no load to
	// spread, and a stable answer avoids flapping.
	cands := candidates("zeta", "alpha")
	for i := range cands {
		cands[i].WarmCapacity = 3
	}
	cands[1].InFlightCold = 9 // alpha
	for i := 0; i < 20; i++ {
		got, _ := tiedProfile(func(int) int { t.Fatal("warm ties must not sample"); return 0 }).
			Schedule(context.Background(), &framework.Request{}, cands)
		if got.Provider != "alpha" {
			t.Fatalf("picked %s; warm ties must resolve to the lowest name", got.Provider)
		}
	}
}

func TestInFlightColdDoesNotOverrideScore(t *testing.T) {
	// Only ties are decided by load; a better score always wins.
	p := &framework.Profile{
		Scorers: []framework.WeightedScorer{
			{Scorer: fixedScore{"a", map[string]int64{"cheap": 10, "dear": 9}}, Weight: 1},
		},
	}
	cands := candidates("cheap", "dear")
	cands[0].InFlightCold = 100
	got, _ := p.Schedule(context.Background(), &framework.Request{}, cands)
	if got.Provider != "cheap" || got.TieBreak != "" {
		t.Fatalf("picked %s (tie %q); the higher score should win outright", got.Provider, got.TieBreak)
	}
}

func TestColdTiesOfMoreThanTwoSampleTwo(t *testing.T) {
	// Power of two choices: the least loaded overall is not necessarily
	// chosen, only the less loaded of the two sampled. That is what keeps a
	// burst from herding onto whichever provider looks emptiest.
	cands := candidates("a", "b", "c")
	cands[0].InFlightCold = 0 // least loaded, but not sampled below
	cands[1].InFlightCold = 5
	cands[2].InFlightCold = 3

	seq := []int{1, 1} // i=1 (b), j=1 -> skips i -> 2 (c)
	rnd := func(n int) int { v := seq[0]; seq = seq[1:]; return v }
	got, _ := tiedProfile(rnd).Schedule(context.Background(), &framework.Request{}, cands)
	if got.Provider != "c" {
		t.Fatalf("picked %s; want c, the less loaded of the sampled b and c", got.Provider)
	}
	if !strings.Contains(got.TieBreak, "sampled 2 of 3") {
		t.Fatalf("tie-break should say it sampled: %q", got.TieBreak)
	}
}

func TestColdTiesSpreadABurstEvenly(t *testing.T) {
	// With each placement feeding back into the count, two choices keep the
	// gap between providers small no matter how long the burst runs.
	cands := candidates("a", "b", "c", "d")
	p := tiedProfile(nil)
	placed := map[string]int{}
	for i := 0; i < 400; i++ {
		got, _ := p.Schedule(context.Background(), &framework.Request{}, cands)
		placed[got.Provider]++
		for j := range cands {
			if cands[j].Provider == got.Provider {
				cands[j].InFlightCold++
			}
		}
	}
	lo, hi := 400, 0
	for _, n := range placed {
		lo, hi = min(lo, n), max(hi, n)
	}
	if len(placed) != 4 || hi-lo > 10 {
		t.Fatalf("placed %v; a burst of cold starts should spread evenly", placed)
	}
}

func TestColdTiesWithEqualLoadSpreadAtRandom(t *testing.T) {
	// Concurrent requests read their snapshots before any records its
	// placement, so a burst sees equal counts. Breaking those by name would
	// send the whole burst to one provider.
	p := tiedProfile(nil)
	placed := map[string]int{}
	for i := 0; i < 400; i++ {
		got, _ := p.Schedule(context.Background(), &framework.Request{}, candidates("alpha", "zeta"))
		placed[got.Provider]++
		if !strings.Contains(got.TieBreak, "equal, chose at random") {
			t.Fatalf("tie-break should say it chose at random: %q", got.TieBreak)
		}
	}
	if placed["alpha"] < 120 || placed["zeta"] < 120 {
		t.Fatalf("placed %v; equal-load cold ties should split", placed)
	}
}
