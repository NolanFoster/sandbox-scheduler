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
	p := &framework.Profile{
		Scorers: []framework.WeightedScorer{
			{Scorer: fixedScore{"a", map[string]int64{"zeta": 10, "alpha": 10}}, Weight: 1},
		},
	}
	for i := 0; i < 20; i++ {
		got, err := p.Schedule(context.Background(), &framework.Request{}, candidates("zeta", "alpha"))
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
