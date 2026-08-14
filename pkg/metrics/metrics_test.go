package metrics_test

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"

	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
	"github.com/NolanFoster/sandbox-scheduler/pkg/metrics"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

func decision(provider string, results ...framework.CandidateResult) *framework.Decision {
	return &framework.Decision{Provider: provider, Results: results}
}

func scored(provider string) framework.CandidateResult {
	return framework.CandidateResult{Provider: provider}
}

func filtered(provider, reason string) framework.CandidateResult {
	return framework.CandidateResult{Provider: provider, Filtered: true, Reason: reason}
}

// --- spill accounting ------------------------------------------------------

func TestChoosingTheCheapestIsNotASpill(t *testing.T) {
	metrics.PlacementsTotal.Reset()
	cands := []framework.Candidate{
		{Provider: "civo", CostPerHour: 1, WarmCapacity: 3},
		{Provider: "gke", CostPerHour: 4, WarmCapacity: 3},
	}
	metrics.RecordDecision("default", decision("civo", scored("civo"), scored("gke")), cands, time.Millisecond)

	if got := testutil.ToFloat64(metrics.PlacementsTotal.WithLabelValues("civo", "default", "true", "true")); got != 1 {
		t.Fatalf("cheapest=true count %v, want 1", got)
	}
}

func TestPassingOverTheCheapestIsASpill(t *testing.T) {
	// The number the whole economic argument rests on.
	metrics.PlacementsTotal.Reset()
	cands := []framework.Candidate{
		{Provider: "civo", CostPerHour: 1, WarmCapacity: 0},
		{Provider: "gke", CostPerHour: 4, WarmCapacity: 5},
	}
	metrics.RecordDecision("default", decision("gke", scored("civo"), scored("gke")), cands, time.Millisecond)

	if got := testutil.ToFloat64(metrics.PlacementsTotal.WithLabelValues("gke", "default", "false", "true")); got != 1 {
		t.Fatalf("cheapest=false count %v, want 1 — this is the spill signal", got)
	}
}

func TestAFilteredCheaperProviderIsNotCountedAsASpill(t *testing.T) {
	// A cheaper provider that could not run the workload was never an
	// alternative. Counting it would inflate the spill rate with placements
	// that had no cheaper option, and make the fleet look mis-sized.
	metrics.PlacementsTotal.Reset()
	cands := []framework.Candidate{
		{Provider: "civo", CostPerHour: 1, WarmCapacity: 3},
		{Provider: "modal", CostPerHour: 9, WarmCapacity: 1},
	}
	d := decision("modal", filtered("civo", "RequiredAttributes: no gpu"), scored("modal"))
	metrics.RecordDecision("gpu", d, cands, time.Millisecond)

	if got := testutil.ToFloat64(metrics.PlacementsTotal.WithLabelValues("modal", "gpu", "true", "true")); got != 1 {
		t.Fatalf("want cheapest=true when the cheaper provider was filtered out, got %v", got)
	}
}

func TestColdPlacementIsRecorded(t *testing.T) {
	metrics.PlacementsTotal.Reset()
	cands := []framework.Candidate{{Provider: "civo", CostPerHour: 1, WarmCapacity: 0}}
	metrics.RecordDecision("default", decision("civo", scored("civo")), cands, time.Millisecond)

	if got := testutil.ToFloat64(metrics.PlacementsTotal.WithLabelValues("civo", "default", "true", "false")); got != 1 {
		t.Fatalf("warm=false count %v, want 1", got)
	}
}

// --- failures --------------------------------------------------------------

func TestFailureReasonsAreDistinct(t *testing.T) {
	metrics.PlacementFailuresTotal.Reset()
	metrics.RecordFailure("no_providers", nil, "", 0)
	metrics.RecordFailure("no_candidates", nil, "default", time.Millisecond)
	metrics.RecordFailure("invalid_policy", nil, "broken", 0)

	for _, reason := range []string{"no_providers", "no_candidates", "invalid_policy"} {
		if got := testutil.ToFloat64(metrics.PlacementFailuresTotal.WithLabelValues(reason)); got != 1 {
			t.Fatalf("%s count %v, want 1", reason, got)
		}
	}
}

func TestFilterRejectionsAreAttributedToTheFilter(t *testing.T) {
	// Answers "unschedulable — but why?" without reading logs.
	metrics.FilteredTotal.Reset()
	results := []framework.CandidateResult{
		filtered("civo", "RequiredAttributes: requires gpu=true, provider does not declare gpu"),
		filtered("gke", "Reachable: no recent healthy report from this provider"),
	}
	metrics.RecordFailure("no_candidates", results, "default", time.Millisecond)

	if got := testutil.ToFloat64(metrics.FilteredTotal.WithLabelValues("civo", "RequiredAttributes")); got != 1 {
		t.Fatalf("civo/RequiredAttributes count %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.FilteredTotal.WithLabelValues("gke", "Reachable")); got != 1 {
		t.Fatalf("gke/Reachable count %v, want 1", got)
	}
}

func TestFilterLabelDropsTheDetail(t *testing.T) {
	// The detail embeds provider-specific values and would be unbounded as a
	// label; one series per distinct message would take out the metrics
	// pipeline long before it explained anything.
	metrics.FilteredTotal.Reset()
	for _, msg := range []string{
		"RequiredAttributes: requires gpu=true, provider has gpu=false",
		"RequiredAttributes: requires region=nyc1, provider has region=lon1",
	} {
		metrics.RecordFailure("no_candidates", []framework.CandidateResult{filtered("civo", msg)}, "p", 0)
	}
	if got := testutil.ToFloat64(metrics.FilteredTotal.WithLabelValues("civo", "RequiredAttributes")); got != 2 {
		t.Fatalf("want both rejections under one series, got %v", got)
	}
}

// --- provider gauges -------------------------------------------------------

type fakeStatuses struct{ statuses []registry.Status }

func (f *fakeStatuses) Statuses() []registry.Status { return f.statuses }

func TestProviderGaugesReflectRegistryState(t *testing.T) {
	src := &fakeStatuses{statuses: []registry.Status{
		{
			Provider:  "civo",
			Reachable: true,
			Age:       5 * time.Second,
			Report:    registry.Report{WarmCapacity: 3},
			Config:    registry.ProviderConfig{CostPerHour: 1},
		},
		{
			Provider:  "gke",
			Reachable: false,
			Stale:     true,
			Age:       90 * time.Second,
			Report:    registry.Report{WarmCapacity: 2},
			Config:    registry.ProviderConfig{CostPerHour: 4},
		},
	}}

	// CollectAndFormat filters to the named metrics; passing none returns
	// nothing at all rather than everything.
	out, err := testutil.CollectAndFormat(metrics.NewRegistryCollector(src), expfmt.TypeTextPlain,
		"sandbox_scheduler_provider_warm_capacity",
		"sandbox_scheduler_provider_reachable",
		"sandbox_scheduler_provider_report_age_seconds",
		"sandbox_scheduler_provider_cost_per_hour")
	if err != nil {
		t.Fatalf("collecting: %v", err)
	}
	text := string(out)

	for _, want := range []string{
		`sandbox_scheduler_provider_warm_capacity{provider="civo"} 3`,
		`sandbox_scheduler_provider_reachable{provider="civo"} 1`,
		`sandbox_scheduler_provider_reachable{provider="gke"} 0`,
		`sandbox_scheduler_provider_report_age_seconds{provider="gke"} 90`,
		`sandbox_scheduler_provider_cost_per_hour{provider="gke"} 4`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in:\n%s", want, text)
		}
	}

	// A stale provider still reports its last known capacity: it remains a
	// placement candidate, so a dashboard showing zero would be misleading.
	if !strings.Contains(text, `sandbox_scheduler_provider_warm_capacity{provider="gke"} 2`) {
		t.Fatal("a stale provider should still report its last known capacity")
	}
}

func TestRemovedProviderLeavesNoStaleSeries(t *testing.T) {
	// Collected at scrape time, so a provider that goes away simply stops
	// being reported — no series left claiming capacity that no longer exists.
	src := &fakeStatuses{statuses: []registry.Status{{Provider: "civo", Reachable: true}}}
	c := metrics.NewRegistryCollector(src)

	before, _ := testutil.CollectAndFormat(c, expfmt.TypeTextPlain, "sandbox_scheduler_provider_reachable")
	if !strings.Contains(string(before), `provider="civo"`) {
		t.Fatal("expected civo before removal")
	}

	src.statuses = nil
	after, _ := testutil.CollectAndFormat(c, expfmt.TypeTextPlain, "sandbox_scheduler_provider_reachable")
	if strings.Contains(string(after), `provider="civo"`) {
		t.Fatalf("civo still reported after removal:\n%s", after)
	}
}
