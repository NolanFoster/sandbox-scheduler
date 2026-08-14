// Package metrics instruments placement.
//
// These exist to answer four questions that cannot be reconstructed after the
// fact, and which are exactly what a proposal or a capacity review will ask:
//
//  1. How often did the cheapest provider lack capacity? That is the spill
//     rate, and it is the entire economic argument for running a cheap primary
//     with an elastic overflow.
//  2. How long does a decision take? The defence of reading from a local cache
//     instead of the API server is a latency claim; without a histogram it is
//     just an assertion.
//  3. What failed, and why? An unschedulable event is only actionable if you
//     know which filter rejected everything.
//  4. How often were providers unreachable? This says whether
//     stale-but-retained capacity actually saved placements, or was never
//     exercised.
//
// Cardinality is deliberately bounded. Provider, policy and filter names are
// all operator-controlled and few. The sandbox name is *not* a label anywhere:
// it is unbounded, and one series per sandbox would take out the metrics
// pipeline long before it told anyone anything.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
)

const namespace = "sandbox_scheduler"

var (
	// PlacementsTotal counts successful decisions.
	//
	// `cheapest` is the label that matters: spill rate is
	// 1 - rate(placements{cheapest="true"}) / rate(placements). Recording it at
	// decision time is the only way to get it — after the fact you cannot know
	// what the alternatives cost at that moment.
	PlacementsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "placements_total",
			Help: "Placement decisions, by chosen provider and policy. " +
				"cheapest=false means the cheapest candidate was passed over — the spill signal. " +
				"warm=false means the chosen provider had no pre-warmed capacity, so the session cold-starts.",
		},
		[]string{"provider", "policy", "cheapest", "warm"},
	)

	// PlacementFailuresTotal counts requests that produced no decision.
	PlacementFailuresTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "placement_failures_total",
			Help: "Placement requests that produced no decision, by reason: " +
				"no_providers (nothing configured), no_candidates (nothing satisfied the request), " +
				"invalid_policy, or policy_selection_error.",
		},
		[]string{"reason"},
	)

	// PlacementDuration measures the decision itself.
	//
	// Buckets are sub-millisecond-heavy on purpose: this path reads local
	// memory, so the interesting question is whether it stays there. Buckets
	// clustered around 100ms would hide the entire distribution in one bar.
	PlacementDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "placement_duration_seconds",
			Help:      "Time to produce a placement decision, excluding HTTP overhead.",
			Buckets: []float64{
				0.00001, 0.00005, 0.0001, 0.00025, 0.0005,
				0.001, 0.0025, 0.005, 0.01, 0.05, 0.1,
			},
		},
		[]string{"policy"},
	)

	// FilteredTotal counts per-provider rejections, by the filter responsible.
	//
	// Answers "unschedulable — but why?" without needing logs, and shows a
	// filter that is quietly excluding a provider nobody noticed losing.
	FilteredTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "filtered_total",
			Help:      "Times a filter rejected a provider during placement.",
		},
		[]string{"provider", "filter"},
	)
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		PlacementsTotal,
		PlacementFailuresTotal,
		PlacementDuration,
		FilteredTotal,
	)
}

// RecordDecision records a successful placement.
//
// `candidates` is the set the decision was made over, needed to determine
// whether the winner was the cheapest option available at that moment.
func RecordDecision(policy string, d *framework.Decision, candidates []framework.Candidate, took time.Duration) {
	chosenWarm := false
	chosenCost := 0.0
	cheapestCost := -1.0
	for i := range candidates {
		c := &candidates[i]
		if c.Provider == d.Provider {
			chosenWarm = c.WarmCapacity > 0
			chosenCost = c.CostPerHour
		}
		// Only candidates that survived filtering are real alternatives; a
		// cheaper provider that could not run the workload is not a spill.
		if !wasFiltered(d, c.Provider) {
			if cheapestCost < 0 || c.CostPerHour < cheapestCost {
				cheapestCost = c.CostPerHour
			}
		}
	}

	PlacementsTotal.WithLabelValues(
		d.Provider, policy, boolLabel(chosenCost <= cheapestCost), boolLabel(chosenWarm),
	).Inc()
	PlacementDuration.WithLabelValues(policy).Observe(took.Seconds())
	recordFiltered(d.Results)
}

// RecordFailure records a request that produced no decision.
func RecordFailure(reason string, results []framework.CandidateResult, policy string, took time.Duration) {
	PlacementFailuresTotal.WithLabelValues(reason).Inc()
	if policy != "" {
		PlacementDuration.WithLabelValues(policy).Observe(took.Seconds())
	}
	recordFiltered(results)
}

func recordFiltered(results []framework.CandidateResult) {
	for _, r := range results {
		if !r.Filtered {
			continue
		}
		FilteredTotal.WithLabelValues(r.Provider, filterName(r.Reason)).Inc()
	}
}

// filterName extracts the filter from a reason of the form "Filter: detail".
// The detail is deliberately dropped: it embeds provider-specific values and
// would be unbounded as a label.
func filterName(reason string) string {
	for i := 0; i < len(reason); i++ {
		if reason[i] == ':' {
			return reason[:i]
		}
	}
	if reason == "" {
		return "unknown"
	}
	return reason
}

func wasFiltered(d *framework.Decision, provider string) bool {
	for _, r := range d.Results {
		if r.Provider == provider {
			return r.Filtered
		}
	}
	return false
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
