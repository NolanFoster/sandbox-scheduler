// Package framework defines the scheduling pipeline: filter, score, bind.
//
// The shape is borrowed from the Kubernetes scheduler framework, and
// deliberately so. Multi-cluster placement has been solved in that vocabulary
// before (Karmada's PropagationPolicy, Open Cluster Management's Placement),
// and reviewers who know one know the others. The novelty here is not the
// pipeline; it is that a candidate is a *provider*, which may not be a
// Kubernetes cluster at all.
//
// Two properties matter more than they might appear:
//
//   - **Scheduling reads a cache, never the providers.** A scheduling decision
//     is a pure function over a capacity snapshot. Probing providers on the
//     decision path would put a network round trip per provider into every
//     sandbox start, and sandbox start is a 90–150ms game. Capacity arrives
//     asynchronously (see pkg/registry); the pipeline only reads it.
//   - **Every decision explains itself.** A scheduler that says "gke" and
//     nothing else is impossible to operate: you cannot tell a misconfigured
//     filter from a genuinely full cluster. Filters record why they rejected a
//     candidate and scorers record what they contributed, always, not only on
//     failure.
package framework

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// MaxScore is the normalized upper bound a scorer may return. Scorers are
// weighted afterwards, so a scorer cannot inflate its own influence by
// returning larger numbers — only the operator's weight decides that.
const MaxScore int64 = 100

// Candidate is somewhere a sandbox could run.
//
// It intentionally does not describe *how* to reach the provider. Binding is a
// separate concern (see Binder), so the pipeline stays a pure function and can
// be tested without any provider being reachable.
type Candidate struct {
	// Provider is the stable identifier recorded on the resulting sandbox.
	Provider string

	// Reachable reports whether the registry has recent, healthy state.
	// An unreachable provider is not automatically disqualified: that is a
	// filter's decision, because "prefer reachable, but use anything rather
	// than fail" is a legitimate policy.
	Reachable bool

	// WarmCapacity is the number of pre-warmed sandboxes ready to claim.
	// This is the difference between a ~100ms start and a ~30s cold start,
	// which is why it exists as a first-class field rather than an attribute.
	WarmCapacity int

	// Attributes carry provider facts that filters and scorers match on:
	// runtime=gvisor, gpu=true, region=nyc1, kubernetes=true. Free-form so a
	// new policy does not require a schema change.
	Attributes map[string]string

	// CostPerHour is the relative price of a sandbox-hour here. Units are the
	// operator's own; only the ordering is used.
	CostPerHour float64
}

// Attr returns an attribute value, or "" when absent.
func (c *Candidate) Attr(key string) string {
	if c.Attributes == nil {
		return ""
	}
	return c.Attributes[key]
}

// Request is what needs placing.
type Request struct {
	// Name of the sandbox being placed, for logs and traces.
	Name string

	// Requires are hard requirements matched against candidate attributes by
	// the RequiredAttributes filter, e.g. {"gpu": "true"}.
	Requires map[string]string

	// PreferProvider names a provider to keep a session on when it is still
	// viable. Set on wake so a session returns to where its data is warm,
	// without pinning it there if that provider is gone or full.
	PreferProvider string
}

// Filter is a hard constraint. Returning false removes the candidate.
//
// The reason is required, not optional: it is the only way an operator can
// tell "no capacity" from "your policy excluded everything".
type Filter interface {
	Name() string
	Filter(ctx context.Context, req *Request, c *Candidate) (ok bool, reason string)
}

// Scorer is a soft preference over the candidates that survived filtering.
//
// Implementations return 0..MaxScore. Values outside that range are clamped
// rather than rejected, so a buggy third-party scorer degrades its own
// influence instead of corrupting the total.
type Scorer interface {
	Name() string
	Score(ctx context.Context, req *Request, c *Candidate) int64
}

// WeightedScorer pairs a scorer with the operator's weight for it. Weight is
// what makes policy tunable without code: "cost matters twice as much as warm
// capacity" is a config change.
type WeightedScorer struct {
	Scorer Scorer
	Weight int64
}

// ScoreDetail records one scorer's contribution to one candidate.
type ScoreDetail struct {
	Scorer   string
	Raw      int64
	Weight   int64
	Weighted int64
}

// CandidateResult is the full reasoning for a single candidate.
type CandidateResult struct {
	Provider string
	// Filtered is set when a filter rejected this candidate; Reason says which
	// and why. Filtered candidates keep no scores.
	Filtered bool
	Reason   string
	Total    int64
	Details  []ScoreDetail
}

// Decision is the pipeline's output: the chosen provider plus the complete
// reasoning for every candidate, chosen or not.
type Decision struct {
	Provider string
	Score    int64
	// Results covers every candidate considered, in evaluation order, so a
	// decision can be explained after the fact from this value alone.
	Results []CandidateResult
}

// Explain renders a human-readable account of the decision. Intended for
// operator-facing logs and `kubectl describe`-style output.
func (d *Decision) Explain() string {
	var b strings.Builder
	fmt.Fprintf(&b, "placed on %q (score %d)\n", d.Provider, d.Score)
	for _, r := range d.Results {
		if r.Filtered {
			fmt.Fprintf(&b, "  %-12s filtered: %s\n", r.Provider, r.Reason)
			continue
		}
		parts := make([]string, 0, len(r.Details))
		for _, d := range r.Details {
			parts = append(parts, fmt.Sprintf("%s=%d*%d", d.Scorer, d.Raw, d.Weight))
		}
		fmt.Fprintf(&b, "  %-12s score %-5d %s\n", r.Provider, r.Total, strings.Join(parts, " "))
	}
	return b.String()
}

// ErrNoCandidates reports that nothing survived filtering. It carries the
// per-candidate reasons so the caller can surface them rather than a bare
// "unschedulable", which is the single most common complaint about schedulers.
type ErrNoCandidates struct {
	Results []CandidateResult
}

func (e *ErrNoCandidates) Error() string {
	if len(e.Results) == 0 {
		return "no providers configured"
	}
	reasons := make([]string, 0, len(e.Results))
	for _, r := range e.Results {
		reasons = append(reasons, fmt.Sprintf("%s: %s", r.Provider, r.Reason))
	}
	return "no provider satisfied the request: " + strings.Join(reasons, "; ")
}

// Profile is a named scheduling configuration: which filters run, which
// scorers run, and how much each scorer counts.
type Profile struct {
	Name    string
	Filters []Filter
	Scorers []WeightedScorer
}

// Schedule runs filter then score and returns the winner.
//
// Pure: no I/O, no clock, no provider calls. Everything time-varying arrives
// through the candidates, which is what makes the policy testable and the hot
// path fast.
func (p *Profile) Schedule(ctx context.Context, req *Request, candidates []Candidate) (*Decision, error) {
	results := make([]CandidateResult, 0, len(candidates))
	feasible := make([]int, 0, len(candidates))

	for i := range candidates {
		c := &candidates[i]
		res := CandidateResult{Provider: c.Provider}

		rejected := false
		for _, f := range p.Filters {
			ok, reason := f.Filter(ctx, req, c)
			if !ok {
				res.Filtered = true
				if reason == "" {
					reason = "rejected by " + f.Name()
				}
				res.Reason = fmt.Sprintf("%s: %s", f.Name(), reason)
				rejected = true
				break
			}
		}
		if rejected {
			results = append(results, res)
			continue
		}

		for _, ws := range p.Scorers {
			raw := ws.Scorer.Score(ctx, req, c)
			// Clamp rather than reject: a misbehaving scorer should lose
			// influence, not break scheduling for everyone.
			if raw < 0 {
				raw = 0
			}
			if raw > MaxScore {
				raw = MaxScore
			}
			weighted := raw * ws.Weight
			res.Details = append(res.Details, ScoreDetail{
				Scorer: ws.Scorer.Name(), Raw: raw, Weight: ws.Weight, Weighted: weighted,
			})
			res.Total += weighted
		}

		feasible = append(feasible, len(results))
		results = append(results, res)
	}

	if len(feasible) == 0 {
		return nil, &ErrNoCandidates{Results: results}
	}

	// Highest score wins; ties break on provider name. Deterministic ordering
	// matters: an unstable tiebreak makes a fleet flap between providers on
	// every scheduling pass, which is invisible until it shows up as churn.
	best := feasible[0]
	for _, idx := range feasible[1:] {
		if results[idx].Total > results[best].Total ||
			(results[idx].Total == results[best].Total && results[idx].Provider < results[best].Provider) {
			best = idx
		}
	}

	// Stable output ordering, best first, so callers can log the top N.
	ordered := append([]CandidateResult(nil), results...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Filtered != ordered[j].Filtered {
			return !ordered[i].Filtered
		}
		if ordered[i].Total != ordered[j].Total {
			return ordered[i].Total > ordered[j].Total
		}
		return ordered[i].Provider < ordered[j].Provider
	})

	return &Decision{
		Provider: results[best].Provider,
		Score:    results[best].Total,
		Results:  ordered,
	}, nil
}
