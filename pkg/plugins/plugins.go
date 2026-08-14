// Package plugins provides the built-in filters and scorers.
//
// These are deliberately small and independent. The point of the framework is
// that a new placement policy is a plugin plus a weight, not a change to the
// scheduler — so the built-ins should read as examples as much as defaults.
package plugins

import (
	"context"
	"fmt"

	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
)

// --- filters ---------------------------------------------------------------

// Reachable rejects providers the registry has not heard from recently.
//
// Not the default in every profile: "place it anywhere rather than fail" is a
// legitimate policy, and a provider can be missing heartbeats while still
// accepting claims perfectly well. Operators who prefer availability over
// precision leave this out and let the Reachability scorer demote instead.
type Reachable struct{}

func (Reachable) Name() string { return "Reachable" }

func (Reachable) Filter(_ context.Context, _ *framework.Request, c *framework.Candidate) (bool, string) {
	if c.Reachable {
		return true, ""
	}
	return false, "no recent healthy report from this provider"
}

// RequiredAttributes enforces Request.Requires against candidate attributes.
//
// This is what keeps a GPU workload off a CPU-only provider, or an untrusted
// workload off a provider without hardware isolation. Exact string match,
// deliberately: fuzzy matching in a security-relevant filter is how workloads
// end up somewhere they should not be.
type RequiredAttributes struct{}

func (RequiredAttributes) Name() string { return "RequiredAttributes" }

func (RequiredAttributes) Filter(_ context.Context, req *framework.Request, c *framework.Candidate) (bool, string) {
	for key, want := range req.Requires {
		got := c.Attr(key)
		if got != want {
			if got == "" {
				return false, fmt.Sprintf("requires %s=%s, provider does not declare %s", key, want, key)
			}
			return false, fmt.Sprintf("requires %s=%s, provider has %s=%s", key, want, key, got)
		}
	}
	return true, ""
}

// --- scorers ---------------------------------------------------------------

// WarmCapacity prefers providers with pre-warmed sandboxes waiting.
//
// This is the single biggest lever on user-visible start latency — a warm claim
// is ~100ms against a cold start measured in tens of seconds — which is why it
// usually carries the highest weight.
//
// The curve saturates on purpose. Beyond a handful of warm sandboxes the
// marginal benefit to *this* request is nil: one is all it will take. Scoring
// linearly would let a provider with 50 idle sandboxes dominate on a dimension
// that stopped mattering at 5, and quietly starve the cheaper one.
type WarmCapacity struct {
	// Saturation is the warm count at which the score reaches maximum.
	// Zero means the default of 5.
	Saturation int
}

func (WarmCapacity) Name() string { return "WarmCapacity" }

func (w WarmCapacity) Score(_ context.Context, _ *framework.Request, c *framework.Candidate) int64 {
	sat := w.Saturation
	if sat <= 0 {
		sat = 5
	}
	if c.WarmCapacity <= 0 {
		return 0
	}
	if c.WarmCapacity >= sat {
		return framework.MaxScore
	}
	return int64(c.WarmCapacity) * framework.MaxScore / int64(sat)
}

// Cost prefers cheaper providers, scored relative to the most expensive
// candidate in the set rather than against an absolute scale.
//
// Relative scoring is what makes cost comparable across wildly different
// pricing models — a per-second hosted sandbox API and a self-hosted cluster's
// amortised node cost have no common absolute unit, but they do have an
// ordering. It also means the scorer needs the whole candidate set, so it is
// constructed per scheduling pass.
type Cost struct {
	maxCost float64
}

// NewCost builds a Cost scorer calibrated against this pass's candidates.
func NewCost(candidates []framework.Candidate) *Cost {
	max := 0.0
	for i := range candidates {
		if candidates[i].CostPerHour > max {
			max = candidates[i].CostPerHour
		}
	}
	return &Cost{maxCost: max}
}

func (*Cost) Name() string { return "Cost" }

func (s *Cost) Score(_ context.Context, _ *framework.Request, c *framework.Candidate) int64 {
	// All free, or all identically priced: cost carries no signal, so stay
	// neutral rather than inventing an ordering other scorers would inherit.
	if s.maxCost <= 0 {
		return framework.MaxScore
	}
	if c.CostPerHour <= 0 {
		return framework.MaxScore
	}
	if c.CostPerHour >= s.maxCost {
		return 0
	}
	return int64((1 - c.CostPerHour/s.maxCost) * float64(framework.MaxScore))
}

// Affinity prefers the provider a session was last placed on.
//
// This is what makes wake cheap without pinning: a hibernated session's data is
// already warm where it ran, but if that provider is gone or full the session
// still moves rather than failing. Pinning would trade a small latency win for
// an availability loss, which is the wrong trade for a scheduler.
type Affinity struct{}

func (Affinity) Name() string { return "Affinity" }

func (Affinity) Score(_ context.Context, req *framework.Request, c *framework.Candidate) int64 {
	if req.PreferProvider != "" && req.PreferProvider == c.Provider {
		return framework.MaxScore
	}
	return 0
}

// Reachability demotes providers with no recent report instead of excluding
// them, for profiles that prefer degraded placement over failure.
type Reachability struct{}

func (Reachability) Name() string { return "Reachability" }

func (Reachability) Score(_ context.Context, _ *framework.Request, c *framework.Candidate) int64 {
	if c.Reachable {
		return framework.MaxScore
	}
	return 0
}

// DefaultProfile is the policy this project was extracted from: run on the
// cheapest provider that has warm capacity, spill when it does not, and return
// a woken session to where it slept when that is still sensible.
//
// The weights encode a specific claim — start latency dominates the user's
// experience, cost dominates the bill, and affinity is a tiebreak rather than a
// constraint. Operators are expected to disagree; that is the point of weights.
func DefaultProfile(candidates []framework.Candidate) *framework.Profile {
	return &framework.Profile{
		Name: "default",
		Filters: []framework.Filter{
			RequiredAttributes{},
		},
		Scorers: []framework.WeightedScorer{
			{Scorer: WarmCapacity{}, Weight: 5},
			{Scorer: NewCost(candidates), Weight: 3},
			{Scorer: Reachability{}, Weight: 3},
			{Scorer: Affinity{}, Weight: 1},
		},
	}
}
