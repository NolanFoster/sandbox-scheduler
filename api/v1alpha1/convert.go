package v1alpha1

import (
	"fmt"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
	"github.com/NolanFoster/sandbox-scheduler/pkg/plugins"
)

// This file turns declarative API objects into the runtime types the scheduler
// executes. It is the boundary where an operator's YAML becomes behaviour, and
// therefore where a mistake becomes a silently mis-scheduled fleet — so
// everything here fails loudly rather than defaulting.

// knownFilters maps API names to implementations.
var knownFilters = map[string]func() framework.Filter{
	"RequiredAttributes": func() framework.Filter { return plugins.RequiredAttributes{} },
	"Reachable":          func() framework.Filter { return plugins.Reachable{} },
}

// knownScorers maps API names to implementations.
//
// Cost is absent: it must be calibrated against the candidate set for this
// pass, so BuildProfile constructs it rather than looking it up.
var knownScorers = map[string]func() framework.Scorer{
	"WarmCapacity": func() framework.Scorer { return plugins.WarmCapacity{} },
	"Reachability": func() framework.Scorer { return plugins.Reachability{} },
	"Affinity":     func() framework.Scorer { return plugins.Affinity{} },
}

// KnownFilterNames returns the filters this build implements, sorted.
func KnownFilterNames() []string { return sortedKeys(knownFilters) }

// KnownScorerNames returns the scorers this build implements, sorted.
func KnownScorerNames() []string {
	names := sortedKeys(knownScorers)
	names = append(names, "Cost")
	sort.Strings(names)
	return names
}

// Validate reports whether a policy can be executed by this scheduler.
//
// Called by the controller before a policy is ever used, so an unusable policy
// surfaces as a Valid=false condition on the object rather than as workloads
// placed under constraints nobody wrote.
func (p *SandboxPlacementPolicy) Validate() error {
	for _, name := range p.Spec.Filters {
		if _, ok := knownFilters[name]; !ok {
			return fmt.Errorf("unknown filter %q (available: %v)", name, KnownFilterNames())
		}
	}
	seen := map[string]bool{}
	for _, sc := range p.Spec.Scorers {
		if sc.Name != "Cost" {
			if _, ok := knownScorers[sc.Name]; !ok {
				return fmt.Errorf("unknown scorer %q (available: %v)", sc.Name, KnownScorerNames())
			}
		}
		if seen[sc.Name] {
			// Two entries for one scorer would silently sum their weights,
			// making the effective weight differ from any number written down.
			return fmt.Errorf("scorer %q listed more than once", sc.Name)
		}
		seen[sc.Name] = true
		if sc.Weight < 0 {
			return fmt.Errorf("scorer %q has negative weight %d", sc.Name, sc.Weight)
		}
	}
	if len(p.Spec.Requires) > 0 && !contains(p.Spec.Filters, "RequiredAttributes") {
		// Requirements without the filter that enforces them read as a
		// constraint but are not one. Failing here is the difference between an
		// operator seeing an error and believing their GPU workloads are pinned.
		return fmt.Errorf(
			"spec.requires is set but the RequiredAttributes filter is not enabled, so the requirements would not be enforced")
	}
	return nil
}

// BuildProfile compiles a policy into an executable scheduling profile.
//
// Candidates are needed because the Cost scorer is relative to the most
// expensive provider in this pass; it cannot be built once and reused.
func (p *SandboxPlacementPolicy) BuildProfile(candidates []framework.Candidate) (*framework.Profile, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}

	profile := &framework.Profile{Name: p.Name}
	for _, name := range p.Spec.Filters {
		profile.Filters = append(profile.Filters, knownFilters[name]())
	}
	for _, sc := range p.Spec.Scorers {
		weight := sc.Weight
		if weight == 0 {
			// The API defaults this to 1, but an object created before that
			// default existed, or built in Go without it, would otherwise get a
			// silently disabled scorer.
			weight = 1
		}
		var scorer framework.Scorer
		if sc.Name == "Cost" {
			scorer = plugins.NewCost(candidates)
		} else {
			scorer = knownScorers[sc.Name]()
		}
		profile.Scorers = append(profile.Scorers, framework.WeightedScorer{
			Scorer: scorer, Weight: weight,
		})
	}
	return profile, nil
}

// BuildRequest turns a policy plus per-sandbox intent into a scheduling
// request. The policy's requirements are merged with the sandbox's own;
// the policy wins on conflict, because it is the operator's constraint and the
// sandbox's is a preference expressed by whoever asked for it.
func (p *SandboxPlacementPolicy) BuildRequest(name string, sandboxRequires map[string]string, preferProvider string) *framework.Request {
	req := &framework.Request{Name: name, PreferProvider: preferProvider}
	if len(sandboxRequires) > 0 || len(p.Spec.Requires) > 0 {
		req.Requires = make(map[string]string, len(sandboxRequires)+len(p.Spec.Requires))
		for k, v := range sandboxRequires {
			req.Requires[k] = v
		}
		for k, v := range p.Spec.Requires {
			req.Requires[k] = v
		}
	}
	return req
}

// SelectPolicy picks the policy governing a sandbox with the given labels.
//
// Highest priority wins; ties break on name. Without a deterministic rule the
// governing policy would depend on list ordering and change as objects come and
// go, which is indistinguishable from a scheduler bug when it happens.
func SelectPolicy(policies []SandboxPlacementPolicy, objectLabels map[string]string) (*SandboxPlacementPolicy, error) {
	var best *SandboxPlacementPolicy
	for i := range policies {
		p := &policies[i]
		match, err := policyMatches(p, objectLabels)
		if err != nil {
			return nil, fmt.Errorf("policy %s: %w", p.Name, err)
		}
		if !match {
			continue
		}
		if best == nil ||
			p.Spec.Priority > best.Spec.Priority ||
			(p.Spec.Priority == best.Spec.Priority && p.Name < best.Name) {
			best = p
		}
	}
	if best == nil {
		return nil, nil
	}
	return best, nil
}

func policyMatches(p *SandboxPlacementPolicy, objectLabels map[string]string) (bool, error) {
	if p.Spec.Selector == nil {
		return true, nil
	}
	sel, err := metav1.LabelSelectorAsSelector(p.Spec.Selector)
	if err != nil {
		return false, err
	}
	return sel.Matches(labels.Set(objectLabels)), nil
}

// ProviderCandidate converts a provider's spec and status into a scheduling
// candidate.
//
// Attributes come from spec, not status: see the note on
// SandboxProviderSpec.Attributes for why a provider is not trusted to assert
// facts about its own isolation.
func (p *SandboxProvider) ProviderCandidate(reachable bool) framework.Candidate {
	c := framework.Candidate{
		Provider:     p.Name,
		Reachable:    reachable,
		WarmCapacity: int(p.Status.WarmCapacity),
	}
	if p.Spec.CostPerHour != nil {
		c.CostPerHour = p.Spec.CostPerHour.AsApproximateFloat64()
	}
	if len(p.Spec.Attributes) > 0 {
		c.Attributes = make(map[string]string, len(p.Spec.Attributes))
		for k, v := range p.Spec.Attributes {
			c.Attributes[k] = v
		}
	}
	return c
}

// DefaultPolicy is the policy this project was extracted from, expressed as an
// API object. Applied when no SandboxPlacementPolicy matches, so a cluster with
// providers but no policy still schedules sensibly instead of refusing to.
func DefaultPolicy() *SandboxPlacementPolicy {
	return &SandboxPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: SandboxPlacementPolicySpec{
			Filters: []string{"RequiredAttributes"},
			Scorers: []ScorerConfig{
				{Name: "WarmCapacity", Weight: 5},
				{Name: "Cost", Weight: 3},
				{Name: "Reachability", Weight: 3},
				{Name: "Affinity", Weight: 1},
			},
		},
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func sortedKeys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
