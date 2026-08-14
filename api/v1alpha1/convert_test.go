package v1alpha1_test

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
)

func policy(spec v1alpha1.SandboxPlacementPolicySpec) *v1alpha1.SandboxPlacementPolicy {
	return &v1alpha1.SandboxPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "test"},
		Spec:       spec,
	}
}

// --- validation ------------------------------------------------------------

func TestValidateAcceptsKnownPlugins(t *testing.T) {
	p := policy(v1alpha1.SandboxPlacementPolicySpec{
		Filters: []string{"RequiredAttributes", "Reachable"},
		Scorers: []v1alpha1.ScorerConfig{
			{Name: "WarmCapacity", Weight: 5},
			{Name: "Cost", Weight: 3},
			{Name: "Reachability", Weight: 1},
			{Name: "Affinity", Weight: 1},
		},
	})
	if err := p.Validate(); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
}

func TestValidateRejectsUnknownPluginsAndSaysWhatExists(t *testing.T) {
	// Silently dropping an unknown filter would place workloads under weaker
	// constraints than the operator wrote — the worst failure for a policy
	// engine, because nothing looks wrong.
	p := policy(v1alpha1.SandboxPlacementPolicySpec{Filters: []string{"Typo"}})
	err := p.Validate()
	if err == nil {
		t.Fatal("expected an unknown filter to be rejected")
	}
	if !strings.Contains(err.Error(), "RequiredAttributes") {
		t.Fatalf("error should list what is available, got %q", err)
	}

	p = policy(v1alpha1.SandboxPlacementPolicySpec{
		Scorers: []v1alpha1.ScorerConfig{{Name: "Vibes", Weight: 1}},
	})
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "WarmCapacity") {
		t.Fatalf("unknown scorer should be rejected and list alternatives, got %v", err)
	}
}

func TestValidateRejectsDuplicateScorers(t *testing.T) {
	// Duplicates would silently sum, making the effective weight differ from
	// any number written in the YAML.
	p := policy(v1alpha1.SandboxPlacementPolicySpec{
		Scorers: []v1alpha1.ScorerConfig{
			{Name: "Cost", Weight: 3},
			{Name: "Cost", Weight: 4},
		},
	})
	if err := p.Validate(); err == nil || !strings.Contains(err.Error(), "more than once") {
		t.Fatalf("expected duplicate rejection, got %v", err)
	}
}

func TestValidateRejectsRequiresWithoutItsEnforcingFilter(t *testing.T) {
	// This is the trap the whole check exists for: requirements that read as a
	// constraint but enforce nothing. An operator would believe their GPU
	// workloads were pinned.
	p := policy(v1alpha1.SandboxPlacementPolicySpec{
		Requires: map[string]string{"gpu": "true"},
		Scorers:  []v1alpha1.ScorerConfig{{Name: "Cost", Weight: 1}},
	})
	err := p.Validate()
	if err == nil {
		t.Fatal("requires without RequiredAttributes must be rejected")
	}
	if !strings.Contains(err.Error(), "would not be enforced") {
		t.Fatalf("error should explain the consequence, got %q", err)
	}
}

func TestValidateRejectsNegativeWeight(t *testing.T) {
	p := policy(v1alpha1.SandboxPlacementPolicySpec{
		Scorers: []v1alpha1.ScorerConfig{{Name: "Cost", Weight: -1}},
	})
	if err := p.Validate(); err == nil {
		t.Fatal("negative weight should be rejected")
	}
}

// --- profile building ------------------------------------------------------

func TestBuildProfileProducesAWorkingScheduler(t *testing.T) {
	cands := []framework.Candidate{
		{Provider: "civo", Reachable: true, WarmCapacity: 3, CostPerHour: 1},
		{Provider: "gke", Reachable: true, WarmCapacity: 8, CostPerHour: 4},
	}
	prof, err := v1alpha1.DefaultPolicy().BuildProfile(cands)
	if err != nil {
		t.Fatalf("building the default policy failed: %v", err)
	}
	got, err := prof.Schedule(context.Background(), &framework.Request{}, cands)
	if err != nil {
		t.Fatalf("scheduling failed: %v", err)
	}
	if got.Provider != "civo" {
		t.Fatalf("placed on %s, want civo:\n%s", got.Provider, got.Explain())
	}
}

func TestBuildProfileRefusesAnInvalidPolicy(t *testing.T) {
	p := policy(v1alpha1.SandboxPlacementPolicySpec{Filters: []string{"Nope"}})
	if _, err := p.BuildProfile(nil); err == nil {
		t.Fatal("BuildProfile must not compile an invalid policy")
	}
}

func TestZeroWeightDefaultsToOneRatherThanDisablingTheScorer(t *testing.T) {
	// An object built in Go, or created before the API default existed, would
	// otherwise get a silently disabled scorer.
	cands := []framework.Candidate{
		{Provider: "a", Reachable: true, WarmCapacity: 5},
		{Provider: "b", Reachable: true, WarmCapacity: 0},
	}
	p := policy(v1alpha1.SandboxPlacementPolicySpec{
		Scorers: []v1alpha1.ScorerConfig{{Name: "WarmCapacity"}}, // weight omitted
	})
	prof, err := p.BuildProfile(cands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := prof.Schedule(context.Background(), &framework.Request{}, cands)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Provider != "a" || got.Score == 0 {
		t.Fatalf("a zero weight should behave as 1, got %s score %d", got.Provider, got.Score)
	}
}

func TestCostScorerIsCalibratedPerPass(t *testing.T) {
	// Cost is relative to the most expensive candidate *in this set*, so the
	// same policy must produce different scores for different candidate sets.
	p := policy(v1alpha1.SandboxPlacementPolicySpec{
		Scorers: []v1alpha1.ScorerConfig{{Name: "Cost", Weight: 1}},
	})
	cheapSet := []framework.Candidate{
		{Provider: "a", Reachable: true, CostPerHour: 1},
		{Provider: "b", Reachable: true, CostPerHour: 2},
	}
	prof1, _ := p.BuildProfile(cheapSet)
	d1, _ := prof1.Schedule(context.Background(), &framework.Request{}, cheapSet)

	wideSet := []framework.Candidate{
		{Provider: "a", Reachable: true, CostPerHour: 1},
		{Provider: "b", Reachable: true, CostPerHour: 100},
	}
	prof2, _ := p.BuildProfile(wideSet)
	d2, _ := prof2.Schedule(context.Background(), &framework.Request{}, wideSet)

	if d1.Score >= d2.Score {
		t.Fatalf("a should score higher against a pricier field: %d vs %d", d1.Score, d2.Score)
	}
}

// --- requests --------------------------------------------------------------

func TestPolicyRequirementsOverrideTheSandboxes(t *testing.T) {
	// The policy is the operator's constraint; the sandbox's is a preference
	// from whoever asked. The operator wins.
	p := policy(v1alpha1.SandboxPlacementPolicySpec{
		Filters:  []string{"RequiredAttributes"},
		Requires: map[string]string{"runtime": "gvisor"},
	})
	req := p.BuildRequest("sb-1", map[string]string{"runtime": "runc", "gpu": "true"}, "civo")
	if req.Requires["runtime"] != "gvisor" {
		t.Fatalf("policy requirement should win, got %q", req.Requires["runtime"])
	}
	if req.Requires["gpu"] != "true" {
		t.Fatal("non-conflicting sandbox requirements should be preserved")
	}
	if req.PreferProvider != "civo" {
		t.Fatal("preferred provider should be carried through")
	}
}

func TestBuildRequestLeavesRequiresNilWhenThereAreNone(t *testing.T) {
	req := v1alpha1.DefaultPolicy().BuildRequest("sb-1", nil, "")
	if req.Requires != nil {
		t.Fatalf("want nil Requires, got %v", req.Requires)
	}
}

// --- policy selection ------------------------------------------------------

func TestSelectPolicyPrefersHigherPriority(t *testing.T) {
	policies := []v1alpha1.SandboxPlacementPolicy{
		{ObjectMeta: metav1.ObjectMeta{Name: "base"}, Spec: v1alpha1.SandboxPlacementPolicySpec{Priority: 0}},
		{ObjectMeta: metav1.ObjectMeta{Name: "override"}, Spec: v1alpha1.SandboxPlacementPolicySpec{Priority: 10}},
	}
	got, err := v1alpha1.SelectPolicy(policies, map[string]string{"team": "a"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Name != "override" {
		t.Fatalf("selected %s, want override", got.Name)
	}
}

func TestSelectPolicyTieBreaksDeterministically(t *testing.T) {
	// Otherwise the governing policy depends on list ordering and changes as
	// objects come and go — indistinguishable from a scheduler bug.
	policies := []v1alpha1.SandboxPlacementPolicy{
		{ObjectMeta: metav1.ObjectMeta{Name: "zeta"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "alpha"}},
	}
	for i := 0; i < 20; i++ {
		got, err := v1alpha1.SelectPolicy(policies, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Name != "alpha" {
			t.Fatalf("iteration %d selected %s; ties must resolve to the lowest name", i, got.Name)
		}
	}
}

func TestSelectPolicyHonoursLabelSelectors(t *testing.T) {
	policies := []v1alpha1.SandboxPlacementPolicy{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "gpu-only"},
			Spec: v1alpha1.SandboxPlacementPolicySpec{
				Priority: 10,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"workload": "gpu"}},
			},
		},
		{ObjectMeta: metav1.ObjectMeta{Name: "catch-all"}},
	}

	got, _ := v1alpha1.SelectPolicy(policies, map[string]string{"workload": "gpu"})
	if got.Name != "gpu-only" {
		t.Fatalf("selected %s, want gpu-only", got.Name)
	}
	// A higher-priority policy that does not match must not win.
	got, _ = v1alpha1.SelectPolicy(policies, map[string]string{"workload": "cpu"})
	if got.Name != "catch-all" {
		t.Fatalf("selected %s, want catch-all", got.Name)
	}
}

func TestSelectPolicyReturnsNilWhenNothingMatches(t *testing.T) {
	policies := []v1alpha1.SandboxPlacementPolicy{{
		ObjectMeta: metav1.ObjectMeta{Name: "narrow"},
		Spec: v1alpha1.SandboxPlacementPolicySpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "a"}},
		},
	}}
	got, err := v1alpha1.SelectPolicy(policies, map[string]string{"team": "b"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("want nil so the caller can fall back to the default, got %s", got.Name)
	}
}

func TestSelectPolicyReportsAMalformedSelector(t *testing.T) {
	policies := []v1alpha1.SandboxPlacementPolicy{{
		ObjectMeta: metav1.ObjectMeta{Name: "broken"},
		Spec: v1alpha1.SandboxPlacementPolicySpec{
			Selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{Key: "x", Operator: "NotAnOperator"},
				},
			},
		},
	}}
	_, err := v1alpha1.SelectPolicy(policies, nil)
	if err == nil || !strings.Contains(err.Error(), "broken") {
		t.Fatalf("want an error naming the offending policy, got %v", err)
	}
}

// --- provider conversion ---------------------------------------------------

func TestProviderCandidateUsesDeclaredNotObservedAttributes(t *testing.T) {
	// The security property: a provider must not be able to assert that it
	// isolates untrusted code. Only the operator declares that.
	qty := resource.MustParse("2.5")
	p := &v1alpha1.SandboxProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "civo"},
		Spec: v1alpha1.SandboxProviderSpec{
			Attributes:  map[string]string{"runtime": "runc"},
			CostPerHour: &qty,
		},
		Status: v1alpha1.SandboxProviderStatus{
			WarmCapacity:       4,
			ObservedAttributes: map[string]string{"runtime": "gvisor"},
		},
	}
	c := p.ProviderCandidate(true)
	if c.Attr("runtime") != "runc" {
		t.Fatalf("candidate took the self-reported attribute %q; spec must win", c.Attr("runtime"))
	}
	if c.WarmCapacity != 4 {
		t.Fatalf("warm capacity %d, want 4", c.WarmCapacity)
	}
	if c.CostPerHour != 2.5 {
		t.Fatalf("cost %v, want 2.5", c.CostPerHour)
	}
	if c.Provider != "civo" || !c.Reachable {
		t.Fatalf("unexpected candidate: %+v", c)
	}
}

func TestProviderCandidateCopiesAttributes(t *testing.T) {
	p := &v1alpha1.SandboxProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "civo"},
		Spec:       v1alpha1.SandboxProviderSpec{Attributes: map[string]string{"region": "nyc1"}},
	}
	c := p.ProviderCandidate(true)
	c.Attributes["region"] = "tampered"
	if p.Spec.Attributes["region"] != "nyc1" {
		t.Fatal("mutating a candidate changed the provider object")
	}
}

func TestProviderWithoutCostIsFree(t *testing.T) {
	p := &v1alpha1.SandboxProvider{ObjectMeta: metav1.ObjectMeta{Name: "x"}}
	if got := p.ProviderCandidate(false).CostPerHour; got != 0 {
		t.Fatalf("cost %v, want 0 when unset", got)
	}
}

func TestKnownPluginNamesAreDiscoverable(t *testing.T) {
	// These strings are the API surface an operator types into YAML; they need
	// to be listable for error messages and docs.
	filters := v1alpha1.KnownFilterNames()
	scorers := v1alpha1.KnownScorerNames()
	if len(filters) == 0 || len(scorers) == 0 {
		t.Fatal("plugin names must be discoverable")
	}
	if !containsStr(scorers, "Cost") {
		t.Fatal("Cost is constructed specially but must still be listed as available")
	}
}

func containsStr(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}
