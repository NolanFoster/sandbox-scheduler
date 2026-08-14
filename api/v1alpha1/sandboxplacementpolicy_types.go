package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ScorerConfig enables a scorer and sets how much it counts.
type ScorerConfig struct {
	// Name of a registered scorer: WarmCapacity, Cost, Reachability, Affinity.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Weight multiplies this scorer's normalized 0..100 output.
	//
	// Weights are the whole tuning surface. A scorer cannot increase its own
	// influence by returning bigger numbers — the framework clamps to 100 — so
	// relative importance is decided here and only here.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=1
	Weight int64 `json:"weight,omitempty"`
}

// SandboxPlacementPolicySpec is a named scheduling profile.
type SandboxPlacementPolicySpec struct {
	// Selector chooses which sandboxes this policy governs. An empty selector
	// matches everything, which is what makes a single cluster-wide default
	// policy expressible.
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`

	// Priority breaks ties when several policies match the same sandbox;
	// highest wins. Without it, "which policy applied?" would depend on
	// resource ordering, and the answer would change as objects are created
	// and deleted.
	// +optional
	// +kubebuilder:default=0
	Priority int32 `json:"priority,omitempty"`

	// Filters are hard constraints, by name: RequiredAttributes, Reachable.
	//
	// Note that omitting Reachable is a legitimate and often correct choice:
	// a provider can miss heartbeats while still accepting claims, and
	// excluding it makes placement fail for want of a heartbeat. Leaving it out
	// and relying on the Reachability scorer demotes instead.
	// +optional
	Filters []string `json:"filters,omitempty"`

	// Scorers are weighted preferences applied to whatever survives filtering.
	// +optional
	Scorers []ScorerConfig `json:"scorers,omitempty"`

	// Requires are attribute requirements applied to every sandbox this policy
	// governs, e.g. {"runtime": "gvisor"} to keep a namespace's workloads on
	// kernel-isolated providers. Enforced by the RequiredAttributes filter, so
	// setting this without that filter has no effect — the controller reports
	// that as a condition rather than silently ignoring it.
	// +optional
	Requires map[string]string `json:"requires,omitempty"`
}

// SandboxPlacementPolicyStatus reports whether a policy is usable.
type SandboxPlacementPolicyStatus struct {
	// Conditions follow the standard convention. "Valid" is false when the
	// policy names a filter or scorer this scheduler does not implement.
	//
	// Invalid policies are rejected rather than partially applied: silently
	// dropping an unknown filter would place workloads under weaker
	// constraints than the operator wrote down, which is the worst possible
	// failure for a policy engine.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=spp
// +kubebuilder:printcolumn:name="Priority",type=integer,JSONPath=`.spec.priority`
// +kubebuilder:printcolumn:name="Valid",type=string,JSONPath=`.status.conditions[?(@.type=="Valid")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SandboxPlacementPolicy declares how sandboxes are placed.
type SandboxPlacementPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SandboxPlacementPolicySpec   `json:"spec,omitempty"`
	Status SandboxPlacementPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SandboxPlacementPolicyList contains a list of SandboxPlacementPolicy.
type SandboxPlacementPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxPlacementPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SandboxPlacementPolicy{}, &SandboxPlacementPolicyList{})
}
