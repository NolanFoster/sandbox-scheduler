package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SandboxProviderSpec describes a place sandboxes can run.
//
// A provider is not necessarily a Kubernetes cluster. That is the whole point:
// the obvious next destinations for agent sandboxes are hosted APIs that have
// no cluster at all, and a placement layer that can only see clusters would
// exclude them by construction.
type SandboxProviderSpec struct {
	// Adapter names the integration used to talk to this provider, e.g.
	// "agent-sandbox" for a Kubernetes cluster running the upstream controller.
	// Unknown adapters are reported in status rather than rejected, so a newer
	// provider type does not break an older scheduler.
	// +kubebuilder:validation:MinLength=1
	Adapter string `json:"adapter"`

	// Endpoint is the adapter-specific address of the provider: a gateway URL,
	// an API base, a kube-apiserver.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// CredentialsRef points at a Secret in the scheduler's namespace holding
	// whatever the adapter needs to authenticate.
	//
	// By reference, never inline: a credential in a CR spec is readable by
	// anyone with get on the CRD, ends up in `kubectl get -o yaml` output, and
	// is copied into every backup of etcd that anyone takes.
	// +optional
	CredentialsRef *corev1.LocalObjectReference `json:"credentialsRef,omitempty"`

	// Attributes are operator-declared facts about this provider that filters
	// and scorers match on: runtime=gvisor, gpu=true, region=nyc1.
	//
	// Declared by the operator and NOT self-reported by the provider, which is
	// deliberate and security-relevant. `runtime=gvisor` is an assertion that
	// untrusted code is kernel-isolated here; a provider that could assert that
	// about itself could claim an isolation property it does not have, and the
	// filter that exists to keep untrusted workloads off weak providers would
	// wave them through. What a provider reports about itself lands in
	// status.observedAttributes, for humans.
	// +optional
	Attributes map[string]string `json:"attributes,omitempty"`

	// CostPerHour is the relative price of a sandbox-hour here.
	//
	// Units are the operator's own and need not be a currency — only the
	// ordering between providers is used. This is what lets a per-second hosted
	// API and a self-hosted cluster's amortised node cost be compared without
	// inventing a shared unit for them.
	// +optional
	CostPerHour *resource.Quantity `json:"costPerHour,omitempty"`

	// RefreshInterval is how often capacity is polled. Defaults to 10s.
	//
	// Polling is off the placement path, so this trades freshness against load
	// on the provider, never placement latency.
	// +optional
	RefreshInterval *metav1.Duration `json:"refreshInterval,omitempty"`

	// Disabled stops this provider being considered without deleting it.
	//
	// Distinct from deletion on purpose: draining a provider for maintenance
	// should not lose its configuration, and should be one field flip to undo.
	// +optional
	Disabled bool `json:"disabled,omitempty"`
}

// SandboxProviderStatus is the scheduler's view of a provider.
type SandboxProviderStatus struct {
	// Conditions follow the standard Kubernetes convention. "Ready" means the
	// last capacity report succeeded and is not stale.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// WarmCapacity is the pre-warmed sandboxes available at the last report.
	// +optional
	WarmCapacity int32 `json:"warmCapacity,omitempty"`

	// LastReportTime is when capacity was last successfully read. Absent means
	// the provider has never been reached — which is a different situation from
	// a provider that has gone quiet, and worth being able to tell apart.
	// +optional
	LastReportTime *metav1.Time `json:"lastReportTime,omitempty"`

	// LastError is the most recent failure. Retained across successful reports
	// only until the next success, so a flapping provider is visible rather
	// than looking healthy between failures.
	// +optional
	LastError string `json:"lastError,omitempty"`

	// ObservedAttributes are facts the provider reported about itself.
	//
	// Informational only. Policy matches on spec.attributes; see the note
	// there for why self-reported facts are not trusted for placement.
	// +optional
	ObservedAttributes map[string]string `json:"observedAttributes,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=sbp
// +kubebuilder:printcolumn:name="Adapter",type=string,JSONPath=`.spec.adapter`
// +kubebuilder:printcolumn:name="Warm",type=integer,JSONPath=`.status.warmCapacity`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SandboxProvider is a destination sandboxes can be placed on.
//
// Cluster-scoped: a provider is infrastructure shared across namespaces, and
// namespacing it would mean re-declaring the same cluster for every team.
type SandboxProvider struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SandboxProviderSpec   `json:"spec,omitempty"`
	Status SandboxProviderStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SandboxProviderList contains a list of SandboxProvider.
type SandboxProviderList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SandboxProvider `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SandboxProvider{}, &SandboxProviderList{})
}
