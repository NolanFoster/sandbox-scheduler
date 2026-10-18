package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TemplateMapping maps a provider-neutral template name to a provider-native
// reference.
type TemplateMapping struct {
	// Name is the provider-neutral template name requested by callers.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	// +kubebuilder:validation:MaxLength=63
	Name string `json:"name"`

	// Ref is the provider-native reference: a SandboxWarmPool name for
	// agent-sandbox (cold-only use is a pool with replicas: 0), or a template
	// ID on a hosted API.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Ref string `json:"ref"`
}

// ClaimsSpec configures scheduler-managed sandbox claiming for one provider.
type ClaimsSpec struct {
	// Enabled opts this provider into scheduler-managed sandbox claiming when
	// the scheduler runs with --enable-claims.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MaxInFlight bounds concurrent claim and bind-wait operations against
	// this provider. Defaults to 512.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4096
	// +optional
	MaxInFlight *int32 `json:"maxInFlight,omitempty"`

	// Timeout bounds a single claim or lookup attempt against this provider.
	// Defaults to 3s.
	// +kubebuilder:validation:XValidation:rule="duration(self) >= duration('1s') && duration(self) <= duration('5m')",message="timeout must be between 1s and 5m"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// MaxRate bounds cold claims per second on cluster providers and all
	// claims per second on elastic providers. Defaults to 50 for clusters and
	// 100 for elastic providers.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxRate *int32 `json:"maxRate,omitempty"`

	// LookupRate bounds consistent idempotency lookups per second against this
	// provider during retry probes. Defaults to 200 for clusters and 20 for
	// elastic providers.
	// +kubebuilder:validation:Minimum=1
	// +optional
	LookupRate *int32 `json:"lookupRate,omitempty"`

	// ElasticWarmScore is the WarmCapacity score (0..100) assigned to an
	// elastic provider when it has no pre-warmed pool of its own. Defaults to
	// 20 so elastic overflow loses to warm pools and reasonably priced cold
	// clusters rather than absorbing all overflow at per-use rates.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	// +optional
	ElasticWarmScore *int32 `json:"elasticWarmScore,omitempty"`
}

// ClusterProfileReference points at a SIG-Multicluster ClusterProfile.
type ClusterProfileReference struct {
	// Namespace of the ClusterProfile. Must be in the scheduler's
	// --cluster-profile-namespaces allowlist; defaults to the scheduler's
	// secret namespace when omitted.
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Name of the ClusterProfile object.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// KubeAPISpec holds Kubernetes-specific settings for providers accessed via a
// remote kube-apiserver.
type KubeAPISpec struct {
	// Namespace where SandboxClaims are created on the remote cluster.
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	Namespace string `json:"namespace"`
}

// ClaimsStatus reports the state of scheduler-managed claiming on a provider.
type ClaimsStatus struct {
	// LastSuccessTime is when a claim or half-open recovery probe last
	// succeeded against this provider.
	// +optional
	LastSuccessTime *metav1.Time `json:"lastSuccessTime,omitempty"`

	// LastFailureReason is the most recent claim failure reason when the claim
	// path is degraded or rejected.
	// +optional
	LastFailureReason string `json:"lastFailureReason,omitempty"`
}

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

	// ClusterProfileRef references a SIG-Multicluster ClusterProfile supplying
	// cluster access for the kubeapi transport. Preferred over raw kubeconfig
	// Secrets when available.
	// +optional
	ClusterProfileRef *ClusterProfileReference `json:"clusterProfileRef,omitempty"`

	// KubeAPI configures Kubernetes-specific claim settings when talking to a
	// remote cluster's API server.
	// +optional
	KubeAPI *KubeAPISpec `json:"kubeapi,omitempty"`

	// Templates maps provider-neutral template names to provider-native
	// references on this provider.
	// +listType=map
	// +listMapKey=name
	// +kubebuilder:validation:MaxItems=256
	// +optional
	Templates []TemplateMapping `json:"templates,omitempty"`

	// Claims configures optional scheduler-managed sandbox claiming on this
	// provider.
	// +optional
	Claims *ClaimsSpec `json:"claims,omitempty"`

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
	// last capacity report succeeded and is not stale; "Claiming" reports
	// whether scheduler-managed claims are active and healthy.
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

	// Claims reports the state of scheduler-managed claiming on this provider.
	// +optional
	Claims *ClaimsStatus `json:"claims,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=sbp
// +kubebuilder:printcolumn:name="Adapter",type=string,JSONPath=`.spec.adapter`
// +kubebuilder:printcolumn:name="Warm",type=integer,JSONPath=`.status.warmCapacity`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Claiming",type=string,JSONPath=`.status.conditions[?(@.type=="Claiming")].status`
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
