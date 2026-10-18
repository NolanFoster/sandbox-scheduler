// Package controller reconciles the placement API onto the running scheduler.
package controller

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

// Condition types and reasons on SandboxProvider.
const (
	ConditionReady = "Ready"

	ReasonReporting      = "Reporting"
	ReasonDisabled       = "Disabled"
	ReasonUnknownAdapter = "UnknownAdapter"
	ReasonCredentials    = "CredentialsUnavailable"
	ReasonAdapterConfig  = "AdapterConfigInvalid"
	ReasonNoReport       = "NoReportYet"
	ReasonStale          = "ReportStale"
	ReasonFetchFailed    = "FetchFailed"
)

// defaultStatusInterval is how often a healthy provider is re-reconciled to
// refresh its status. Independent of how often capacity is *polled* — the
// registry does that on its own schedule. This only controls how quickly
// `kubectl get sandboxproviders` catches up.
const defaultStatusInterval = 15 * time.Second

// SandboxProviderReconciler keeps the capacity registry in step with the
// SandboxProvider objects in the cluster.
//
// It owns registration, not polling. The registry polls on its own schedule so
// that capacity freshness is independent of reconcile timing — a controller
// backed up on a slow API server must not also mean stale placement data.
type SandboxProviderReconciler struct {
	client.Client
	Registry *registry.Registry

	// SecretNamespace is where credentialsRef Secrets are read from.
	// Providers are cluster-scoped, so the reference has no namespace of its
	// own; confining it to the scheduler's own namespace means a provider
	// cannot be pointed at an arbitrary Secret elsewhere in the cluster.
	SecretNamespace string

	// StatusInterval overrides defaultStatusInterval in tests.
	StatusInterval time.Duration

	// WriteStatus, when set, reports whether this replica may write status.
	//
	// The reconciler runs on every replica, not only the leader, because it
	// is what fills the registry: a standby with an empty registry would
	// take over unable to place anything until it had reconciled and polled
	// every provider. Status, though, is written by the leader alone, so
	// replicas do not overwrite each other. Nil means always.
	WriteStatus func() bool

	// built records, per provider, a digest of the adapter configuration its
	// registered source was built from. Most reconciles are status ticks that
	// change nothing; rebuilding and re-registering the source on each would
	// restart its polling schedule and discard whatever the adapter holds.
	mu    sync.Mutex
	built map[string][sha256.Size]byte
}

// readiness is the Ready condition a reconcile arrives at.
type readiness struct {
	status  metav1.ConditionStatus
	reason  string
	message string
}

// +kubebuilder:rbac:groups=placement.agents.x-k8s.io,resources=sandboxproviders,verbs=get;list;watch
// +kubebuilder:rbac:groups=placement.agents.x-k8s.io,resources=sandboxproviders/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile brings one provider's registration and status up to date.
func (r *SandboxProviderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var provider v1alpha1.SandboxProvider
	if err := r.Get(ctx, req.NamespacedName, &provider); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted. Drop it from the registry so it stops being a candidate.
			// No finalizer: the only state to clean up is in memory, and if the
			// process dies first, startup rebuilds the registry from whatever
			// providers still exist.
			r.deregister(req.Name)
			logger.Info("provider deleted, removed from registry", "provider", req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	observed := provider.Status.DeepCopy()
	ready := r.sync(ctx, &provider)
	return r.writeStatus(ctx, &provider, observed, ready)
}

// sync registers the provider as its spec says and returns its readiness,
// filling in the rest of its status along the way.
func (r *SandboxProviderReconciler) sync(ctx context.Context, provider *v1alpha1.SandboxProvider) readiness {
	if provider.Spec.Disabled {
		// Drained, not deleted. Configuration is preserved and one field flip
		// brings it back.
		r.deregister(provider.Name)
		return readiness{metav1.ConditionFalse, ReasonDisabled,
			"provider is disabled and will not be considered for placement"}
	}

	if !adapter.Has(provider.Spec.Adapter) {
		// Fail visibly rather than never polling it. An unknown adapter usually
		// means a typo or a scheduler older than the config.
		r.deregister(provider.Name)
		return readiness{metav1.ConditionFalse, ReasonUnknownAdapter,
			fmt.Sprintf("unknown adapter %q (available: %v)", provider.Spec.Adapter, adapter.Names())}
	}

	creds, err := r.resolveCredentials(ctx, provider)
	if err != nil {
		// Keep polling with what we had rather than dropping the provider: a
		// Secret that is briefly unreadable should not evacuate a cluster.
		return readiness{metav1.ConditionFalse, ReasonCredentials, err.Error()}
	}

	cfg := adapter.Config{
		ProviderID:  provider.Name,
		Endpoint:    provider.Spec.Endpoint,
		Credentials: creds,
		Options:     providerOptions(provider),
	}
	digest := configDigest(provider.Spec.Adapter, cfg)
	if !r.isBuilt(provider.Name, digest) {
		source, err := adapter.New(provider.Spec.Adapter, cfg)
		if err != nil {
			r.deregister(provider.Name)
			return readiness{metav1.ConditionFalse, ReasonAdapterConfig, err.Error()}
		}
		// Re-registering keeps any capacity already known, so a changed
		// endpoint or rotated credential does not blind the scheduler to this
		// provider until the new source's first poll.
		r.Registry.AddSource(source)
		r.setBuilt(provider.Name, digest)
	}

	var interval time.Duration
	if provider.Spec.RefreshInterval != nil {
		interval = provider.Spec.RefreshInterval.Duration
	}
	r.Registry.SetRefreshInterval(provider.Name, interval)

	// The trusted half. Attributes and cost come from the spec — written by
	// someone with permission to write it — and never from what the provider
	// reports about itself. See registry.ProviderConfig.
	pc := registry.ProviderConfig{Attributes: provider.Spec.Attributes}
	if provider.Spec.CostPerHour != nil {
		pc.CostPerHour = provider.Spec.CostPerHour.AsApproximateFloat64()
	}
	r.Registry.SetConfig(provider.Name, pc)

	return r.registryReadiness(provider)
}

// deregister stops polling a provider and forgets the source built for it.
func (r *SandboxProviderReconciler) deregister(name string) {
	r.Registry.RemoveSource(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.built, name)
}

// isBuilt reports whether the registered source was built from digest. The
// registry is checked too, so a source it no longer has is rebuilt whatever
// this reconciler remembers.
func (r *SandboxProviderReconciler) isBuilt(name string, digest [sha256.Size]byte) bool {
	r.mu.Lock()
	d, ok := r.built[name]
	r.mu.Unlock()
	return ok && d == digest && r.Registry.HasSource(name)
}

func (r *SandboxProviderReconciler) setBuilt(name string, digest [sha256.Size]byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.built == nil {
		r.built = map[string][sha256.Size]byte{}
	}
	r.built[name] = digest
}

// configDigest identifies everything a source is built from, so a change to
// any of it, a rotated credential included, rebuilds the source and nothing
// else does. A digest rather than a copy, so credentials are not held twice.
func configDigest(adapterName string, cfg adapter.Config) [sha256.Size]byte {
	h := sha256.New()
	field := func(b []byte) {
		// Length-prefixed so that no two different configurations can
		// concatenate to the same bytes.
		_ = binary.Write(h, binary.BigEndian, uint64(len(b)))
		h.Write(b)
	}
	field([]byte(adapterName))
	field([]byte(cfg.ProviderID))
	field([]byte(cfg.Endpoint))
	writeMap(h, field, cfg.Options, func(v string) []byte { return []byte(v) })
	writeMap(h, field, cfg.Credentials, func(v []byte) []byte { return v })
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func writeMap[V any](h hash.Hash, field func([]byte), m map[string]V, bytes func(V) []byte) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	_ = binary.Write(h, binary.BigEndian, uint64(len(keys)))
	for _, k := range keys {
		field([]byte(k))
		field(bytes(m[k]))
	}
}

// resolveCredentials reads the referenced Secret, if any.
func (r *SandboxProviderReconciler) resolveCredentials(ctx context.Context, p *v1alpha1.SandboxProvider) (map[string][]byte, error) {
	if p.Spec.CredentialsRef == nil {
		return nil, nil
	}
	if r.SecretNamespace == "" {
		return nil, fmt.Errorf("credentialsRef is set but the scheduler has no secret namespace configured")
	}
	var secret corev1.Secret
	key := types.NamespacedName{Namespace: r.SecretNamespace, Name: p.Spec.CredentialsRef.Name}
	if err := r.Get(ctx, key, &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("secret %s/%s not found", key.Namespace, key.Name)
		}
		return nil, fmt.Errorf("reading secret %s/%s: %w", key.Namespace, key.Name, err)
	}
	return secret.Data, nil
}

// registryReadiness copies what the registry knows onto the object's status
// and says what it means for readiness.
func (r *SandboxProviderReconciler) registryReadiness(p *v1alpha1.SandboxProvider) readiness {
	var found *registry.Status
	for _, st := range r.Registry.Statuses() {
		if st.Provider == p.Name {
			s := st
			found = &s
			break
		}
	}
	if found == nil {
		return readiness{metav1.ConditionFalse, ReasonNoReport, "registered, but no capacity report yet"}
	}

	p.Status.WarmCapacity = int32(found.Report.WarmCapacity)
	p.Status.ObservedAttributes = found.Report.Attributes
	if found.LastError != nil {
		p.Status.LastError = found.LastError.Error()
	} else {
		p.Status.LastError = ""
	}
	if found.Reachable || found.Stale {
		// At the precision the API server stores, so an unchanged report
		// compares equal to what was last written and costs no write.
		t := metav1.NewTime(found.UpdatedAt).Rfc3339Copy()
		p.Status.LastReportTime = &t
	}

	switch {
	case found.Reachable && found.LastError != nil:
		// Reporting, but the most recent attempt failed. Still Ready — the data
		// is inside its freshness window — but the error is surfaced so a
		// flapping provider is visible rather than looking healthy.
		return readiness{metav1.ConditionTrue, ReasonReporting,
			fmt.Sprintf("serving last known capacity; most recent fetch failed: %s", found.LastError)}
	case found.Reachable:
		return readiness{metav1.ConditionTrue, ReasonReporting,
			fmt.Sprintf("%d warm sandboxes available", found.Report.WarmCapacity)}
	case found.Stale:
		// Since a time rather than for a duration: a duration would change the
		// message, and so cost a status write, on every reconcile for as long
		// as the provider stays down.
		return readiness{metav1.ConditionFalse, ReasonStale,
			fmt.Sprintf("no successful report since %s; last error: %v",
				found.UpdatedAt.UTC().Format(time.RFC3339), found.LastError)}
	case found.LastError != nil:
		return readiness{metav1.ConditionFalse, ReasonFetchFailed, found.LastError.Error()}
	default:
		return readiness{metav1.ConditionFalse, ReasonNoReport, "registered, but no capacity report yet"}
	}
}

// writeStatus records readiness on the object, writing only if its status
// differs from what was observed. Most reconciles are status ticks on a
// provider whose state has not moved; writing those anyway costs an API
// server round trip and an etcd revision each, and wakes every watcher of
// SandboxProviders, for nothing.
func (r *SandboxProviderReconciler) writeStatus(ctx context.Context, p *v1alpha1.SandboxProvider,
	observed *v1alpha1.SandboxProviderStatus, ready readiness) (ctrl.Result, error) {

	if r.WriteStatus != nil && !r.WriteStatus() {
		return ctrl.Result{RequeueAfter: r.statusInterval()}, nil
	}

	setCondition(&p.Status.Conditions, metav1.Condition{
		Type:               ConditionReady,
		Status:             ready.status,
		Reason:             ready.reason,
		Message:            ready.message,
		ObservedGeneration: p.Generation,
		LastTransitionTime: metav1.Now(),
	})
	if equality.Semantic.DeepEqual(observed, &p.Status) {
		return ctrl.Result{RequeueAfter: r.statusInterval()}, nil
	}

	if err := r.Status().Update(ctx, p); err != nil {
		if apierrors.IsConflict(err) {
			// Someone else wrote first. Requeue rather than fight: the next
			// pass reads fresh and converges.
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: r.statusInterval()}, nil
}

func (r *SandboxProviderReconciler) statusInterval() time.Duration {
	if r.StatusInterval > 0 {
		return r.StatusInterval
	}
	return defaultStatusInterval
}

func providerOptions(p *v1alpha1.SandboxProvider) map[string]string {
	// Adapter-specific settings currently ride on annotations, so a new adapter
	// option does not require an API change while the API is still v1alpha1.
	opts := map[string]string{}
	for k, v := range p.Annotations {
		const prefix = "placement.agents.x-k8s.io/option-"
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			opts[k[len(prefix):]] = v
		}
	}
	return opts
}

// setCondition upserts a condition, preserving LastTransitionTime when only the
// message changed. Without that, every reconcile would rewrite the timestamp
// and "how long has this been broken?" would always answer "a few seconds".
func setCondition(conditions *[]metav1.Condition, next metav1.Condition) {
	for i := range *conditions {
		if (*conditions)[i].Type != next.Type {
			continue
		}
		if (*conditions)[i].Status == next.Status {
			next.LastTransitionTime = (*conditions)[i].LastTransitionTime
		}
		(*conditions)[i] = next
		return
	}
	*conditions = append(*conditions, next)
}

// SetupWithManager registers the controller.
//
// Secrets in SecretNamespace are watched too, so a rotated or newly created
// credential reaches the providers that reference it within a reconcile
// rather than at their next status tick.
func (r *SandboxProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	everyReplica := false
	inSecretNamespace := predicate.NewPredicateFuncs(func(o client.Object) bool {
		return r.SecretNamespace != "" && o.GetNamespace() == r.SecretNamespace
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.SandboxProvider{}).
		Watches(&corev1.Secret{},
			handler.EnqueueRequestsFromMapFunc(r.providersForSecret),
			builder.WithPredicates(inSecretNamespace)).
		WithOptions(crcontroller.Options{NeedLeaderElection: &everyReplica}).
		Complete(r)
}

// providersForSecret maps a Secret to the providers whose credentialsRef
// names it.
func (r *SandboxProviderReconciler) providersForSecret(ctx context.Context, secret client.Object) []reconcile.Request {
	if secret.GetNamespace() != r.SecretNamespace {
		return nil
	}
	var providers v1alpha1.SandboxProviderList
	if err := r.List(ctx, &providers); err != nil {
		log.FromContext(ctx).Error(err, "listing providers for a changed secret", "secret", secret.GetName())
		return nil
	}
	var out []reconcile.Request
	for _, p := range providers.Items {
		if ref := p.Spec.CredentialsRef; ref != nil && ref.Name == secret.GetName() {
			out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Name: p.Name}})
		}
	}
	return out
}
