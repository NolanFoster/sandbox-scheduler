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
	eventsv1 "k8s.io/api/events/v1"
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
	"github.com/NolanFoster/sandbox-scheduler/pkg/binder"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
	"github.com/NolanFoster/sandbox-scheduler/pkg/reaper"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

// Condition types and reasons on SandboxProvider.
const (
	ConditionReady    = "Ready"
	ConditionClaiming = "Claiming"

	ReasonReporting      = "Reporting"
	ReasonDisabled       = "Disabled"
	ReasonUnknownAdapter = "UnknownAdapter"
	ReasonCredentials    = "CredentialsUnavailable"
	ReasonAdapterConfig  = "AdapterConfigInvalid"
	ReasonNoReport       = "NoReportYet"
	ReasonStale          = "ReportStale"
	ReasonFetchFailed    = "FetchFailed"

	ReasonClaimCapable     = "ClaimCapable"
	ReasonClaimsDisabled   = "ClaimsDisabled"
	ReasonNotClaimCapable  = "AdapterPollOnly"
	ReasonClaimCircuitOpen = "CircuitOpen"
	ReasonClaimRejected    = "ClaimRejected"
	ReasonReapDryRun       = "ReapDryRun"

	reportingController = "sandbox-scheduler.placement.agents.x-k8s.io"
	defaultEventNS      = "default"
)

// defaultStatusInterval is how often a healthy provider is re-reconciled to
// refresh its status. Independent of how often capacity is *polled* — the
// registry does that on its own schedule. This only controls how quickly
// `kubectl get sandboxproviders` catches up.
const defaultStatusInterval = 15 * time.Second

// SandboxProviderReconciler keeps the capacity registry and claim binder in
// step with the SandboxProvider objects in the cluster.
//
// It owns registration, not polling. The registry polls on its own schedule so
// that capacity freshness is independent of reconcile timing — a controller
// backed up on a slow API server must not also mean stale placement data.
type SandboxProviderReconciler struct {
	client.Client
	Registry *registry.Registry
	Binder   *binder.Binder
	Reaper   *reaper.Reaper

	// SecretNamespace is where credentialsRef Secrets are read from.
	// Providers are cluster-scoped, so the reference has no namespace of its
	// own; confining it to the scheduler's own namespace means a provider
	// cannot be pointed at an arbitrary Secret elsewhere in the cluster.
	SecretNamespace string

	// EventInstance identifies the reporting pod/replica on emitted
	// events.k8s.io/v1 Events. Defaults to "sandbox-scheduler" when empty.
	EventInstance string

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
	mu        sync.Mutex
	built     map[string][sha256.Size]byte
	providers map[string]provider.Provider
}

// readiness is the Ready condition a reconcile arrives at.
type readiness struct {
	status  metav1.ConditionStatus
	reason  string
	message string
}

// +kubebuilder:rbac:groups=placement.agents.x-k8s.io,resources=sandboxproviders,verbs=get;list;watch
// +kubebuilder:rbac:groups=placement.agents.x-k8s.io,resources=sandboxproviders/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",namespace=sandbox-scheduler,resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=namespaces,resourceNames=kube-system,verbs=get
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile brings one provider's registration and status up to date.
func (r *SandboxProviderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var sp v1alpha1.SandboxProvider
	if err := r.Get(ctx, req.NamespacedName, &sp); err != nil {
		if apierrors.IsNotFound(err) {
			// Deleted. Drop it from the registry and binder so it stops being a
			// candidate. No finalizer: the only state to clean up is in memory,
			// and if the process dies first, startup rebuilds the registry from
			// whatever providers still exist.
			r.deregister(req.Name)
			logger.Info("provider deleted, removed from registry", "provider", req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	observed := sp.Status.DeepCopy()
	ready, claiming := r.sync(ctx, &sp)
	return r.writeStatus(ctx, &sp, observed, ready, claiming)
}

// sync registers the provider as its spec says and returns its Ready and
// Claiming conditions, filling in the rest of its status along the way.
func (r *SandboxProviderReconciler) sync(ctx context.Context, p *v1alpha1.SandboxProvider) (readiness, *readiness) {
	if !adapter.Has(p.Spec.Adapter) {
		// Fail visibly rather than never polling it. An unknown adapter usually
		// means a typo or a scheduler older than the config.
		r.deregister(p.Name)
		msg := fmt.Sprintf("unknown adapter %q (available: %v)", p.Spec.Adapter, adapter.Names())
		return readiness{metav1.ConditionFalse, ReasonUnknownAdapter, msg},
			&readiness{metav1.ConditionFalse, ReasonUnknownAdapter, msg}
	}

	if p.Spec.Disabled {
		// Drained, not deleted. Remove from placement registry and claim binder
		// while keeping lifecycle reaping active for any existing owned sandboxes.
		r.Registry.RemoveSource(p.Name)
		if r.Binder != nil {
			r.Binder.RemoveTarget(p.Name)
		}
		r.syncReaperWhenDisabled(ctx, p)
		return readiness{metav1.ConditionFalse, ReasonDisabled,
				"provider is disabled and will not be considered for placement"},
			&readiness{metav1.ConditionFalse, ReasonDisabled, "provider is disabled"}
	}

	claimsEnabled := p.Spec.Claims != nil && p.Spec.Claims.Enabled
	if !claimsEnabled {
		if r.Binder != nil {
			r.Binder.RemoveTarget(p.Name)
		}
	}

	creds, err := r.resolveCredentials(ctx, p)
	if err != nil {
		// Keep polling with what we had rather than dropping the provider: a
		// Secret that is briefly unreadable should not evacuate a cluster, but
		// billable claims must stop immediately when credentials are missing.
		if r.Binder != nil {
			r.Binder.RemoveTarget(p.Name)
		}
		if r.Reaper != nil {
			r.Reaper.RemoveTarget(p.Name)
		}
		claiming := &readiness{
			status:  metav1.ConditionFalse,
			reason:  ReasonCredentials,
			message: err.Error(),
		}
		if !claimsEnabled {
			claiming = &readiness{
				status:  metav1.ConditionFalse,
				reason:  ReasonClaimsDisabled,
				message: "claims are not enabled on this provider (set spec.claims.enabled=true)",
			}
		} else if r.Binder == nil {
			claiming = &readiness{
				status:  metav1.ConditionFalse,
				reason:  ReasonClaimsDisabled,
				message: "sandbox claims are disabled on the scheduler (start with --enable-claims)",
			}
		}
		return readiness{metav1.ConditionFalse, ReasonCredentials, err.Error()}, claiming
	}

	templates := templateMap(p.Spec.Templates)
	opts := providerOptions(p)
	if p.Spec.KubeAPI != nil && p.Spec.KubeAPI.Namespace != "" {
		if _, exists := opts["namespace"]; !exists {
			opts["namespace"] = p.Spec.KubeAPI.Namespace
		}
	}

	cfg := adapter.Config{
		ProviderID:  p.Name,
		Endpoint:    p.Spec.Endpoint,
		Credentials: creds,
		Options:     opts,
		Templates:   templates,
	}
	digest := configDigest(p.Spec.Adapter, cfg)
	prov, ok := r.builtProvider(p.Name, digest)
	if !ok {
		built, err := adapter.New(p.Spec.Adapter, cfg)
		if err != nil {
			r.deregister(p.Name)
			return readiness{metav1.ConditionFalse, ReasonAdapterConfig, err.Error()},
				&readiness{metav1.ConditionFalse, ReasonAdapterConfig, err.Error()}
		}
		prov = built
		// Re-registering keeps any capacity already known, so a changed
		// endpoint or rotated credential does not blind the scheduler to this
		// provider until the new source's first poll.
		r.Registry.AddSource(prov.Source)
		r.setBuilt(p.Name, digest, prov)
	}

	var interval time.Duration
	if p.Spec.RefreshInterval != nil {
		interval = p.Spec.RefreshInterval.Duration
	}
	r.Registry.SetRefreshInterval(p.Name, interval)

	// The trusted half. Attributes, templates, and cost come from the spec —
	// written by someone with permission to write it — and never from what the
	// provider reports about itself. See registry.ProviderConfig.
	pc := registry.ProviderConfig{
		Attributes: p.Spec.Attributes,
		Templates:  templates,
	}
	if p.Spec.CostPerHour != nil {
		pc.CostPerHour = p.Spec.CostPerHour.AsApproximateFloat64()
	}
	if p.Spec.Claims != nil && p.Spec.Claims.ElasticWarmScore != nil {
		score := int(*p.Spec.Claims.ElasticWarmScore)
		pc.ElasticWarmScore = &score
	}
	r.Registry.SetConfig(p.Name, pc)

	ready, isElastic := r.registryReadiness(p)
	claiming := r.syncClaims(p, prov, templates, isElastic)
	return ready, claiming
}

func (r *SandboxProviderReconciler) syncClaims(
	p *v1alpha1.SandboxProvider,
	prov provider.Provider,
	templates map[string]string,
	isElastic bool,
) *readiness {
	claimsEnabled := p.Spec.Claims != nil && p.Spec.Claims.Enabled

	tcfg := binder.TargetConfig{
		Enabled:   claimsEnabled && prov.Binder != nil,
		Elastic:   isElastic,
		Templates: templates,
	}
	if cs := p.Spec.Claims; cs != nil {
		if cs.MaxInFlight != nil && *cs.MaxInFlight > 0 {
			tcfg.MaxInFlight = int(*cs.MaxInFlight)
		}
		if cs.Timeout != nil && cs.Timeout.Duration > 0 {
			tcfg.Timeout = cs.Timeout.Duration
		}
		if cs.MaxRate != nil && *cs.MaxRate > 0 {
			tcfg.MaxRate = int(*cs.MaxRate)
		}
		if cs.LookupRate != nil && *cs.LookupRate > 0 {
			tcfg.LookupRate = int(*cs.LookupRate)
		}
		if cs.ElasticWarmScore != nil {
			score := int(*cs.ElasticWarmScore)
			tcfg.ElasticWarmScore = &score
		}
	}

	if r.Binder != nil {
		r.Binder.SetTarget(p.Name, prov, tcfg)
	}
	if r.Reaper != nil {
		r.Reaper.SetTarget(p.Name, prov, reaper.TargetConfig{
			Enabled: prov.Binder != nil && prov.Lister != nil,
			Timeout: tcfg.Timeout,
		})
	}

	if !claimsEnabled {
		return &readiness{
			status:  metav1.ConditionFalse,
			reason:  ReasonClaimsDisabled,
			message: "claims are not enabled on this provider (set spec.claims.enabled=true)",
		}
	}
	if r.Binder == nil {
		return &readiness{
			status:  metav1.ConditionFalse,
			reason:  ReasonClaimsDisabled,
			message: "sandbox claims are disabled on the scheduler (start with --enable-claims)",
		}
	}
	if prov.Binder == nil {
		return &readiness{
			status:  metav1.ConditionFalse,
			reason:  ReasonNotClaimCapable,
			message: fmt.Sprintf("adapter %q supports capacity polling only", p.Spec.Adapter),
		}
	}

	if r.Binder != nil {
		if hs, ok := r.Binder.HealthStatus(p.Name); ok {
			if !hs.LastSuccessTime.IsZero() || hs.LastFailureReason != "" {
				if p.Status.Claims == nil {
					p.Status.Claims = &v1alpha1.ClaimsStatus{}
				}
				if !hs.LastSuccessTime.IsZero() {
					t := metav1.NewTime(hs.LastSuccessTime).Rfc3339Copy()
					p.Status.Claims.LastSuccessTime = &t
				}
				p.Status.Claims.LastFailureReason = hs.LastFailureReason
			}
			if hs.CredentialsError {
				return &readiness{
					status:  metav1.ConditionFalse,
					reason:  ReasonClaimRejected,
					message: fmt.Sprintf("provider rejected recent claim: %s", hs.LastFailureReason),
				}
			}
			if !hs.Healthy {
				return &readiness{
					status:  metav1.ConditionFalse,
					reason:  ReasonClaimCircuitOpen,
					message: fmt.Sprintf("claim circuit breaker open: %s", hs.LastFailureReason),
				}
			}
		}
	}

	return &readiness{
		status:  metav1.ConditionTrue,
		reason:  ReasonClaimCapable,
		message: "provider is ready to serve sandbox claims",
	}
}

func (r *SandboxProviderReconciler) syncReaperWhenDisabled(ctx context.Context, p *v1alpha1.SandboxProvider) {
	if r.Reaper == nil {
		return
	}
	creds, err := r.resolveCredentials(ctx, p)
	if err != nil {
		r.Reaper.RemoveTarget(p.Name)
		return
	}
	templates := templateMap(p.Spec.Templates)
	opts := providerOptions(p)
	if p.Spec.KubeAPI != nil && p.Spec.KubeAPI.Namespace != "" {
		if _, exists := opts["namespace"]; !exists {
			opts["namespace"] = p.Spec.KubeAPI.Namespace
		}
	}
	cfg := adapter.Config{
		ProviderID:  p.Name,
		Endpoint:    p.Spec.Endpoint,
		Credentials: creds,
		Options:     opts,
		Templates:   templates,
	}
	digest := configDigest(p.Spec.Adapter, cfg)
	r.mu.Lock()
	d, ok := r.built[p.Name]
	prov := r.providers[p.Name]
	r.mu.Unlock()
	if !ok || d != digest {
		built, err := adapter.New(p.Spec.Adapter, cfg)
		if err != nil {
			r.deregister(p.Name)
			return
		}
		prov = built
		r.setBuilt(p.Name, digest, prov)
	}
	var timeout time.Duration
	if p.Spec.Claims != nil && p.Spec.Claims.Timeout != nil && p.Spec.Claims.Timeout.Duration > 0 {
		timeout = p.Spec.Claims.Timeout.Duration
	}
	r.Reaper.SetTarget(p.Name, prov, reaper.TargetConfig{
		Enabled: prov.Binder != nil && prov.Lister != nil,
		Timeout: timeout,
	})
}

// deregister stops polling a provider and forgets the source built for it.
func (r *SandboxProviderReconciler) deregister(name string) {
	r.Registry.RemoveSource(name)
	if r.Binder != nil {
		r.Binder.RemoveTarget(name)
	}
	if r.Reaper != nil {
		r.Reaper.RemoveTarget(name)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.built, name)
	delete(r.providers, name)
}

// builtProvider returns the cached provider.Provider if its digest matches and
// the registry still has its source.
func (r *SandboxProviderReconciler) builtProvider(name string, digest [sha256.Size]byte) (provider.Provider, bool) {
	r.mu.Lock()
	d, ok := r.built[name]
	prov := r.providers[name]
	r.mu.Unlock()
	if !ok || d != digest || !r.Registry.HasSource(name) {
		return provider.Provider{}, false
	}
	return prov, true
}

func (r *SandboxProviderReconciler) setBuilt(name string, digest [sha256.Size]byte, prov provider.Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.built == nil {
		r.built = map[string][sha256.Size]byte{}
	}
	if r.providers == nil {
		r.providers = map[string]provider.Provider{}
	}
	r.built[name] = digest
	r.providers[name] = prov
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
	writeMap(h, field, cfg.Templates, func(v string) []byte { return []byte(v) })
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

func templateMap(list []v1alpha1.TemplateMapping) map[string]string {
	if len(list) == 0 {
		return nil
	}
	m := make(map[string]string, len(list))
	for _, item := range list {
		m[item.Name] = item.Ref
	}
	return m
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
func (r *SandboxProviderReconciler) registryReadiness(p *v1alpha1.SandboxProvider) (readiness, bool) {
	var found *registry.Status
	for _, st := range r.Registry.Statuses() {
		if st.Provider == p.Name {
			s := st
			found = &s
			break
		}
	}
	if found == nil {
		return readiness{metav1.ConditionFalse, ReasonNoReport, "registered, but no capacity report yet"}, false
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
			fmt.Sprintf("serving last known capacity; most recent fetch failed: %s", found.LastError)}, found.Report.Elastic
	case found.Reachable:
		return readiness{metav1.ConditionTrue, ReasonReporting,
			fmt.Sprintf("%d warm sandboxes available", found.Report.WarmCapacity)}, found.Report.Elastic
	case found.Stale:
		// Since a time rather than for a duration: a duration would change the
		// message, and so cost a status write, on every reconcile for as long
		// as the provider stays down.
		return readiness{metav1.ConditionFalse, ReasonStale,
			fmt.Sprintf("no successful report since %s; last error: %v",
				found.UpdatedAt.UTC().Format(time.RFC3339), found.LastError)}, found.Report.Elastic
	case found.LastError != nil:
		return readiness{metav1.ConditionFalse, ReasonFetchFailed, found.LastError.Error()}, found.Report.Elastic
	default:
		return readiness{metav1.ConditionFalse, ReasonNoReport, "registered, but no capacity report yet"}, found.Report.Elastic
	}
}

// writeStatus records readiness on the object, writing only if its status
// differs from what was observed. Most reconciles are status ticks on a
// provider whose state has not moved; writing those anyway costs an API
// server round trip and an etcd revision each, and wakes every watcher of
// SandboxProviders, for nothing.
func (r *SandboxProviderReconciler) writeStatus(ctx context.Context, p *v1alpha1.SandboxProvider,
	observed *v1alpha1.SandboxProviderStatus, ready readiness, claiming *readiness) (ctrl.Result, error) {

	if r.WriteStatus != nil && !r.WriteStatus() {
		return ctrl.Result{RequeueAfter: r.statusInterval()}, nil
	}

	prevClaiming := findCondition(observed.Conditions, ConditionClaiming)

	setCondition(&p.Status.Conditions, metav1.Condition{
		Type:               ConditionReady,
		Status:             ready.status,
		Reason:             ready.reason,
		Message:            ready.message,
		ObservedGeneration: p.Generation,
		LastTransitionTime: metav1.Now(),
	})
	if claiming != nil {
		setCondition(&p.Status.Conditions, metav1.Condition{
			Type:               ConditionClaiming,
			Status:             claiming.status,
			Reason:             claiming.reason,
			Message:            claiming.message,
			ObservedGeneration: p.Generation,
			LastTransitionTime: metav1.Now(),
		})
	}
	if equality.Semantic.DeepEqual(observed, &p.Status) {
		return ctrl.Result{RequeueAfter: r.statusInterval()}, nil
	}

	base := p.DeepCopy()
	base.Status = *observed
	if err := r.Status().Patch(ctx, p, client.MergeFrom(base)); err != nil {
		if apierrors.IsConflict(err) {
			// Someone else wrote first. Requeue rather than fight: the next
			// pass reads fresh and converges.
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	if claiming != nil && claimingTransitioned(prevClaiming, *claiming, p.Spec.Claims != nil && p.Spec.Claims.Enabled) {
		evType := corev1.EventTypeNormal
		if claiming.status == metav1.ConditionFalse &&
			claiming.reason != ReasonClaimsDisabled &&
			claiming.reason != ReasonDisabled {
			evType = corev1.EventTypeWarning
		}
		r.emitEvent(ctx, p, evType, "ReconcileClaiming", claiming.reason, claiming.message)
	}

	return ctrl.Result{RequeueAfter: r.statusInterval()}, nil
}

// RecordReapDryRunEvent emits a Kubernetes events.k8s.io/v1 Event on the
// SandboxProvider summarizing expired sandboxes observed during a reaper
// dry-run pass.
func (r *SandboxProviderReconciler) RecordReapDryRunEvent(ctx context.Context, providerName string, handles []provider.Handle) {
	if len(handles) == 0 || r.Client == nil {
		return
	}
	if r.WriteStatus != nil && !r.WriteStatus() {
		return
	}
	var sp v1alpha1.SandboxProvider
	if err := r.Get(ctx, types.NamespacedName{Name: providerName}, &sp); err != nil {
		return
	}
	msg := fmt.Sprintf("dry-run: would reap %d expired sandbox(es) (first: %s)", len(handles), handles[0].ID)
	r.emitEvent(ctx, &sp, corev1.EventTypeNormal, "ReapDryRun", ReasonReapDryRun, msg)
}

func (r *SandboxProviderReconciler) emitEvent(
	ctx context.Context,
	p *v1alpha1.SandboxProvider,
	eventType, action, reason, note string,
) {
	if r.Client == nil || p == nil {
		return
	}
	instance := r.EventInstance
	if instance == "" {
		instance = "sandbox-scheduler"
	}
	if len(note) > 1024 {
		note = note[:1024]
	}
	ev := &eventsv1.Event{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: p.Name + "-",
			Namespace:    defaultEventNS,
		},
		EventTime:           metav1.NewMicroTime(time.Now()),
		ReportingController: reportingController,
		ReportingInstance:   instance,
		Action:              action,
		Reason:              reason,
		Regarding: corev1.ObjectReference{
			APIVersion: v1alpha1.GroupVersion.String(),
			Kind:       "SandboxProvider",
			Name:       p.Name,
			UID:        p.UID,
		},
		Type: eventType,
		Note: note,
	}
	if err := r.Create(ctx, ev); err != nil {
		log.FromContext(ctx).V(1).Info("unable to emit event for SandboxProvider",
			"provider", p.Name, "reason", reason, "error", err)
	}
}

func findCondition(conditions []metav1.Condition, condType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == condType {
			c := conditions[i]
			return &c
		}
	}
	return nil
}

func claimingTransitioned(prev *metav1.Condition, next readiness, claimsEnabled bool) bool {
	if prev == nil {
		return claimsEnabled || next.status == metav1.ConditionTrue
	}
	return prev.Status != next.status || prev.Reason != next.reason
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
