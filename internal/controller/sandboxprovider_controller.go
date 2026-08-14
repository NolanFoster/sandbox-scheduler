// Package controller reconciles the placement API onto the running scheduler.
package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

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
			r.Registry.RemoveSource(req.Name)
			logger.Info("provider deleted, removed from registry", "provider", req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if provider.Spec.Disabled {
		// Drained, not deleted. Configuration is preserved and one field flip
		// brings it back.
		r.Registry.RemoveSource(provider.Name)
		return r.setStatus(ctx, &provider, metav1.ConditionFalse, ReasonDisabled,
			"provider is disabled and will not be considered for placement")
	}

	if !adapter.Has(provider.Spec.Adapter) {
		// Fail visibly rather than never polling it. An unknown adapter usually
		// means a typo or a scheduler older than the config.
		r.Registry.RemoveSource(provider.Name)
		return r.setStatus(ctx, &provider, metav1.ConditionFalse, ReasonUnknownAdapter,
			fmt.Sprintf("unknown adapter %q (available: %v)", provider.Spec.Adapter, adapter.Names()))
	}

	creds, err := r.resolveCredentials(ctx, &provider)
	if err != nil {
		// Keep polling with what we had rather than dropping the provider: a
		// Secret that is briefly unreadable should not evacuate a cluster.
		return r.setStatus(ctx, &provider, metav1.ConditionFalse, ReasonCredentials, err.Error())
	}

	source, err := adapter.New(provider.Spec.Adapter, adapter.Config{
		ProviderID:  provider.Name,
		Endpoint:    provider.Spec.Endpoint,
		Credentials: creds,
		Options:     providerOptions(&provider),
	})
	if err != nil {
		r.Registry.RemoveSource(provider.Name)
		return r.setStatus(ctx, &provider, metav1.ConditionFalse, ReasonAdapterConfig, err.Error())
	}

	// Idempotent: re-registering keeps any capacity already known, so a
	// reconcile triggered by an unrelated field edit does not blind the
	// scheduler to this provider until the next poll.
	r.Registry.AddSource(source)

	// The trusted half. Attributes and cost come from the spec — written by
	// someone with permission to write it — and never from what the provider
	// reports about itself. See registry.ProviderConfig.
	cfg := registry.ProviderConfig{Attributes: provider.Spec.Attributes}
	if provider.Spec.CostPerHour != nil {
		cfg.CostPerHour = provider.Spec.CostPerHour.AsApproximateFloat64()
	}
	r.Registry.SetConfig(provider.Name, cfg)

	return r.reportRegistryStatus(ctx, &provider)
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

// reportRegistryStatus copies what the registry knows onto the object.
func (r *SandboxProviderReconciler) reportRegistryStatus(ctx context.Context, p *v1alpha1.SandboxProvider) (ctrl.Result, error) {
	var found *registry.Status
	for _, st := range r.Registry.Statuses() {
		if st.Provider == p.Name {
			s := st
			found = &s
			break
		}
	}
	if found == nil {
		return r.setStatus(ctx, p, metav1.ConditionFalse, ReasonNoReport,
			"registered, but no capacity report yet")
	}

	p.Status.WarmCapacity = int32(found.Report.WarmCapacity)
	p.Status.ObservedAttributes = found.Report.Attributes
	if found.LastError != nil {
		p.Status.LastError = found.LastError.Error()
	} else {
		p.Status.LastError = ""
	}
	if found.Reachable || found.Stale {
		t := metav1.NewTime(time.Now().Add(-found.Age))
		p.Status.LastReportTime = &t
	}

	switch {
	case found.Reachable && found.LastError != nil:
		// Reporting, but the most recent attempt failed. Still Ready — the data
		// is inside its freshness window — but the error is surfaced so a
		// flapping provider is visible rather than looking healthy.
		return r.setStatus(ctx, p, metav1.ConditionTrue, ReasonReporting,
			fmt.Sprintf("serving last known capacity; most recent fetch failed: %s", found.LastError))
	case found.Reachable:
		return r.setStatus(ctx, p, metav1.ConditionTrue, ReasonReporting,
			fmt.Sprintf("%d warm sandboxes available", found.Report.WarmCapacity))
	case found.Stale:
		return r.setStatus(ctx, p, metav1.ConditionFalse, ReasonStale,
			fmt.Sprintf("no successful report for %s; last error: %v", found.Age.Truncate(time.Second), found.LastError))
	case found.LastError != nil:
		return r.setStatus(ctx, p, metav1.ConditionFalse, ReasonFetchFailed, found.LastError.Error())
	default:
		return r.setStatus(ctx, p, metav1.ConditionFalse, ReasonNoReport,
			"registered, but no capacity report yet")
	}
}

func (r *SandboxProviderReconciler) setStatus(ctx context.Context, p *v1alpha1.SandboxProvider,
	status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {

	meta := metav1.Condition{
		Type:               ConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: p.Generation,
		LastTransitionTime: metav1.Now(),
	}
	setCondition(&p.Status.Conditions, meta)

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
func (r *SandboxProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.SandboxProvider{}).
		Complete(r)
}
