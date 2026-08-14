package controller

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
)

// Condition types and reasons on SandboxPlacementPolicy.
const (
	ConditionValid = "Valid"

	ReasonAccepted = "Accepted"
	ReasonRejected = "Rejected"
)

// SandboxPlacementPolicyReconciler validates policies and reports the result.
//
// Validation is the entire job, and it happens here rather than at scheduling
// time for a specific reason: a policy naming a filter this build does not
// implement must be visible as broken on the object, before any workload is
// placed under it. Discovering it at placement time would mean either failing
// live traffic or — far worse — silently scheduling under weaker constraints
// than the operator wrote down.
type SandboxPlacementPolicyReconciler struct {
	client.Client
}

// +kubebuilder:rbac:groups=placement.agents.x-k8s.io,resources=sandboxplacementpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=placement.agents.x-k8s.io,resources=sandboxplacementpolicies/status,verbs=get;update;patch

// Reconcile validates one policy.
func (r *SandboxPlacementPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var policy v1alpha1.SandboxPlacementPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	status := metav1.ConditionTrue
	reason := ReasonAccepted
	message := "policy is valid and will be used for placement"
	if err := policy.Validate(); err != nil {
		status = metav1.ConditionFalse
		reason = ReasonRejected
		message = err.Error()
	}

	setCondition(&policy.Status.Conditions, metav1.Condition{
		Type:               ConditionValid,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: policy.Generation,
		LastTransitionTime: metav1.Now(),
	})

	if err := r.Status().Update(ctx, &policy); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager registers the controller.
func (r *SandboxPlacementPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.SandboxPlacementPolicy{}).
		Complete(r)
}
