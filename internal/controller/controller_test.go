package controller_test

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/internal/controller"
	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter"
	_ "github.com/NolanFoster/sandbox-scheduler/pkg/adapter/agentsandbox"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

const secretNS = "sandbox-scheduler"

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func newClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.SandboxProvider{}, &v1alpha1.SandboxPlacementPolicy{}).
		Build()
}

func provider(name string, mutate func(*v1alpha1.SandboxProvider)) *v1alpha1.SandboxProvider {
	p := &v1alpha1.SandboxProvider{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1alpha1.SandboxProviderSpec{
			Adapter:  "agent-sandbox",
			Endpoint: "https://gateway.example.com",
		},
	}
	if mutate != nil {
		mutate(p)
	}
	return p
}

func reconcileProvider(t *testing.T, c client.Client, reg *registry.Registry, name string) (ctrl.Result, error) {
	t.Helper()
	r := &controller.SandboxProviderReconciler{
		Client:          c,
		Registry:        reg,
		SecretNamespace: secretNS,
		StatusInterval:  time.Second,
	}
	return r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name},
	})
}

func readyCondition(t *testing.T, c client.Client, name string) metav1.Condition {
	t.Helper()
	var p v1alpha1.SandboxProvider
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &p); err != nil {
		t.Fatalf("reading provider: %v", err)
	}
	for _, cond := range p.Status.Conditions {
		if cond.Type == controller.ConditionReady {
			return cond
		}
	}
	t.Fatalf("no Ready condition on %s", name)
	return metav1.Condition{}
}

func registered(reg *registry.Registry, name string) bool {
	for _, s := range reg.Statuses() {
		if s.Provider == name {
			return true
		}
	}
	return false
}

// --- provider reconciler ---------------------------------------------------

func TestProviderIsRegisteredAndReportedNotReadyUntilItReports(t *testing.T) {
	reg := registry.New(registry.Options{})
	c := newClient(t, provider("civo", nil))

	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !registered(reg, "civo") {
		t.Fatal("provider should be registered with the registry")
	}
	cond := readyCondition(t, c, "civo")
	if cond.Status != metav1.ConditionFalse || cond.Reason != controller.ReasonNoReport {
		t.Fatalf("want NoReportYet/False before any poll, got %s/%s", cond.Reason, cond.Status)
	}
}

func TestProviderBecomesReadyOnceCapacityIsKnown(t *testing.T) {
	reg := registry.New(registry.Options{})
	c := newClient(t, provider("civo", nil))
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Simulate the registry's own poll landing.
	reg.Report("civo", registry.Report{
		WarmCapacity: 3,
		Attributes:   map[string]string{"warmPoolName": "python-warm-pool"},
	})
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cond := readyCondition(t, c, "civo")
	if cond.Status != metav1.ConditionTrue || cond.Reason != controller.ReasonReporting {
		t.Fatalf("want Reporting/True, got %s/%s: %s", cond.Reason, cond.Status, cond.Message)
	}
	var p v1alpha1.SandboxProvider
	_ = c.Get(context.Background(), types.NamespacedName{Name: "civo"}, &p)
	if p.Status.WarmCapacity != 3 {
		t.Fatalf("status warmCapacity %d, want 3", p.Status.WarmCapacity)
	}
	if p.Status.ObservedAttributes["warmPoolName"] != "python-warm-pool" {
		t.Fatal("observed attributes should be recorded for humans")
	}
	if p.Status.LastReportTime == nil {
		t.Fatal("lastReportTime should be set once a report exists")
	}
}

func TestDisabledProviderIsDeregisteredButKeptConfigured(t *testing.T) {
	// Draining for maintenance must not lose configuration, and must be one
	// field flip to undo.
	reg := registry.New(registry.Options{})
	c := newClient(t, provider("civo", nil))
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !registered(reg, "civo") {
		t.Fatal("expected registration before disabling")
	}

	var p v1alpha1.SandboxProvider
	_ = c.Get(context.Background(), types.NamespacedName{Name: "civo"}, &p)
	p.Spec.Disabled = true
	if err := c.Update(context.Background(), &p); err != nil {
		t.Fatalf("updating: %v", err)
	}
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if registered(reg, "civo") {
		t.Fatal("a disabled provider must not remain a placement candidate")
	}
	if cond := readyCondition(t, c, "civo"); cond.Reason != controller.ReasonDisabled {
		t.Fatalf("want Disabled, got %s", cond.Reason)
	}
	// The object, and therefore its configuration, still exists.
	if err := c.Get(context.Background(), types.NamespacedName{Name: "civo"}, &p); err != nil {
		t.Fatalf("disabled provider should still exist: %v", err)
	}
}

func TestDeletedProviderIsRemovedFromTheRegistry(t *testing.T) {
	reg := registry.New(registry.Options{})
	c := newClient(t, provider("civo", nil))
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var p v1alpha1.SandboxProvider
	_ = c.Get(context.Background(), types.NamespacedName{Name: "civo"}, &p)
	if err := c.Delete(context.Background(), &p); err != nil {
		t.Fatalf("deleting: %v", err)
	}
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("reconciling a deleted provider should not error: %v", err)
	}
	if registered(reg, "civo") {
		t.Fatal("a deleted provider must leave the registry")
	}
}

func TestUnknownAdapterIsReportedNotSilentlyIgnored(t *testing.T) {
	// Usually a typo, or a scheduler older than its config. Either way the
	// provider would otherwise just never be polled, with nothing saying why.
	reg := registry.New(registry.Options{})
	c := newClient(t, provider("civo", func(p *v1alpha1.SandboxProvider) {
		p.Spec.Adapter = "not-a-real-adapter"
	}))
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cond := readyCondition(t, c, "civo")
	if cond.Reason != controller.ReasonUnknownAdapter {
		t.Fatalf("want UnknownAdapter, got %s", cond.Reason)
	}
	if !strings.Contains(cond.Message, "agent-sandbox") {
		t.Fatalf("message should list available adapters, got %q", cond.Message)
	}
	if registered(reg, "civo") {
		t.Fatal("a provider with an unusable adapter must not be a candidate")
	}
}

func TestCredentialsAreResolvedFromTheSchedulerNamespace(t *testing.T) {
	reg := registry.New(registry.Options{})
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "civo-token", Namespace: secretNS},
		Data:       map[string][]byte{"token": []byte("s3cret")},
	}
	c := newClient(t, provider("civo", func(p *v1alpha1.SandboxProvider) {
		p.Spec.CredentialsRef = &corev1.LocalObjectReference{Name: "civo-token"}
	}), secret)

	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !registered(reg, "civo") {
		t.Fatal("provider with resolvable credentials should be registered")
	}
}

func TestMissingSecretKeepsThePreviousRegistration(t *testing.T) {
	// A Secret that is briefly unreadable must not evacuate a cluster.
	reg := registry.New(registry.Options{})
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "civo-token", Namespace: secretNS},
		Data:       map[string][]byte{"token": []byte("s3cret")},
	}
	c := newClient(t, provider("civo", func(p *v1alpha1.SandboxProvider) {
		p.Spec.CredentialsRef = &corev1.LocalObjectReference{Name: "civo-token"}
	}), secret)
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	reg.Report("civo", registry.Report{WarmCapacity: 5})

	if err := c.Delete(context.Background(), secret); err != nil {
		t.Fatalf("deleting secret: %v", err)
	}
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cond := readyCondition(t, c, "civo")
	if cond.Reason != controller.ReasonCredentials {
		t.Fatalf("want CredentialsUnavailable, got %s", cond.Reason)
	}
	if !registered(reg, "civo") {
		t.Fatal("an unreadable Secret must not deregister a working provider")
	}
}

func TestAdapterConfigErrorDeregistersAndExplains(t *testing.T) {
	reg := registry.New(registry.Options{})
	c := newClient(t, provider("civo", func(p *v1alpha1.SandboxProvider) {
		p.Spec.Endpoint = "" // the agent-sandbox adapter requires one
	}))
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cond := readyCondition(t, c, "civo")
	if cond.Reason != controller.ReasonAdapterConfig {
		t.Fatalf("want AdapterConfigInvalid, got %s: %s", cond.Reason, cond.Message)
	}
	if !strings.Contains(cond.Message, "endpoint") {
		t.Fatalf("message should name the missing field, got %q", cond.Message)
	}
}

func TestStaleProviderIsNotReadyButKeepsItsCapacity(t *testing.T) {
	reg := registry.New(registry.Options{StaleAfter: time.Nanosecond})
	c := newClient(t, provider("civo", nil))
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	reg.Report("civo", registry.Report{WarmCapacity: 4})
	time.Sleep(2 * time.Millisecond)

	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cond := readyCondition(t, c, "civo")
	if cond.Status != metav1.ConditionFalse || cond.Reason != controller.ReasonStale {
		t.Fatalf("want ReportStale/False, got %s/%s", cond.Reason, cond.Status)
	}
	var p v1alpha1.SandboxProvider
	_ = c.Get(context.Background(), types.NamespacedName{Name: "civo"}, &p)
	if p.Status.WarmCapacity != 4 {
		t.Fatal("stale status should retain the last known capacity")
	}
}

func TestConditionTimestampSurvivesAMessageOnlyChange(t *testing.T) {
	// Otherwise every reconcile rewrites it and "how long has this been
	// broken?" always answers "a few seconds".
	reg := registry.New(registry.Options{})
	c := newClient(t, provider("civo", nil))
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	reg.Report("civo", registry.Report{WarmCapacity: 1})
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	first := readyCondition(t, c, "civo").LastTransitionTime

	time.Sleep(10 * time.Millisecond)
	reg.Report("civo", registry.Report{WarmCapacity: 2}) // still Ready, new message
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second := readyCondition(t, c, "civo")

	if !second.LastTransitionTime.Equal(&first) {
		t.Fatal("LastTransitionTime changed without the status changing")
	}
	if !strings.Contains(second.Message, "2 warm") {
		t.Fatalf("message should have updated, got %q", second.Message)
	}
}

func TestReconcileRequeuesToRefreshStatus(t *testing.T) {
	reg := registry.New(registry.Options{})
	c := newClient(t, provider("civo", nil))
	res, err := reconcileProvider(t, c, reg, "civo")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatal("status would never refresh without a requeue")
	}
}

func TestAdapterOptionsComeFromAnnotations(t *testing.T) {
	reg := registry.New(registry.Options{})
	c := newClient(t, provider("civo", func(p *v1alpha1.SandboxProvider) {
		p.Annotations = map[string]string{
			"placement.agents.x-k8s.io/option-warmPoolName": "gpu-pool",
			"unrelated.example.com/thing":                   "ignored",
		}
	}))
	if _, err := reconcileProvider(t, c, reg, "civo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !registered(reg, "civo") {
		t.Fatal("provider should register with its options applied")
	}
	if !adapter.Has("agent-sandbox") {
		t.Fatal("agent-sandbox adapter should be linked in")
	}
}

// --- policy reconciler -----------------------------------------------------

func reconcilePolicy(t *testing.T, c client.Client, name string) error {
	t.Helper()
	r := &controller.SandboxPlacementPolicyReconciler{Client: c}
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name},
	})
	return err
}

func validCondition(t *testing.T, c client.Client, name string) metav1.Condition {
	t.Helper()
	var p v1alpha1.SandboxPlacementPolicy
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &p); err != nil {
		t.Fatalf("reading policy: %v", err)
	}
	for _, cond := range p.Status.Conditions {
		if cond.Type == controller.ConditionValid {
			return cond
		}
	}
	t.Fatalf("no Valid condition on %s", name)
	return metav1.Condition{}
}

func TestValidPolicyIsAccepted(t *testing.T) {
	policy := v1alpha1.DefaultPolicy()
	c := newClient(t, policy)
	if err := reconcilePolicy(t, c, policy.Name); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cond := validCondition(t, c, policy.Name); cond.Status != metav1.ConditionTrue {
		t.Fatalf("want Valid=True, got %s: %s", cond.Status, cond.Message)
	}
}

func TestInvalidPolicyIsRejectedOnTheObject(t *testing.T) {
	// Surfacing this at scheduling time would mean either failing live traffic
	// or scheduling under weaker constraints than the operator wrote.
	policy := &v1alpha1.SandboxPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "broken"},
		Spec: v1alpha1.SandboxPlacementPolicySpec{
			Requires: map[string]string{"gpu": "true"}, // no RequiredAttributes filter
		},
	}
	c := newClient(t, policy)
	if err := reconcilePolicy(t, c, "broken"); err != nil {
		t.Fatalf("reconcile should not error on an invalid policy: %v", err)
	}
	cond := validCondition(t, c, "broken")
	if cond.Status != metav1.ConditionFalse || cond.Reason != controller.ReasonRejected {
		t.Fatalf("want Rejected/False, got %s/%s", cond.Reason, cond.Status)
	}
	if !strings.Contains(cond.Message, "would not be enforced") {
		t.Fatalf("message should explain the consequence, got %q", cond.Message)
	}
}

func TestDeletedPolicyReconcilesCleanly(t *testing.T) {
	c := newClient(t)
	if err := reconcilePolicy(t, c, "gone"); err != nil {
		t.Fatalf("reconciling a missing policy should not error: %v", err)
	}
}
