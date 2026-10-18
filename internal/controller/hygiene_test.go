package controller_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/internal/controller"
	"github.com/NolanFoster/sandbox-scheduler/pkg/adapter"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

// countingAdapter records how often sources are built and fetched.
const countingAdapter = "counting"

var counting = struct {
	mu      sync.Mutex
	built   map[string]int
	fetches map[string]*atomic.Int64
}{built: map[string]int{}, fetches: map[string]*atomic.Int64{}}

func init() {
	adapter.Register(countingAdapter, func(cfg adapter.Config) (registry.Source, error) {
		counting.mu.Lock()
		defer counting.mu.Unlock()
		counting.built[cfg.ProviderID]++
		n, ok := counting.fetches[cfg.ProviderID]
		if !ok {
			n = &atomic.Int64{}
			counting.fetches[cfg.ProviderID] = n
		}
		return &countingSource{id: cfg.ProviderID, fetches: n}, nil
	})
}

type countingSource struct {
	id      string
	fetches *atomic.Int64
}

func (s *countingSource) ProviderID() string { return s.id }

func (s *countingSource) Fetch(context.Context) (registry.Report, error) {
	s.fetches.Add(1)
	return registry.Report{WarmCapacity: 1}, nil
}

func builds(name string) int {
	counting.mu.Lock()
	defer counting.mu.Unlock()
	return counting.built[name]
}

func fetches(name string) int64 {
	counting.mu.Lock()
	defer counting.mu.Unlock()
	if n, ok := counting.fetches[name]; ok {
		return n.Load()
	}
	return 0
}

// countingProvider is a provider on the counting adapter, with its counts
// reset, so a rerun under -count starts from zero.
func countingProvider(name string, mutate func(*v1alpha1.SandboxProvider)) *v1alpha1.SandboxProvider {
	counting.mu.Lock()
	delete(counting.built, name)
	delete(counting.fetches, name)
	counting.mu.Unlock()
	return provider(name, func(p *v1alpha1.SandboxProvider) {
		p.Spec.Adapter = countingAdapter
		if mutate != nil {
			mutate(p)
		}
	})
}

func newReconciler(c client.Client, reg *registry.Registry) *controller.SandboxProviderReconciler {
	return &controller.SandboxProviderReconciler{
		Client:          c,
		Registry:        reg,
		SecretNamespace: secretNS,
		StatusInterval:  time.Second,
	}
}

func reconcileWith(t *testing.T, r *controller.SandboxProviderReconciler, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func update(t *testing.T, c client.Client, name string, mutate func(*v1alpha1.SandboxProvider)) {
	t.Helper()
	var p v1alpha1.SandboxProvider
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &p); err != nil {
		t.Fatal(err)
	}
	mutate(&p)
	if err := c.Update(context.Background(), &p); err != nil {
		t.Fatal(err)
	}
}

func resourceVersion(t *testing.T, c client.Client, name string) string {
	t.Helper()
	var p v1alpha1.SandboxProvider
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &p); err != nil {
		t.Fatal(err)
	}
	return p.ResourceVersion
}

func TestStatusTicksDoNotRebuildTheSource(t *testing.T) {
	// A status tick changes nothing about how to reach the provider, so the
	// source it already has must be kept, along with its polling schedule.
	reg := registry.New(registry.Options{})
	c := newClient(t, countingProvider("tick", nil))
	r := newReconciler(c, reg)
	for i := 0; i < 5; i++ {
		reconcileWith(t, r, "tick")
	}
	if n := builds("tick"); n != 1 {
		t.Fatalf("source built %d times over 5 reconciles, want 1", n)
	}
}

func TestAnyChangeToHowItIsReachedRebuildsTheSource(t *testing.T) {
	reg := registry.New(registry.Options{})
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "rebuild-token", Namespace: secretNS},
		Data:       map[string][]byte{"token": []byte("one")},
	}
	c := newClient(t, countingProvider("rebuild", func(p *v1alpha1.SandboxProvider) {
		p.Spec.CredentialsRef = &corev1.LocalObjectReference{Name: "rebuild-token"}
	}), secret)
	r := newReconciler(c, reg)
	reconcileWith(t, r, "rebuild")

	update(t, c, "rebuild", func(p *v1alpha1.SandboxProvider) { p.Spec.Endpoint = "https://other.example.com" })
	reconcileWith(t, r, "rebuild")
	if n := builds("rebuild"); n != 2 {
		t.Fatalf("built %d times; a new endpoint should rebuild", n)
	}

	update(t, c, "rebuild", func(p *v1alpha1.SandboxProvider) {
		p.Annotations = map[string]string{"placement.agents.x-k8s.io/option-warmPoolName": "gpu"}
	})
	reconcileWith(t, r, "rebuild")
	if n := builds("rebuild"); n != 3 {
		t.Fatalf("built %d times; a new adapter option should rebuild", n)
	}

	secret.Data["token"] = []byte("two")
	if err := c.Update(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	reconcileWith(t, r, "rebuild")
	if n := builds("rebuild"); n != 4 {
		t.Fatalf("built %d times; a rotated credential should rebuild", n)
	}

	update(t, c, "rebuild", func(p *v1alpha1.SandboxProvider) {
		p.Spec.Attributes = map[string]string{"region": "nyc1"}
	})
	reconcileWith(t, r, "rebuild")
	if n := builds("rebuild"); n != 4 {
		t.Fatalf("built %d times; attributes are not part of how it is reached", n)
	}
}

func TestADeregisteredProviderIsRebuiltWhenItReturns(t *testing.T) {
	reg := registry.New(registry.Options{})
	c := newClient(t, countingProvider("back", nil))
	r := newReconciler(c, reg)
	reconcileWith(t, r, "back")

	update(t, c, "back", func(p *v1alpha1.SandboxProvider) { p.Spec.Disabled = true })
	reconcileWith(t, r, "back")
	update(t, c, "back", func(p *v1alpha1.SandboxProvider) { p.Spec.Disabled = false })
	reconcileWith(t, r, "back")

	if !reg.HasSource("back") {
		t.Fatal("re-enabled provider should be polled again")
	}
	if n := builds("back"); n != 2 {
		t.Fatalf("built %d times, want a fresh source on re-enable", n)
	}
}

func TestUnchangedStatusIsNotWritten(t *testing.T) {
	// Every write is an etcd revision and an event to every watcher. A
	// provider whose state has not moved should cost neither.
	reg := registry.New(registry.Options{})
	c := newClient(t, countingProvider("quiet", nil))
	r := newReconciler(c, reg)
	reconcileWith(t, r, "quiet")
	reg.Report("quiet", registry.Report{WarmCapacity: 3})
	reconcileWith(t, r, "quiet")
	if readyCondition(t, c, "quiet").Status != metav1.ConditionTrue {
		t.Fatal("should be Ready once reported")
	}

	before := resourceVersion(t, c, "quiet")
	for i := 0; i < 3; i++ {
		reconcileWith(t, r, "quiet")
	}
	if after := resourceVersion(t, c, "quiet"); after != before {
		t.Fatalf("resourceVersion %s -> %s; status was rewritten unchanged", before, after)
	}

	reg.Report("quiet", registry.Report{WarmCapacity: 4})
	reconcileWith(t, r, "quiet")
	if after := resourceVersion(t, c, "quiet"); after == before {
		t.Fatal("a real change in capacity should still be written")
	}
}

func TestAStaleProviderIsNotRewrittenEveryTick(t *testing.T) {
	// The stale message names when the last report arrived, not how long
	// ago, so it does not change, and cost a write, on every reconcile.
	reg := registry.New(registry.Options{StaleAfter: time.Nanosecond})
	c := newClient(t, countingProvider("down", nil))
	r := newReconciler(c, reg)
	reconcileWith(t, r, "down")
	reg.Report("down", registry.Report{WarmCapacity: 1})
	time.Sleep(2 * time.Millisecond)
	reconcileWith(t, r, "down")
	if readyCondition(t, c, "down").Reason != controller.ReasonStale {
		t.Fatal("should be stale")
	}

	before := resourceVersion(t, c, "down")
	time.Sleep(1100 * time.Millisecond) // past a second boundary
	reconcileWith(t, r, "down")
	if after := resourceVersion(t, c, "down"); after != before {
		t.Fatalf("stale status rewritten with nothing new: %s -> %s", before, after)
	}
}

func TestSpecRefreshIntervalDrivesPolling(t *testing.T) {
	reg := registry.New(registry.Options{})
	c := newClient(t, countingProvider("eager", func(p *v1alpha1.SandboxProvider) {
		p.Spec.RefreshInterval = &metav1.Duration{Duration: 10 * time.Millisecond}
	}))
	reconcileWith(t, newReconciler(c, reg), "eager")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { reg.Run(ctx, time.Hour); close(done) }()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(2 * time.Second)
	for fetches("eager") < 10 {
		if time.Now().After(deadline) {
			t.Fatalf("fetched %d times; spec.refreshInterval of 10ms should override the 1h default",
				fetches("eager"))
		}
		time.Sleep(5 * time.Millisecond)
	}
}
