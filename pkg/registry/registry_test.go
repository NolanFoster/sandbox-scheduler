package registry_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

// --- test doubles ----------------------------------------------------------

type fakeSource struct {
	id    string
	mu    sync.Mutex
	rep   registry.Report
	err   error
	delay time.Duration
	calls int
}

func (f *fakeSource) ProviderID() string { return f.id }

func (f *fakeSource) Fetch(ctx context.Context) (registry.Report, error) {
	f.mu.Lock()
	delay, rep, err := f.delay, f.rep, f.err
	f.calls++
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return registry.Report{}, ctx.Err()
		}
	}
	return rep, err
}

func (f *fakeSource) set(rep registry.Report, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rep, f.err = rep, err
}

func (f *fakeSource) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// clock is a manually advanced clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newClock() *clock { return &clock{t: time.Unix(1_700_000_000, 0)} }

// --- tests -----------------------------------------------------------------

func TestSnapshotIsEmptyBeforeAnythingIsRegistered(t *testing.T) {
	r := registry.New(registry.Options{})
	if got := r.Snapshot(); len(got) != 0 {
		t.Fatalf("want empty snapshot, got %d candidates", len(got))
	}
}

func TestRegisteredButNeverPolledProviderIsVisibleAndUnreachable(t *testing.T) {
	// A misconfigured provider must show up as a candidate that policy rejects,
	// not vanish. "Why wasn't civo considered?" needs an answer.
	r := registry.New(registry.Options{})
	r.AddSource(&fakeSource{id: "civo"})

	snap := r.Snapshot()
	if len(snap) != 1 || snap[0].Provider != "civo" {
		t.Fatalf("want civo present, got %+v", snap)
	}
	if snap[0].Reachable {
		t.Fatal("a provider we have never heard from must not be reachable")
	}
	if snap[0].WarmCapacity != 0 {
		t.Fatal("unknown capacity must be zero, not invented")
	}
}

func TestRefreshPopulatesCapacity(t *testing.T) {
	clk := newClock()
	r := registry.New(registry.Options{Now: clk.now})
	src := &fakeSource{id: "civo"}
	src.set(registry.Report{
		WarmCapacity: 3,
		Attributes:   map[string]string{"runtime": "gvisor"},
	}, nil)
	r.AddSource(src)
	// Attributes and cost that policy matches on are declared, not reported.
	r.SetConfig("civo", registry.ProviderConfig{
		Attributes:  map[string]string{"runtime": "gvisor"},
		CostPerHour: 1.5,
	})

	r.Refresh(context.Background())

	snap := r.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("want 1 candidate, got %d", len(snap))
	}
	c := snap[0]
	if !c.Reachable || c.WarmCapacity != 3 || c.CostPerHour != 1.5 {
		t.Fatalf("unexpected candidate: %+v", c)
	}
	if c.Attr("runtime") != "gvisor" {
		t.Fatalf("attributes not carried through: %+v", c.Attributes)
	}
}

func TestFailedRefreshKeepsTheLastGoodReport(t *testing.T) {
	// Rule 1. A blip that zeroed capacity would silently redirect an entire
	// fleet away from a healthy provider.
	clk := newClock()
	r := registry.New(registry.Options{Now: clk.now})
	src := &fakeSource{id: "civo"}
	src.set(registry.Report{WarmCapacity: 5}, nil)
	r.AddSource(src)
	r.Refresh(context.Background())

	src.set(registry.Report{}, errors.New("connection refused"))
	r.Refresh(context.Background())

	snap := r.Snapshot()
	if snap[0].WarmCapacity != 5 {
		t.Fatalf("capacity %d; a failed fetch must not erase the last good report",
			snap[0].WarmCapacity)
	}
	if !snap[0].Reachable {
		t.Fatal("a single failure within the staleness window should not mark it unreachable")
	}
}

func TestReportsGoStaleAndProviderStaysInTheSnapshot(t *testing.T) {
	// Rule 2. Staleness demotes; only RemoveSource deletes.
	clk := newClock()
	r := registry.New(registry.Options{StaleAfter: 30 * time.Second, Now: clk.now})
	src := &fakeSource{id: "civo"}
	src.set(registry.Report{WarmCapacity: 4}, nil)
	r.AddSource(src)
	r.Refresh(context.Background())

	if !r.Snapshot()[0].Reachable {
		t.Fatal("should be reachable immediately after a successful refresh")
	}

	clk.advance(31 * time.Second)

	snap := r.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("a stale provider must stay in the snapshot, got %d candidates", len(snap))
	}
	if snap[0].Reachable {
		t.Fatal("a stale provider must be marked unreachable")
	}
	if snap[0].WarmCapacity != 4 {
		t.Fatal("stale capacity is still the best information available; keep it")
	}
}

func TestStalenessBoundaryIsInclusive(t *testing.T) {
	clk := newClock()
	r := registry.New(registry.Options{StaleAfter: 30 * time.Second, Now: clk.now})
	src := &fakeSource{id: "civo"}
	r.AddSource(src)
	r.Refresh(context.Background())

	clk.advance(30 * time.Second)
	if !r.Snapshot()[0].Reachable {
		t.Fatal("exactly at the staleness boundary should still count as fresh")
	}
	clk.advance(time.Nanosecond)
	if r.Snapshot()[0].Reachable {
		t.Fatal("one tick past the boundary should be stale")
	}
}

func TestOneSlowProviderDoesNotDelayTheOthers(t *testing.T) {
	// Sources are polled in parallel; a hung provider must not hold the whole
	// refresh, or one bad cluster degrades freshness everywhere.
	r := registry.New(registry.Options{FetchTimeout: 50 * time.Millisecond})
	slow := &fakeSource{id: "slow", delay: time.Second}
	fast := &fakeSource{id: "fast"}
	fast.set(registry.Report{WarmCapacity: 2}, nil)
	r.AddSource(slow)
	r.AddSource(fast)

	start := time.Now()
	r.Refresh(context.Background())
	elapsed := time.Since(start)

	// Bounded by the fetch timeout, not by the slow provider's delay.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("refresh took %v; a slow provider should be bounded by FetchTimeout", elapsed)
	}
	byID := map[string]int{}
	for _, c := range r.Snapshot() {
		byID[c.Provider] = c.WarmCapacity
	}
	if byID["fast"] != 2 {
		t.Fatal("the healthy provider's report should have landed regardless")
	}
}

func TestFetchTimeoutIsEnforced(t *testing.T) {
	r := registry.New(registry.Options{FetchTimeout: 20 * time.Millisecond})
	src := &fakeSource{id: "civo", delay: time.Second}
	r.AddSource(src)

	done := make(chan struct{})
	go func() { r.Refresh(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Refresh did not respect FetchTimeout")
	}
	if r.Snapshot()[0].Reachable {
		t.Fatal("a timed-out provider must not be marked reachable")
	}
}

func TestPushReportBypassesPolling(t *testing.T) {
	clk := newClock()
	r := registry.New(registry.Options{Now: clk.now})
	r.Report("hosted", registry.Report{WarmCapacity: 7})

	snap := r.Snapshot()
	if len(snap) != 1 || snap[0].Provider != "hosted" {
		t.Fatalf("push-reported provider missing: %+v", snap)
	}
	if !snap[0].Reachable || snap[0].WarmCapacity != 7 {
		t.Fatalf("unexpected candidate: %+v", snap[0])
	}
}

func TestSelfReportedAttributesNeverReachPolicy(t *testing.T) {
	// The property that keeps a provider from asserting its own isolation. A
	// provider claiming runtime=gvisor about itself must not satisfy a filter
	// that exists to keep untrusted workloads off weak providers.
	r := registry.New(registry.Options{})
	r.Report("civo", registry.Report{
		WarmCapacity: 1,
		Attributes:   map[string]string{"runtime": "gvisor", "gpu": "true"},
	})
	r.SetConfig("civo", registry.ProviderConfig{
		Attributes: map[string]string{"runtime": "runc"},
	})

	c := r.Snapshot()[0]
	if c.Attr("runtime") != "runc" {
		t.Fatalf("candidate runtime %q; the declared value must win", c.Attr("runtime"))
	}
	if c.Attr("gpu") != "" {
		t.Fatal("a self-reported attribute must not appear on the candidate at all")
	}
	// ...but it is still visible to humans.
	if r.Statuses()[0].Report.Attributes["gpu"] != "true" {
		t.Fatal("self-reported attributes should still surface in status")
	}
}

func TestDeclaredCostIsNotSelfReportable(t *testing.T) {
	// A provider that could report its own price could report zero and win
	// every placement.
	r := registry.New(registry.Options{})
	r.Report("civo", registry.Report{WarmCapacity: 1})
	r.SetConfig("civo", registry.ProviderConfig{CostPerHour: 4})
	if got := r.Snapshot()[0].CostPerHour; got != 4 {
		t.Fatalf("cost %v, want the declared 4", got)
	}
}

func TestRemoveSourceIsTheOnlyWayOut(t *testing.T) {
	r := registry.New(registry.Options{})
	src := &fakeSource{id: "civo"}
	r.AddSource(src)
	r.Refresh(context.Background())
	if len(r.Snapshot()) != 1 {
		t.Fatal("expected civo present")
	}

	r.RemoveSource("civo")
	if len(r.Snapshot()) != 0 {
		t.Fatal("RemoveSource should drop the provider entirely")
	}
}

func TestReAddingASourceKeepsKnownCapacity(t *testing.T) {
	// A CRD update re-registers the source; the scheduler should not go blind
	// to a provider it already knows about.
	clk := newClock()
	r := registry.New(registry.Options{Now: clk.now})
	src := &fakeSource{id: "civo"}
	src.set(registry.Report{WarmCapacity: 6}, nil)
	r.AddSource(src)
	r.Refresh(context.Background())

	r.AddSource(&fakeSource{id: "civo"}) // replacement source, same provider
	snap := r.Snapshot()
	if snap[0].WarmCapacity != 6 || !snap[0].Reachable {
		t.Fatalf("re-registering lost known capacity: %+v", snap[0])
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	// A scorer mutating its candidate must not corrupt registry state for the
	// next decision.
	r := registry.New(registry.Options{})
	r.Report("civo", registry.Report{WarmCapacity: 3})
	r.SetConfig("civo", registry.ProviderConfig{
		Attributes: map[string]string{"region": "nyc1"},
	})

	first := r.Snapshot()
	first[0].WarmCapacity = 999
	first[0].Attributes["region"] = "tampered"

	second := r.Snapshot()
	if second[0].WarmCapacity != 3 {
		t.Fatal("mutating a snapshot changed registry state")
	}
	if second[0].Attr("region") != "nyc1" {
		t.Fatal("attribute map is shared with the registry; it must be copied")
	}
}

func TestSnapshotOrderingIsStable(t *testing.T) {
	// Reproducible scheduling and readable log diffs both depend on this.
	r := registry.New(registry.Options{})
	for _, id := range []string{"zeta", "alpha", "mid"} {
		r.Report(id, registry.Report{})
	}
	for i := 0; i < 10; i++ {
		snap := r.Snapshot()
		if snap[0].Provider != "alpha" || snap[1].Provider != "mid" || snap[2].Provider != "zeta" {
			t.Fatalf("unstable ordering on iteration %d: %v", i, snap)
		}
	}
}

func TestRunRefreshesImmediatelyThenOnInterval(t *testing.T) {
	// Without an immediate first pass the scheduler is blind for a whole
	// interval after startup — exactly when a rollout is placing sandboxes.
	r := registry.New(registry.Options{})
	src := &fakeSource{id: "civo"}
	src.set(registry.Report{WarmCapacity: 1}, nil)
	r.AddSource(src)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx, 20*time.Millisecond)

	deadline := time.After(2 * time.Second)
	for {
		if src.callCount() >= 2 {
			return // refreshed at least twice: immediate + at least one tick
		}
		select {
		case <-deadline:
			t.Fatalf("Run refreshed only %d times", src.callCount())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	r := registry.New(registry.Options{})
	src := &fakeSource{id: "civo"}
	r.AddSource(src)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx, 10*time.Millisecond); close(done) }()

	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestStatusesSurfaceStalenessAndLastError(t *testing.T) {
	// This is what operators and the CRD status will read; a provider that is
	// failing needs to say why, not just look quiet.
	clk := newClock()
	r := registry.New(registry.Options{StaleAfter: 10 * time.Second, Now: clk.now})
	src := &fakeSource{id: "civo"}
	src.set(registry.Report{WarmCapacity: 2}, nil)
	r.AddSource(src)
	r.Refresh(context.Background())

	src.set(registry.Report{}, errors.New("dial tcp: i/o timeout"))
	r.Refresh(context.Background())
	clk.advance(11 * time.Second)

	st := r.Statuses()
	if len(st) != 1 {
		t.Fatalf("want 1 status, got %d", len(st))
	}
	if !st[0].Stale || st[0].Reachable {
		t.Fatalf("expected stale and unreachable, got %+v", st[0])
	}
	if st[0].LastError == nil {
		t.Fatal("the last fetch error must be retained for diagnosis")
	}
	if st[0].Report.WarmCapacity != 2 {
		t.Fatal("status should still carry the last known capacity")
	}
}

func TestConcurrentSnapshotAndRefreshAreSafe(t *testing.T) {
	// Run with -race. Snapshot is called on every scheduling decision while
	// refresh runs in the background; this is the contended path.
	r := registry.New(registry.Options{})
	for _, id := range []string{"a", "b", "c"} {
		s := &fakeSource{id: id}
		s.set(registry.Report{WarmCapacity: 1, Attributes: map[string]string{"k": "v"}}, nil)
		r.AddSource(s)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = r.Snapshot()
					_ = r.Statuses()
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		r.Refresh(context.Background())
		r.Report("push", registry.Report{WarmCapacity: i})
	}
	close(stop)
	wg.Wait()
}
