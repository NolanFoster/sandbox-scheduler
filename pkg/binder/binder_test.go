package binder_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/pkg/binder"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider/adaptertest"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

type scriptBinder struct {
	claimFn      func(ctx context.Context, req provider.ClaimRequest) (provider.Handle, error)
	lookupFn     func(ctx context.Context, req provider.ClaimRequest) (provider.Handle, error)
	releaseFn    func(ctx context.Context, h provider.Handle) error
	lookupManyFn func(ctx context.Context, reqs []provider.ClaimRequest) []provider.LookupResult
}

func (s *scriptBinder) ProviderID() string {
	return "script"
}

func (s *scriptBinder) Fetch(context.Context) (provider.Report, error) {
	return provider.Report{}, nil
}

func (s *scriptBinder) Claim(ctx context.Context, req provider.ClaimRequest) (provider.Handle, error) {
	if s.claimFn != nil {
		return s.claimFn(ctx, req)
	}
	return provider.Handle{}, provider.Unavailable(errors.New("unimplemented"))
}

func (s *scriptBinder) Lookup(ctx context.Context, req provider.ClaimRequest) (provider.Handle, error) {
	if s.lookupFn != nil {
		return s.lookupFn(ctx, req)
	}
	return provider.Handle{}, provider.ErrNotFound
}

func (s *scriptBinder) Release(ctx context.Context, h provider.Handle) error {
	if s.releaseFn != nil {
		return s.releaseFn(ctx, h)
	}
	return nil
}

func (s *scriptBinder) LookupMany(ctx context.Context, reqs []provider.ClaimRequest) []provider.LookupResult {
	if s.lookupManyFn != nil {
		return s.lookupManyFn(ctx, reqs)
	}
	out := make([]provider.LookupResult, len(reqs))
	for i, r := range reqs {
		h, err := s.Lookup(ctx, r)
		out[i] = provider.LookupResult{Handle: h, Err: err}
	}
	return out
}

func (s *scriptBinder) asProvider(batch bool) provider.Provider {
	p := provider.Provider{
		Source: s,
		Binder: s,
	}
	if batch {
		p.BatchLookup = s
	}
	return p
}

func newTestBinder(t *testing.T, opts binder.Options) (*binder.Binder, *registry.Registry) {
	t.Helper()
	reg := registry.New(registry.Options{})
	opts.Registry = reg
	if opts.Owner == "" {
		opts.Owner = provider.OwnerID("sandbox-scheduler", "default", "cluster-uid-1")
	}
	b := binder.New(opts)
	t.Cleanup(func() {
		_ = b.Shutdown(context.Background())
	})
	return b, reg
}

func TestNoCapacitySpillsKeepsReservationAndScopesRefusalByTemplate(t *testing.T) {
	now := time.Now()
	b, reg := newTestBinder(t, binder.Options{
		Now:        func() time.Time { return now },
		RefusalTTL: 5 * time.Second,
	})

	reg.Report("civo", registry.Report{WarmCapacity: 1})
	reg.Report("gke", registry.Report{WarmCapacity: 1})
	reg.SetConfig("civo", registry.ProviderConfig{CostPerHour: 1, Templates: map[string]string{"gpu": "gpu-pool", "python": "py-pool"}})
	reg.SetConfig("gke", registry.ProviderConfig{CostPerHour: 4, Templates: map[string]string{"gpu": "gpu-pool", "python": "py-pool"}})

	var civoCalls atomic.Int32
	civoFake := &scriptBinder{
		claimFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			civoCalls.Add(1)
			if req.Template == "gpu" {
				return provider.Handle{}, provider.NoCapacity(errors.New("gpu pool empty"))
			}
			return provider.Handle{Provider: "civo", ID: "civo-" + req.Key, Endpoint: "http://civo:8080"}, nil
		},
	}
	gkeFake := adaptertest.NewFake("gke", 10, false)

	b.SetTarget("civo", civoFake.asProvider(false), binder.TargetConfig{
		Enabled:   true,
		Templates: map[string]string{"gpu": "gpu-pool", "python": "py-pool"},
	})
	b.SetTarget("gke", gkeFake.Provider(false), binder.TargetConfig{
		Enabled:   true,
		Templates: map[string]string{"gpu": "gpu-pool", "python": "py-pool"},
	})

	// 1. Claiming template "gpu" tries civo first, gets NoCapacity, keeps civo's
	// reservation consumed, records refusal for (civo, "gpu"), and spills to gke.
	res, berr := b.Bind(context.Background(), nil, binder.Request{
		Key:      "req-gpu-1",
		Template: "gpu",
	})
	if berr != nil {
		t.Fatalf("unexpected error: %v", berr)
	}
	if res.Handle.Provider != "gke" {
		t.Fatalf("expected spill to gke, got %q", res.Handle.Provider)
	}
	if civoCalls.Load() != 1 {
		t.Fatalf("expected 1 call to civo, got %d", civoCalls.Load())
	}

	// Replenish warm capacity on civo so only the template refusal cache could exclude it.
	reg.Report("civo", registry.Report{WarmCapacity: 2})
	reg.Report("gke", registry.Report{WarmCapacity: 2})

	// 2. A second "gpu" request within RefusalTTL skips civo without calling it.
	res2, berr := b.Bind(context.Background(), nil, binder.Request{
		Key:      "req-gpu-2",
		Template: "gpu",
	})
	if berr != nil {
		t.Fatalf("unexpected error on second gpu claim: %v", berr)
	}
	if res2.Handle.Provider != "gke" {
		t.Fatalf("expected civo to be skipped while refused for gpu, got %q", res2.Handle.Provider)
	}
	if civoCalls.Load() != 1 {
		t.Fatalf("civo should not have been called for gpu while refused, calls=%d", civoCalls.Load())
	}

	// 3. A "python" request on the same provider is NOT suppressed by the "gpu" refusal!
	resPy, berr := b.Bind(context.Background(), nil, binder.Request{
		Key:      "req-py-1",
		Template: "python",
	})
	if berr != nil {
		t.Fatalf("unexpected error on python claim: %v", berr)
	}
	if resPy.Handle.Provider != "civo" {
		t.Fatalf("expected python template to still place on civo, got %q", resPy.Handle.Provider)
	}
}

func TestUnavailableReleasesReservationAndSpills(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{})
	reg.Report("civo", registry.Report{WarmCapacity: 1})
	reg.Report("gke", registry.Report{WarmCapacity: 1})
	reg.SetConfig("civo", registry.ProviderConfig{CostPerHour: 1})
	reg.SetConfig("gke", registry.ProviderConfig{CostPerHour: 4})

	civoBinder := &scriptBinder{
		claimFn: func(context.Context, provider.ClaimRequest) (provider.Handle, error) {
			return provider.Handle{}, provider.Unavailable(errors.New("connection refused"))
		},
	}
	gkeFake := adaptertest.NewFake("gke", 10, false)

	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{Enabled: true})
	b.SetTarget("gke", gkeFake.Provider(false), binder.TargetConfig{Enabled: true})

	res, berr := b.Bind(context.Background(), nil, binder.Request{Key: "k-unavail"})
	if berr != nil {
		t.Fatalf("unexpected error: %v", berr)
	}
	if res.Handle.Provider != "gke" {
		t.Fatalf("expected spill to gke, got %q", res.Handle.Provider)
	}

	// Verify civo's warm reservation was released back to the ledger.
	for _, st := range reg.Statuses() {
		if st.Provider == "civo" && st.Reserved != 0 {
			t.Fatalf("expected civo Reserved=0 after Unavailable release, got %d", st.Reserved)
		}
	}
}

func TestKeyConflictReturns409WithoutSpilling(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{})
	reg.Report("civo", registry.Report{WarmCapacity: 1})
	reg.Report("gke", registry.Report{WarmCapacity: 1})
	reg.SetConfig("civo", registry.ProviderConfig{CostPerHour: 1})
	reg.SetConfig("gke", registry.ProviderConfig{CostPerHour: 4})

	var gkeCalled atomic.Bool
	civoBinder := &scriptBinder{
		claimFn: func(context.Context, provider.ClaimRequest) (provider.Handle, error) {
			return provider.Handle{}, provider.KeyConflict(errors.New("spec hash mismatch"))
		},
	}
	gkeBinder := &scriptBinder{
		claimFn: func(context.Context, provider.ClaimRequest) (provider.Handle, error) {
			gkeCalled.Store(true)
			return provider.Handle{Provider: "gke", ID: "sb-1"}, nil
		},
	}

	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{Enabled: true})
	b.SetTarget("gke", gkeBinder.asProvider(false), binder.TargetConfig{Enabled: true})

	_, berr := b.Bind(context.Background(), nil, binder.Request{Key: "k-conflict"})
	if berr == nil || berr.Status != 409 || berr.Reason != binder.ReasonSpecMismatch {
		t.Fatalf("expected 409 SpecMismatch, got %+v", berr)
	}
	if gkeCalled.Load() {
		t.Fatal("KeyConflict must not spill to another provider")
	}
}

func TestUnknownOutcomeProbesLookupAndNeverSpills(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{
		RandFloat64: func() float64 { return 0.01 },
	})
	reg.Report("civo", registry.Report{WarmCapacity: 1})
	reg.Report("gke", registry.Report{WarmCapacity: 1})
	reg.SetConfig("civo", registry.ProviderConfig{CostPerHour: 1})
	reg.SetConfig("gke", registry.ProviderConfig{CostPerHour: 4})

	var gkeCalled atomic.Bool
	civoBinder := &scriptBinder{
		claimFn: func(context.Context, provider.ClaimRequest) (provider.Handle, error) {
			// Pretend the response was lost after creating the sandbox.
			return provider.Handle{}, errors.New("EOF after write")
		},
		lookupFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			return provider.Handle{
				Provider: "civo",
				ID:       "sb-" + req.Key,
				Endpoint: "http://civo:8080",
			}, nil
		},
	}
	gkeBinder := &scriptBinder{
		claimFn: func(context.Context, provider.ClaimRequest) (provider.Handle, error) {
			gkeCalled.Store(true)
			return provider.Handle{Provider: "gke", ID: "sb-gke"}, nil
		},
	}

	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{Enabled: true})
	b.SetTarget("gke", gkeBinder.asProvider(false), binder.TargetConfig{Enabled: true})

	res, berr := b.Bind(context.Background(), nil, binder.Request{Key: "k-unknown-ok"})
	if berr != nil {
		t.Fatalf("expected recovery via Lookup, got error: %+v", berr)
	}
	if res.Handle.Provider != "civo" || !res.Replay {
		t.Fatalf("expected replay from civo, got %+v", res)
	}
	if gkeCalled.Load() {
		t.Fatal("UnknownOutcome must never spill to gke")
	}
}

func TestRetryProbesAllProvidersUnfilteredAndCoalescesBatches(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{
		CoalesceWindow: 20 * time.Millisecond,
	})
	// Even if civo has no warm capacity in the snapshot, retry:true must still
	// probe it unfiltered.
	reg.Report("civo", registry.Report{WarmCapacity: 0})
	reg.Report("gke", registry.Report{WarmCapacity: 2})

	var batchCalls atomic.Int32
	civoBinder := &scriptBinder{
		lookupManyFn: func(_ context.Context, reqs []provider.ClaimRequest) []provider.LookupResult {
			batchCalls.Add(1)
			out := make([]provider.LookupResult, len(reqs))
			for i, r := range reqs {
				out[i] = provider.LookupResult{
					Handle: provider.Handle{
						Provider: "civo",
						ID:       "found-" + r.Key,
						Endpoint: "http://civo:8080",
					},
				}
			}
			return out
		},
	}
	gkeFake := adaptertest.NewFake("gke", 10, false)

	b.SetTarget("civo", civoBinder.asProvider(true), binder.TargetConfig{
		Enabled:    true,
		LookupRate: 1, // Only 1 batch token/sec: concurrent retries must share a batch!
	})
	b.SetTarget("gke", gkeFake.Provider(false), binder.TargetConfig{
		Enabled:    true,
		LookupRate: 100,
	})

	const n = 5
	var wg sync.WaitGroup
	errs := make([]*binder.Error, n)
	results := make([]*binder.Result, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "retry-key-" + string(rune('a'+i))
			results[i], errs[i] = b.Bind(context.Background(), nil, binder.Request{
				Key:   key,
				Retry: true,
			})
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("req %d failed: %+v", i, errs[i])
		}
		if results[i].Handle.Provider != "civo" || !results[i].Replay {
			t.Fatalf("req %d expected replay from civo, got %+v", i, results[i])
		}
	}
	if got := batchCalls.Load(); got > 2 {
		t.Fatalf("expected concurrent retry lookups to coalesce into <=2 batches, got %d", got)
	}
}

func TestSingleflightCoalescesAndEnforcesJoinerConstraints(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{MaxJoinersPerKey: 4})
	reg.Report("civo", registry.Report{WarmCapacity: 0}) // cold start
	reg.SetConfig("civo", registry.ProviderConfig{Attributes: map[string]string{"tier": "dev"}})

	started := make(chan struct{})
	releaseClaim := make(chan struct{})
	var once sync.Once
	civoBinder := &scriptBinder{
		claimFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			once.Do(func() { close(started) })
			<-releaseClaim
			return provider.Handle{Provider: "civo", ID: "sb-" + req.Key}, nil
		},
	}
	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{Enabled: true})

	var (
		leaderRes *binder.Result
		leaderErr *binder.Error
		warmRes   *binder.Result
		warmErr   *binder.Error
		specErr   *binder.Error
		wg        sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		leaderRes, leaderErr = b.Bind(context.Background(), nil, binder.Request{
			Key: "sf-key",
		})
	}()

	<-started

	// Joiner with differing spec -> immediate 409 SpecMismatch.
	_, specErr = b.Bind(context.Background(), nil, binder.Request{
		Key:      "sf-key",
		Template: "different-template",
	})
	if specErr == nil || specErr.Status != 409 || specErr.Reason != binder.ReasonSpecMismatch {
		t.Fatalf("expected 409 SpecMismatch for joiner with different spec, got %+v", specErr)
	}

	// Joiner with matching specHash (WarmOnly is not part of specHash) but
	// WarmOnly: true -> once leader finishes cold on civo, joiner gets 409 ConstraintExcluded.
	wg.Add(1)
	go func() {
		defer wg.Done()
		warmRes, warmErr = b.Bind(context.Background(), nil, binder.Request{
			Key:      "sf-key",
			WarmOnly: true,
		})
	}()

	time.Sleep(15 * time.Millisecond)
	close(releaseClaim)
	wg.Wait()

	if leaderErr != nil || leaderRes == nil || leaderRes.Replay {
		t.Fatalf("leader failed: res=%+v err=%+v", leaderRes, leaderErr)
	}
	if warmErr == nil || warmErr.Status != 409 || warmErr.Reason != binder.ReasonConstraintExcluded {
		t.Fatalf("expected joiner with WarmOnly=true to get 409 ConstraintExcluded, got res=%+v err=%+v", warmRes, warmErr)
	}
}

func TestSlotsFullSpillsWithoutBlockingOrCountingSpill(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{
		QueueTimeout: 20 * time.Millisecond,
	})
	reg.Report("civo", registry.Report{WarmCapacity: 2})
	reg.Report("gke", registry.Report{WarmCapacity: 2})
	reg.SetConfig("civo", registry.ProviderConfig{CostPerHour: 1})
	reg.SetConfig("gke", registry.ProviderConfig{CostPerHour: 4})

	blockCivo := make(chan struct{})
	civoInFlight := make(chan struct{})
	civoBinder := &scriptBinder{
		claimFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			close(civoInFlight)
			<-blockCivo
			return provider.Handle{Provider: "civo", ID: "civo-" + req.Key}, nil
		},
	}
	gkeFake := adaptertest.NewFake("gke", 10, false)

	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{
		Enabled:     true,
		MaxInFlight: 1,
	})
	b.SetTarget("gke", gkeFake.Provider(false), binder.TargetConfig{
		Enabled:     true,
		MaxInFlight: 2,
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = b.Bind(context.Background(), nil, binder.Request{Key: "slot-1"})
	}()

	<-civoInFlight

	// Second request sees civo's single slot full and immediately places on gke.
	res2, berr := b.Bind(context.Background(), nil, binder.Request{Key: "slot-2"})
	if berr != nil {
		t.Fatalf("unexpected error: %+v", berr)
	}
	if res2.Handle.Provider != "gke" {
		t.Fatalf("expected immediate re-decision to gke when civo slot is full, got %q", res2.Handle.Provider)
	}

	close(blockCivo)
	wg.Wait()
}

func TestPostElectionHoldOffCoversMaxProviderTimeoutPlusSkewGrace(t *testing.T) {
	var nowMu sync.Mutex
	now := time.Now()
	b, reg := newTestBinder(t, binder.Options{
		Now: func() time.Time {
			nowMu.Lock()
			defer nowMu.Unlock()
			return now
		},
	})
	reg.Report("civo", registry.Report{WarmCapacity: 1})
	fake := adaptertest.NewFake("civo", 10, false)
	b.SetTarget("civo", fake.Provider(false), binder.TargetConfig{
		Enabled: true,
		Timeout: 12 * time.Second,
	})

	b.NotifyElected()

	// At +12s (before RemoteTimeoutSkewGrace = 500ms has elapsed), hold-off must still block!
	nowMu.Lock()
	now = now.Add(12 * time.Second)
	nowMu.Unlock()

	_, berr := b.Bind(context.Background(), nil, binder.Request{Key: "holdoff-key"})
	if berr == nil || berr.Status != 503 || berr.Reason != binder.ReasonHoldOff {
		t.Fatalf("expected 503 HoldOff at +12s (within SkewGrace), got %+v", berr)
	}

	// At +12.6s (past 12s + 500ms), hold-off is over and claims proceed.
	nowMu.Lock()
	now = now.Add(600 * time.Millisecond)
	nowMu.Unlock()

	res, berr := b.Bind(context.Background(), nil, binder.Request{Key: "holdoff-key"})
	if berr != nil {
		t.Fatalf("expected claim to succeed after hold-off window, got %+v", berr)
	}
	if res.Handle.Provider != "civo" {
		t.Fatalf("expected civo, got %q", res.Handle.Provider)
	}
}

func TestReleaseTreatsNotFoundAsIdempotentSuccess(t *testing.T) {
	b, _ := newTestBinder(t, binder.Options{})
	fake := adaptertest.NewFake("civo", 10, false)
	b.SetTarget("civo", fake.Provider(false), binder.TargetConfig{Enabled: true})

	if berr := b.Release(context.Background(), "civo", "non-existent"); berr != nil {
		t.Fatalf("expected nil on releasing non-existent sandbox, got %+v", berr)
	}
}

func TestWarmOnlyRequiresWarmCapacity(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{})
	reg.Report("civo", registry.Report{WarmCapacity: 0})
	fake := adaptertest.NewFake("civo", 0, false)
	b.SetTarget("civo", fake.Provider(false), binder.TargetConfig{Enabled: true})

	policy := &v1alpha1.SandboxPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
	}
	_, berr := b.Bind(context.Background(), policy, binder.Request{
		Key:      "warm-only-key",
		WarmOnly: true,
	})
	if berr == nil || berr.Status != 409 || berr.Reason != binder.ReasonNoCapacity {
		t.Fatalf("expected 409 NoCapacity when WarmOnly=true and WarmCapacity=0, got %+v", berr)
	}
}

func TestPinnedKeyBusyRetryReacquiresWarmCapacity(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{
		RandFloat64: func() float64 { return 0.01 },
	})
	reg.Report("civo", registry.Report{WarmCapacity: 1})

	var attempts atomic.Int32
	civoBinder := &scriptBinder{
		claimFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			if attempts.Add(1) == 1 {
				return provider.Handle{}, provider.KeyBusy(5*time.Millisecond, errors.New("transient key lock"))
			}
			return provider.Handle{Provider: "civo", ID: "sb-" + req.Key}, nil
		},
	}
	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{Enabled: true})

	res, berr := b.Bind(context.Background(), nil, binder.Request{
		Key:      "key-busy-1",
		WarmOnly: true,
	})
	if berr != nil {
		t.Fatalf("unexpected error on KeyBusy retry: %+v", berr)
	}
	if !res.Warm {
		t.Fatalf("expected Warm=true after reacquiring warm reservation on retry")
	}
	for _, st := range reg.Statuses() {
		if st.Provider == "civo" && st.Reserved != 1 {
			t.Fatalf("expected civo Reserved=1 after KeyBusy retry succeeded, got %d", st.Reserved)
		}
	}
}

func TestReplayEvaluatesReachablePolicyFilter(t *testing.T) {
	b, _ := newTestBinder(t, binder.Options{})
	// Do not report capacity for "unreachable", so its candidate has Reachable=false.
	unreachBinder := &scriptBinder{
		lookupFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			return provider.Handle{Provider: "unreachable", ID: "sb-" + req.Key}, nil
		},
	}
	b.SetTarget("unreachable", unreachBinder.asProvider(false), binder.TargetConfig{Enabled: true})

	policy := &v1alpha1.SandboxPlacementPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "reachable-only"},
		Spec: v1alpha1.SandboxPlacementPolicySpec{
			Filters: []string{"Reachable"},
		},
	}
	_, berr := b.Bind(context.Background(), policy, binder.Request{
		Key:   "existing-unreachable-sb",
		Retry: true,
	})
	if berr == nil || berr.Status != 409 || berr.Reason != binder.ReasonConstraintExcluded {
		t.Fatalf("expected 409 ConstraintExcluded when replayed sandbox violates Reachable policy filter, got %+v", berr)
	}
}

func TestPinnedKeyBusyRecoversUnknownOutcome(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{
		RandFloat64: func() float64 { return 0.01 },
	})
	reg.Report("civo", registry.Report{WarmCapacity: 1})

	var attempts atomic.Int32
	civoBinder := &scriptBinder{
		claimFn: func(context.Context, provider.ClaimRequest) (provider.Handle, error) {
			if attempts.Add(1) == 1 {
				return provider.Handle{}, provider.KeyBusy(5*time.Millisecond, errors.New("deleting old"))
			}
			return provider.Handle{}, errors.New("EOF after write")
		},
		lookupFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			return provider.Handle{Provider: "civo", ID: "sb-" + req.Key}, nil
		},
	}
	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{Enabled: true})

	res, berr := b.Bind(context.Background(), nil, binder.Request{Key: "kb-unknown"})
	if berr != nil {
		t.Fatalf("expected unknown outcome after KeyBusy retry to recover via Lookup, got %+v", berr)
	}
	if !res.Replay || res.Handle.Provider != "civo" {
		t.Fatalf("expected Replay=true from civo, got %+v", res)
	}
}

func TestMixedBusyAndUnavailableSpillReturns503(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{})
	reg.Report("civo", registry.Report{WarmCapacity: 1})
	reg.Report("gke", registry.Report{WarmCapacity: 1})
	reg.SetConfig("civo", registry.ProviderConfig{CostPerHour: 1})
	reg.SetConfig("gke", registry.ProviderConfig{CostPerHour: 4})

	civoBinder := &scriptBinder{
		claimFn: func(context.Context, provider.ClaimRequest) (provider.Handle, error) {
			return provider.Handle{}, provider.Busy(time.Second, errors.New("429 too many requests"))
		},
	}
	gkeBinder := &scriptBinder{
		claimFn: func(context.Context, provider.ClaimRequest) (provider.Handle, error) {
			return provider.Handle{}, provider.Unavailable(errors.New("503 upstream down"))
		},
	}
	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{Enabled: true})
	b.SetTarget("gke", gkeBinder.asProvider(false), binder.TargetConfig{Enabled: true})

	_, berr := b.Bind(context.Background(), nil, binder.Request{Key: "mixed-spill"})
	if berr == nil || berr.Status != 503 || berr.Reason != binder.ReasonUnavailable {
		t.Fatalf("expected 503 Unavailable for mixed Busy+Unavailable spill, got %+v", berr)
	}
}

func TestIneligibleOverRateProviderReturnsNoCapacityNotRateLimited(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{})
	reg.Report("civo", registry.Report{WarmCapacity: 0})

	civoBinder := &scriptBinder{
		claimFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			return provider.Handle{Provider: "civo", ID: "sb-" + req.Key}, nil
		},
	}
	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{
		Enabled: true,
		MaxRate: 1,
	})

	// Drain the single cold rate token on civo.
	if _, berr := b.Bind(context.Background(), nil, binder.Request{Key: "drain-1"}); berr != nil {
		t.Fatalf("drain claim failed: %+v", berr)
	}

	// Now civo has 0 tokens and WarmCapacity=0. A WarmOnly request must fail
	// with 409 NoCapacity (ineligible due to WarmOnly), not 429 RateLimited.
	_, berr := b.Bind(context.Background(), nil, binder.Request{
		Key:      "warm-req",
		WarmOnly: true,
	})
	if berr == nil || berr.Status != 409 || berr.Reason != binder.ReasonNoCapacity {
		t.Fatalf("expected 409 NoCapacity for WarmOnly request on cold over-rate provider, got %+v", berr)
	}
}

func TestUnknownOutcomeConfirmedNotFoundThenNoCapacitySpills(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{
		RandFloat64: func() float64 { return 0.01 },
	})
	reg.Report("civo", registry.Report{WarmCapacity: 1})
	reg.Report("gke", registry.Report{WarmCapacity: 1})
	reg.SetConfig("civo", registry.ProviderConfig{CostPerHour: 1})
	reg.SetConfig("gke", registry.ProviderConfig{CostPerHour: 4})

	var civoClaims atomic.Int32
	civoBinder := &scriptBinder{
		claimFn: func(context.Context, provider.ClaimRequest) (provider.Handle, error) {
			if civoClaims.Add(1) == 1 {
				return provider.Handle{}, errors.New("EOF after write")
			}
			return provider.Handle{}, provider.NoCapacity(errors.New("pool empty on retry"))
		},
		lookupFn: func(context.Context, provider.ClaimRequest) (provider.Handle, error) {
			return provider.Handle{}, provider.ErrNotFound
		},
	}
	gkeFake := adaptertest.NewFake("gke", 10, false)

	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{Enabled: true})
	b.SetTarget("gke", gkeFake.Provider(false), binder.TargetConfig{Enabled: true})

	res, berr := b.Bind(context.Background(), nil, binder.Request{Key: "unknown-then-nocap"})
	if berr != nil {
		t.Fatalf("expected spill to gke after Lookup ErrNotFound + NoCapacity, got %+v", berr)
	}
	if res.Handle.Provider != "gke" {
		t.Fatalf("expected provider gke, got %q", res.Handle.Provider)
	}
}

func TestCompletedFlightDeduplicatesOverlappingCallersUntilRelease(t *testing.T) {
	b, reg := newTestBinder(t, binder.Options{})
	reg.Report("civo", registry.Report{WarmCapacity: 2})
	reg.Report("gke", registry.Report{WarmCapacity: 2})
	reg.SetConfig("civo", registry.ProviderConfig{CostPerHour: 3})
	reg.SetConfig("gke", registry.ProviderConfig{CostPerHour: 4})

	civoFake := adaptertest.NewFake("civo", 2, false)
	var gkeClaims atomic.Int32
	gkeBinder := &scriptBinder{
		claimFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			gkeClaims.Add(1)
			return provider.Handle{Provider: "gke", ID: "gke-" + req.Key}, nil
		},
	}
	b.SetTarget("civo", civoFake.Provider(false), binder.TargetConfig{Enabled: true})
	b.SetTarget("gke", gkeBinder.asProvider(false), binder.TargetConfig{Enabled: true})

	// First caller claims "overlap-key" on civo, consuming civo's single warm slot.
	res1, berr := b.Bind(context.Background(), nil, binder.Request{Key: "overlap-key"})
	if berr != nil {
		t.Fatalf("first claim failed: %+v", berr)
	}
	if res1.Handle.Provider != "civo" || res1.Replay {
		t.Fatalf("expected fresh claim on civo, got %+v", res1)
	}

	// Second overlapping caller with retry:false arrives just after the first
	// flight finishes. Even though gke now outscores civo (warm vs depleted),
	// it must replay the completed flight on civo rather than placing on gke.
	res2, berr := b.Bind(context.Background(), nil, binder.Request{Key: "overlap-key"})
	if berr != nil {
		t.Fatalf("second overlapping claim failed: %+v", berr)
	}
	if !res2.Replay || res2.Handle.Provider != "civo" || res2.Handle.ID != res1.Handle.ID {
		t.Fatalf("expected replay of civo handle, got %+v", res2)
	}
	if gkeClaims.Load() != 0 {
		t.Fatalf("expected 0 claims on gke, got %d", gkeClaims.Load())
	}

	// Releasing the sandbox evicts the completed flight so a new claim with the
	// same key can place freshly on gke.
	if berr := b.Release(context.Background(), "civo", res1.Handle.ID); berr != nil {
		t.Fatalf("release failed: %+v", berr)
	}
	res3, berr := b.Bind(context.Background(), nil, binder.Request{Key: "overlap-key"})
	if berr != nil {
		t.Fatalf("third claim after release failed: %+v", berr)
	}
	if res3.Replay || res3.Handle.Provider != "gke" {
		t.Fatalf("expected fresh placement on gke after release, got %+v", res3)
	}
}

func TestSetTargetReusesRateLimitersWhenRatesUnchanged(t *testing.T) {
	now := time.Now()
	b, reg := newTestBinder(t, binder.Options{
		Now: func() time.Time { return now },
	})
	reg.Report("civo", registry.Report{WarmCapacity: 0})

	civoFake := adaptertest.NewFake("civo", 0, false)
	cfg := binder.TargetConfig{
		Enabled: true,
		MaxRate: 1,
	}
	b.SetTarget("civo", civoFake.Provider(false), cfg)

	// Drain the single cold rate token.
	if _, berr := b.Bind(context.Background(), nil, binder.Request{Key: "rate-1"}); berr != nil {
		t.Fatalf("first claim failed: %+v", berr)
	}

	// Re-reconcile with identical MaxRate: must preserve the drained limiter.
	b.SetTarget("civo", civoFake.Provider(false), cfg)

	_, berr := b.Bind(context.Background(), nil, binder.Request{Key: "rate-2"})
	if berr == nil || berr.Status != 429 || berr.Reason != binder.ReasonRateLimited {
		t.Fatalf("expected 429 RateLimited after unchanged SetTarget, got %+v", berr)
	}
}

func TestHalfOpenProbeUsesConfiguredProviderTemplate(t *testing.T) {
	var nowMu sync.Mutex
	now := time.Now()
	b, reg := newTestBinder(t, binder.Options{
		Now: func() time.Time {
			nowMu.Lock()
			defer nowMu.Unlock()
			return now
		},
	})
	reg.Report("civo", registry.Report{WarmCapacity: 2})
	reg.Report("gke", registry.Report{WarmCapacity: 2})
	reg.SetConfig("civo", registry.ProviderConfig{CostPerHour: 1, Templates: map[string]string{"python": "py-pool"}})
	reg.SetConfig("gke", registry.ProviderConfig{CostPerHour: 4, Templates: map[string]string{"python": "py-pool"}})

	var civoDown atomic.Bool
	civoDown.Store(true)
	probeDone := make(chan struct{}, 1)
	civoBinder := &scriptBinder{
		claimFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			if civoDown.Load() {
				return provider.Handle{}, provider.Unavailable(errors.New("503 gateway down"))
			}
			return provider.Handle{Provider: "civo", ID: "civo-" + req.Key}, nil
		},
		lookupFn: func(_ context.Context, req provider.ClaimRequest) (provider.Handle, error) {
			defer func() {
				select {
				case probeDone <- struct{}{}:
				default:
				}
			}()
			if req.Template != "python" {
				return provider.Handle{}, provider.Rejected(errors.New("unknown template " + req.Template))
			}
			return provider.Handle{}, provider.ErrNotFound
		},
	}
	gkeFake := adaptertest.NewFake("gke", 20, false)

	b.SetTarget("civo", civoBinder.asProvider(false), binder.TargetConfig{
		Enabled:   true,
		Templates: map[string]string{"python": "py-pool"},
	})
	b.SetTarget("gke", gkeFake.Provider(false), binder.TargetConfig{
		Enabled:   true,
		Templates: map[string]string{"python": "py-pool"},
	})

	// Trigger 20 Unavailable outcomes on civo to open its circuit breaker.
	for i := 0; i < 20; i++ {
		reg.Report("civo", registry.Report{WarmCapacity: 2})
		reg.Report("gke", registry.Report{WarmCapacity: 2})
		key := "demote-" + string(rune('a'+i))
		if _, berr := b.Bind(context.Background(), nil, binder.Request{Key: key, Template: "python"}); berr != nil {
			t.Fatalf("spill claim %d failed: %+v", i, berr)
		}
	}
	if hs, ok := b.HealthStatus("civo"); !ok || hs.Healthy {
		t.Fatalf("expected civo to be demoted after 20 Unavailable errors, got %+v", hs)
	}

	// Bring civo back and advance time past probeInitialBackoff (2s).
	civoDown.Store(false)
	nowMu.Lock()
	now = now.Add(3 * time.Second)
	nowMu.Unlock()

	reg.Report("civo", registry.Report{WarmCapacity: 2})
	reg.Report("gke", registry.Report{WarmCapacity: 2})
	if _, berr := b.Bind(context.Background(), nil, binder.Request{Key: "trigger-probe", Template: "python"}); berr != nil {
		t.Fatalf("claim triggering half-open probe failed: %+v", berr)
	}

	select {
	case <-probeDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for half-open probe Lookup")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if hs, ok := b.HealthStatus("civo"); ok && hs.Healthy && !hs.CredentialsError {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	hs, _ := b.HealthStatus("civo")
	t.Fatalf("expected civo to recover after half-open probe with valid template, got %+v", hs)
}
