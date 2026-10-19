package reaper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/NolanFoster/sandbox-scheduler/pkg/metrics"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider/adaptertest"
)

func TestReaperDeletesExpiredWithVersionPrecondition(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	fb := adaptertest.NewFake("e2b-primary", 0, true)
	fb.SetNow(func() time.Time { return now })

	owner := provider.OwnerID("sandbox-scheduler", "sandbox-scheduler", "cluster-1")
	ctx := context.Background()

	// Create 3 sandboxes with TTL=1m and 2 sandboxes with TTL=10m.
	var expiredIDs []string
	var activeIDs []string
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("exp-%d", i)
		h, err := fb.Claim(ctx, provider.ClaimRequest{
			Template: "python",
			Key:      key,
			SpecHash: provider.SpecHashOf("python", nil, time.Minute),
			Owner:    owner,
			TTL:      time.Minute,
		})
		if err != nil {
			t.Fatalf("Claim(%s): %v", key, err)
		}
		expiredIDs = append(expiredIDs, h.ID)
	}
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("act-%d", i)
		h, err := fb.Claim(ctx, provider.ClaimRequest{
			Template: "python",
			Key:      key,
			SpecHash: provider.SpecHashOf("python", nil, 10*time.Minute),
			Owner:    owner,
			TTL:      10 * time.Minute,
		})
		if err != nil {
			t.Fatalf("Claim(%s): %v", key, err)
		}
		activeIDs = append(activeIDs, h.ID)
	}

	// Advance clock by 2 minutes so the 1m sandboxes are expired and 10m are live.
	now = now.Add(2 * time.Minute)

	var released []string
	r := New(Options{
		Owner:         owner,
		PageSize:      2,
		MaxPerCycle:   10,
		RatePerSecond: 1000,
		Burst:         1000,
		Now:           func() time.Time { return now },
		OnReleased: func(providerID, sandboxID string) {
			released = append(released, providerID+"/"+sandboxID)
		},
	})
	r.SetTarget("e2b-primary", fb.Provider(true), TargetConfig{Enabled: true, Timeout: time.Second})

	beforeDeleted := testutil.ToFloat64(metrics.ReapTotal.WithLabelValues("e2b-primary", OutcomeDeleted))
	r.RunOnce(ctx)
	afterDeleted := testutil.ToFloat64(metrics.ReapTotal.WithLabelValues("e2b-primary", OutcomeDeleted))

	if got := afterDeleted - beforeDeleted; got != 3 {
		t.Fatalf("expected 3 deleted metric increments, got %v", got)
	}
	if len(released) != 3 {
		t.Fatalf("expected 3 OnReleased callbacks, got %v", released)
	}

	// Verify only the 2 active sandboxes remain.
	remaining, _, err := fb.ListOwned(ctx, owner, "", 10)
	if err != nil {
		t.Fatalf("ListOwned: %v", err)
	}
	if len(remaining) != 2 {
		t.Fatalf("expected 2 remaining sandboxes, got %d (%+v)", len(remaining), remaining)
	}
	for _, h := range remaining {
		if h.ID != activeIDs[0] && h.ID != activeIDs[1] {
			t.Fatalf("unexpected remaining sandbox %q (expiredIDs=%v)", h.ID, expiredIDs)
		}
	}
}

func TestReaperDryRunDoesNotDeleteAndEmitsCallback(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	fb := adaptertest.NewFake("e2b-dry", 0, true)
	fb.SetNow(func() time.Time { return now })

	owner := provider.OwnerID("sandbox-scheduler", "sandbox-scheduler", "cluster-1")
	ctx := context.Background()

	h, err := fb.Claim(ctx, provider.ClaimRequest{
		Template: "python",
		Key:      "dry-key-1",
		SpecHash: provider.SpecHashOf("python", nil, time.Minute),
		Owner:    owner,
		TTL:      time.Minute,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	now = now.Add(2 * time.Minute)

	var dryRunProvider string
	var dryRunHandles []provider.Handle
	r := New(Options{
		Owner:         owner,
		DryRun:        true,
		RatePerSecond: 1000,
		Burst:         1000,
		Now:           func() time.Time { return now },
		OnDryRun: func(_ context.Context, providerID string, handles []provider.Handle) {
			dryRunProvider = providerID
			dryRunHandles = append(dryRunHandles, handles...)
		},
	})
	r.SetTarget("e2b-dry", fb.Provider(true), TargetConfig{Enabled: true})

	beforeDry := testutil.ToFloat64(metrics.ReapTotal.WithLabelValues("e2b-dry", OutcomeDryRun))
	r.RunOnce(ctx)
	afterDry := testutil.ToFloat64(metrics.ReapTotal.WithLabelValues("e2b-dry", OutcomeDryRun))

	if got := afterDry - beforeDry; got != 1 {
		t.Fatalf("expected 1 dry_run metric increment, got %v", got)
	}
	if dryRunProvider != "e2b-dry" || len(dryRunHandles) != 1 || dryRunHandles[0].ID != h.ID {
		t.Fatalf("unexpected OnDryRun callback: provider=%q handles=%+v", dryRunProvider, dryRunHandles)
	}

	// Sandbox must still exist on the provider.
	remaining, _, err := fb.ListOwned(ctx, owner, "", 10)
	if err != nil {
		t.Fatalf("ListOwned: %v", err)
	}
	if len(remaining) != 1 || remaining[0].ID != h.ID {
		t.Fatalf("expected sandbox %q to remain after dry-run, got %+v", h.ID, remaining)
	}
}

func TestReaperCircuitBreakerCapsDeletionsPerCycle(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	fb := adaptertest.NewFake("e2b-cb", 0, true)
	fb.SetNow(func() time.Time { return now })

	owner := provider.OwnerID("sandbox-scheduler", "sandbox-scheduler", "cluster-1")
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("cb-key-%d", i)
		if _, err := fb.Claim(ctx, provider.ClaimRequest{
			Template: "python",
			Key:      key,
			SpecHash: provider.SpecHashOf("python", nil, time.Minute),
			Owner:    owner,
			TTL:      time.Minute,
		}); err != nil {
			t.Fatalf("Claim(%s): %v", key, err)
		}
	}

	now = now.Add(2 * time.Minute)

	r := New(Options{
		Owner:         owner,
		MaxPerCycle:   2,
		RatePerSecond: 1000,
		Burst:         1000,
		Now:           func() time.Time { return now },
	})
	r.SetTarget("e2b-cb", fb.Provider(true), TargetConfig{Enabled: true})

	beforeCB := testutil.ToFloat64(metrics.ReapTotal.WithLabelValues("e2b-cb", OutcomeCircuitBreaker))
	r.RunOnce(ctx)
	afterCB := testutil.ToFloat64(metrics.ReapTotal.WithLabelValues("e2b-cb", OutcomeCircuitBreaker))

	if got := afterCB - beforeCB; got != 1 {
		t.Fatalf("expected 1 circuit_breaker metric increment, got %v", got)
	}

	remaining, _, err := fb.ListOwned(ctx, owner, "", 10)
	if err != nil {
		t.Fatalf("ListOwned: %v", err)
	}
	if len(remaining) != 3 {
		t.Fatalf("expected 3 remaining sandboxes after maxPerCycle=2, got %d", len(remaining))
	}
}

func TestReaperHonorsConfiguredLowRateBurst(t *testing.T) {
	r := New(Options{RatePerSecond: 1})
	if r.burst != 1 {
		t.Fatalf("expected burst=1 for RatePerSecond=1, got %d", r.burst)
	}
	rFrac := New(Options{RatePerSecond: 0.25})
	if rFrac.burst != 1 {
		t.Fatalf("expected burst=1 for RatePerSecond=0.25, got %d", rFrac.burst)
	}
}
