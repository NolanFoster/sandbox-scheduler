// Package adaptertest provides a reusable conformance test suite and an
// in-memory FakeBinder for provider adapters.
//
// Modelled on testing/fstest: any in-tree or third-party adapter can call
// TestBinder with a Harness to prove it obeys the idempotency, replay,
// error-classification, and bounded-lookup rules that pkg/binder relies on.
package adaptertest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
)

// Fault names a provider failure mode that TestBinder asks the harness to
// inject for the next call.
type Fault int

const (
	FaultNone Fault = iota
	FaultNoCapacity
	FaultBusy
	FaultUnavailable
	FaultRejected
	// FaultDropAfterSend simulates a claim that took effect on the provider
	// before the response was lost (connection reset or timeout), so the call
	// fails with KindUnknownOutcome while a subsequent Lookup finds the
	// sandbox.
	FaultDropAfterSend
)

// Harness connects TestBinder to an adapter implementation under test.
type Harness struct {
	Provider provider.Provider
	// InjectFault configures the next Claim call to fail with fault.
	InjectFault func(fault Fault)
	// MarkDeleting puts the sandbox identified by key into a terminating
	// state, so Claim and Lookup must report KindKeyBusy until deletion
	// finishes.
	MarkDeleting func(key string, deleting bool)
	// CallCount returns the number of underlying provider RPCs made so far,
	// used to assert that Lookup does not list every sandbox on the provider.
	CallCount func() int
}

// TestBinder runs the provider contract conformance suite against newHarness.
func TestBinder(t *testing.T, newHarness func(*testing.T) Harness) {
	t.Helper()

	t.Run("ClaimAndReplayAndLookup", func(t *testing.T) {
		h := newHarness(t)
		ctx := context.Background()
		req := sampleRequest("key-1", "python-3.12")

		first, err := h.Provider.Binder.Claim(ctx, req)
		if err != nil {
			t.Fatalf("first Claim failed: %v", err)
		}
		if first.ID == "" || first.Provider == "" {
			t.Fatalf("Claim returned incomplete handle: %+v", first)
		}

		// Repeating Claim with the same key and spec must return the same
		// sandbox rather than creating a second.
		second, err := h.Provider.Binder.Claim(ctx, req)
		if err != nil {
			t.Fatalf("replay Claim failed: %v", err)
		}
		if second.ID != first.ID {
			t.Fatalf("replay Claim returned ID %q, want %q", second.ID, first.ID)
		}

		// Lookup with matching spec must find the same sandbox.
		lookedUp, err := h.Provider.Binder.Lookup(ctx, req)
		if err != nil {
			t.Fatalf("Lookup failed: %v", err)
		}
		if lookedUp.ID != first.ID {
			t.Fatalf("Lookup returned ID %q, want %q", lookedUp.ID, first.ID)
		}
	})

	t.Run("LookupUnknownKeyReturnsErrNotFound", func(t *testing.T) {
		h := newHarness(t)
		_, err := h.Provider.Binder.Lookup(context.Background(), sampleRequest("missing-key", "python-3.12"))
		if !errors.Is(err, provider.ErrNotFound) {
			t.Fatalf("Lookup of missing key returned %v, want errors.Is(_, provider.ErrNotFound)", err)
		}
	})

	t.Run("KeyConflictOnSpecMismatch", func(t *testing.T) {
		h := newHarness(t)
		ctx := context.Background()
		req := sampleRequest("key-conflict", "python-3.12")
		if _, err := h.Provider.Binder.Claim(ctx, req); err != nil {
			t.Fatalf("initial Claim failed: %v", err)
		}

		different := sampleRequest("key-conflict", "node-22")
		if _, err := h.Provider.Binder.Claim(ctx, different); provider.KindOf(err) != provider.KindKeyConflict {
			t.Fatalf("Claim with mismatched spec returned %v (kind %v), want KindKeyConflict", err, provider.KindOf(err))
		}
		if _, err := h.Provider.Binder.Lookup(ctx, different); provider.KindOf(err) != provider.KindKeyConflict {
			t.Fatalf("Lookup with mismatched spec returned %v (kind %v), want KindKeyConflict", err, provider.KindOf(err))
		}
	})

	t.Run("DeletingSandboxReportsKeyBusy", func(t *testing.T) {
		h := newHarness(t)
		if h.MarkDeleting == nil {
			t.Skip("harness does not support MarkDeleting")
		}
		ctx := context.Background()
		req := sampleRequest("key-deleting", "python-3.12")
		if _, err := h.Provider.Binder.Claim(ctx, req); err != nil {
			t.Fatalf("initial Claim failed: %v", err)
		}

		h.MarkDeleting("key-deleting", true)
		if _, err := h.Provider.Binder.Claim(ctx, req); provider.KindOf(err) != provider.KindKeyBusy {
			t.Fatalf("Claim on deleting sandbox returned %v (kind %v), want KindKeyBusy", err, provider.KindOf(err))
		}
		if _, err := h.Provider.Binder.Lookup(ctx, req); provider.KindOf(err) != provider.KindKeyBusy {
			t.Fatalf("Lookup on deleting sandbox returned %v (kind %v), want KindKeyBusy", err, provider.KindOf(err))
		}
	})

	t.Run("LookupIsBoundedRegardlessOfSandboxCount", func(t *testing.T) {
		h := newHarness(t)
		if h.CallCount == nil {
			t.Skip("harness does not report CallCount")
		}
		ctx := context.Background()
		for i := 0; i < 40; i++ {
			req := sampleRequest(fmt.Sprintf("bulk-key-%d", i), "python-3.12")
			if _, err := h.Provider.Binder.Claim(ctx, req); err != nil {
				t.Fatalf("bulk Claim %d: %v", i, err)
			}
		}
		before := h.CallCount()
		if _, err := h.Provider.Binder.Lookup(ctx, sampleRequest("bulk-key-25", "python-3.12")); err != nil {
			t.Fatalf("Lookup failed: %v", err)
		}
		if delta := h.CallCount() - before; delta > 2 {
			t.Fatalf("Lookup issued %d provider calls with 40 sandboxes present; must not scale with sandbox count", delta)
		}
	})

	t.Run("BatchLookupMatchesIndividualLookup", func(t *testing.T) {
		h := newHarness(t)
		if h.Provider.BatchLookup == nil {
			t.Skip("provider does not implement BatchLookuper")
		}
		ctx := context.Background()
		reqA := sampleRequest("batch-a", "python-3.12")
		reqB := sampleRequest("batch-b", "python-3.12")
		hA, err := h.Provider.Binder.Claim(ctx, reqA)
		if err != nil {
			t.Fatalf("Claim A: %v", err)
		}
		if _, err := h.Provider.Binder.Claim(ctx, reqB); err != nil {
			t.Fatalf("Claim B: %v", err)
		}

		conflictB := sampleRequest("batch-b", "node-22")
		missing := sampleRequest("batch-missing", "python-3.12")
		results := h.Provider.BatchLookup.LookupMany(ctx, []provider.ClaimRequest{reqA, missing, conflictB})
		if len(results) != 3 {
			t.Fatalf("LookupMany returned %d results, want 3", len(results))
		}
		if results[0].Err != nil || results[0].Handle.ID != hA.ID {
			t.Fatalf("result[0] = %+v, want handle %q", results[0], hA.ID)
		}
		if !errors.Is(results[1].Err, provider.ErrNotFound) {
			t.Fatalf("result[1] err = %v, want ErrNotFound", results[1].Err)
		}
		if provider.KindOf(results[2].Err) != provider.KindKeyConflict {
			t.Fatalf("result[2] kind = %v, want KindKeyConflict", provider.KindOf(results[2].Err))
		}
	})

	t.Run("FaultTaxonomyAndDropAfterSendRecovery", func(t *testing.T) {
		h := newHarness(t)
		if h.InjectFault == nil {
			t.Skip("harness does not support InjectFault")
		}
		ctx := context.Background()
		cases := []struct {
			fault Fault
			want  provider.Kind
		}{
			{FaultNoCapacity, provider.KindNoCapacity},
			{FaultBusy, provider.KindBusy},
			{FaultUnavailable, provider.KindUnavailable},
			{FaultRejected, provider.KindRejected},
		}
		for i, tc := range cases {
			h.InjectFault(tc.fault)
			req := sampleRequest(fmt.Sprintf("fault-%d", i), "python-3.12")
			_, err := h.Provider.Binder.Claim(ctx, req)
			if got := provider.KindOf(err); got != tc.want {
				t.Fatalf("fault %v produced kind %v (%v), want %v", tc.fault, got, err, tc.want)
			}
		}

		// Drop-after-send must return KindUnknownOutcome, and a subsequent
		// Lookup must find the created sandbox.
		h.InjectFault(FaultDropAfterSend)
		droppedReq := sampleRequest("dropped-key", "python-3.12")
		if _, err := h.Provider.Binder.Claim(ctx, droppedReq); provider.KindOf(err) != provider.KindUnknownOutcome {
			t.Fatalf("drop-after-send produced kind %v, want KindUnknownOutcome", provider.KindOf(err))
		}
		recovered, err := h.Provider.Binder.Lookup(ctx, droppedReq)
		if err != nil || recovered.ID == "" {
			t.Fatalf("Lookup after drop-after-send failed to recover sandbox: %+v, %v", recovered, err)
		}
	})

	t.Run("ReleaseAndVersionPrecondition", func(t *testing.T) {
		h := newHarness(t)
		ctx := context.Background()
		req := sampleRequest("release-key", "python-3.12")
		created, err := h.Provider.Binder.Claim(ctx, req)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}

		// A stale Version precondition must not delete the live sandbox.
		if created.Version != "" {
			stale := provider.Handle{Provider: created.Provider, ID: created.ID, Version: "stale-version"}
			_ = h.Provider.Binder.Release(ctx, stale)
			if _, err := h.Provider.Binder.Lookup(ctx, req); err != nil {
				t.Fatalf("Release with mismatched Version deleted the sandbox: %v", err)
			}
		}

		// Release by Provider and ID alone must succeed and be idempotent.
		if err := h.Provider.Binder.Release(ctx, provider.Handle{Provider: created.Provider, ID: created.ID}); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if err := h.Provider.Binder.Release(ctx, provider.Handle{Provider: created.Provider, ID: created.ID}); err != nil {
			t.Fatalf("second Release should return nil when already gone, got: %v", err)
		}
		if _, err := h.Provider.Binder.Lookup(ctx, req); !errors.Is(err, provider.ErrNotFound) {
			t.Fatalf("Lookup after Release = %v, want ErrNotFound", err)
		}
	})
}

func sampleRequest(key, template string) provider.ClaimRequest {
	ttl := 10 * time.Minute
	labels := map[string]string{provider.DefaultAllowedLabelDomain + "/suite": "adaptertest"}
	return provider.ClaimRequest{
		Template: template,
		Key:      key,
		SpecHash: provider.SpecHashOf(template, labels, ttl),
		Owner:    provider.OwnerID("test", "default", "cluster-1"),
		Labels:   labels,
		TTL:      ttl,
	}
}
