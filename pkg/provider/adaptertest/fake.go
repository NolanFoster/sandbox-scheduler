package adaptertest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
)

// Fake is an in-memory implementation of provider.Source, provider.Binder,
// provider.BatchLookuper, and provider.OwnedLister for tests.
type Fake struct {
	id      string
	elastic bool
	warm    atomic.Int64
	calls   atomic.Int64

	mu       sync.Mutex
	fault    Fault
	records  map[string]*fakeRecord
	byID     map[string]string
	nextVers uint64

	// Hook, when set, runs at the start of Claim and Lookup so tests can
	// inject custom delays, panics, or context-cancellation checks.
	Hook func(ctx context.Context, op string, req provider.ClaimRequest) error
}

type fakeRecord struct {
	key      string
	owner    string
	specHash string
	deleting bool
	handle   provider.Handle
}

// NewFake constructs an in-memory Fake provider with initial warm capacity.
func NewFake(providerID string, warm int, elastic bool) *Fake {
	f := &Fake{
		id:      providerID,
		elastic: elastic,
		records: map[string]*fakeRecord{},
		byID:    map[string]string{},
	}
	f.warm.Store(int64(warm))
	return f
}

// Provider returns the provider.Provider bundle for f.
func (f *Fake) Provider(withBatch bool) provider.Provider {
	p := provider.Provider{
		Source: f,
		Binder: f,
		Lister: f,
	}
	if withBatch {
		p.BatchLookup = f
	}
	return p
}

// ProviderID implements provider.Source.
func (f *Fake) ProviderID() string { return f.id }

// SetWarm updates the warm capacity reported by Fetch.
func (f *Fake) SetWarm(n int) { f.warm.Store(int64(n)) }

// Fetch implements provider.Source.
func (f *Fake) Fetch(_ context.Context) (provider.Report, error) {
	f.calls.Add(1)
	return provider.Report{
		WarmCapacity: int(f.warm.Load()),
		Elastic:      f.elastic,
	}, nil
}

// InjectFault sets the fault triggered by the next Claim call.
func (f *Fake) InjectFault(fault Fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fault = fault
}

// MarkDeleting marks or unmarks a key's sandbox as being deleted.
func (f *Fake) MarkDeleting(key string, deleting bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if rec, ok := f.records[key]; ok {
		rec.deleting = deleting
	}
}

// CallCount returns the total number of provider calls recorded.
func (f *Fake) CallCount() int {
	return int(f.calls.Load())
}

// LiveCount returns the number of sandboxes currently stored on f.
func (f *Fake) LiveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

// Claim implements provider.Binder.
func (f *Fake) Claim(ctx context.Context, req provider.ClaimRequest) (provider.Handle, error) {
	f.calls.Add(1)
	if f.Hook != nil {
		if err := f.Hook(ctx, "claim", req); err != nil {
			return provider.Handle{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return provider.Handle{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	fault := f.fault
	f.fault = FaultNone
	switch fault {
	case FaultNoCapacity:
		return provider.Handle{}, provider.NoCapacity(errors.New("no capacity available"))
	case FaultBusy:
		return provider.Handle{}, provider.Busy(100*time.Millisecond, errors.New("rate limit exceeded"))
	case FaultUnavailable:
		return provider.Handle{}, provider.Unavailable(errors.New("connection refused"))
	case FaultRejected:
		return provider.Handle{}, provider.Rejected(errors.New("unknown template or invalid credentials"))
	}

	if rec, ok := f.records[req.Key]; ok {
		return f.replayLocked(rec, req)
	}

	f.nextVers++
	id := deterministicID(req.Owner, req.Key)
	h := provider.Handle{
		Provider:  f.id,
		ID:        id,
		Version:   fmt.Sprintf("v%d", f.nextVers),
		Endpoint:  "https://" + f.id + ".example.com/sandboxes/" + id,
		Connect:   map[string]string{"X-Sandbox-ID": id},
		ExpiresAt: time.Now().Add(req.TTL),
	}
	f.records[req.Key] = &fakeRecord{
		key:      req.Key,
		owner:    req.Owner,
		specHash: req.SpecHash,
		handle:   h,
	}
	f.byID[id] = req.Key

	if fault == FaultDropAfterSend {
		return provider.Handle{}, errors.New("connection reset after create")
	}
	return cloneHandle(h), nil
}

// Lookup implements provider.Binder.
func (f *Fake) Lookup(ctx context.Context, req provider.ClaimRequest) (provider.Handle, error) {
	f.calls.Add(1)
	if f.Hook != nil {
		if err := f.Hook(ctx, "lookup", req); err != nil {
			return provider.Handle{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return provider.Handle{}, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lookupLocked(req)
}

// LookupMany implements provider.BatchLookuper.
func (f *Fake) LookupMany(ctx context.Context, reqs []provider.ClaimRequest) []provider.LookupResult {
	f.calls.Add(1)
	out := make([]provider.LookupResult, len(reqs))
	if err := ctx.Err(); err != nil {
		for i := range out {
			out[i] = provider.LookupResult{Err: err}
		}
		return out
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	for i, req := range reqs {
		h, err := f.lookupLocked(req)
		out[i] = provider.LookupResult{Handle: h, Err: err}
	}
	return out
}

func (f *Fake) lookupLocked(req provider.ClaimRequest) (provider.Handle, error) {
	rec, ok := f.records[req.Key]
	if !ok {
		return provider.Handle{}, provider.ErrNotFound
	}
	return f.replayLocked(rec, req)
}

func (f *Fake) replayLocked(rec *fakeRecord, req provider.ClaimRequest) (provider.Handle, error) {
	if rec.deleting {
		return provider.Handle{}, provider.KeyBusy(50*time.Millisecond, errors.New("sandbox is being deleted"))
	}
	if rec.specHash != req.SpecHash {
		return provider.Handle{}, provider.KeyConflict(errors.New("key already exists with a different spec"))
	}
	return cloneHandle(rec.handle), nil
}

// Release implements provider.Binder.
func (f *Fake) Release(ctx context.Context, h provider.Handle) error {
	f.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	key, ok := f.byID[h.ID]
	if !ok {
		return nil
	}
	rec, ok := f.records[key]
	if !ok {
		delete(f.byID, h.ID)
		return nil
	}
	if h.Version != "" && rec.handle.Version != h.Version {
		return provider.KeyConflict(errors.New("version precondition failed"))
	}
	delete(f.records, key)
	delete(f.byID, h.ID)
	return nil
}

// ListOwned implements provider.OwnedLister.
func (f *Fake) ListOwned(ctx context.Context, owner, pageToken string, limit int) ([]provider.Handle, string, error) {
	f.calls.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for k, rec := range f.records {
		if owner == "" || rec.owner == owner {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	start := 0
	if pageToken != "" {
		idx, err := strconv.Atoi(pageToken)
		if err != nil || idx < 0 {
			return nil, "", fmt.Errorf("invalid page token %q", pageToken)
		}
		start = idx
	}
	if start >= len(keys) {
		return nil, "", nil
	}
	if limit <= 0 {
		limit = 500
	}
	end := start + limit
	next := ""
	if end < len(keys) {
		next = strconv.Itoa(end)
	} else {
		end = len(keys)
	}
	out := make([]provider.Handle, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneHandle(f.records[k].handle))
	}
	return out, next, nil
}

// SampleRequestForTest builds a valid ClaimRequest for key on f.
func (f *Fake) SampleRequestForTest(key string) provider.ClaimRequest {
	return sampleRequest(key, "python-3.12")
}

func deterministicID(owner, key string) string {
	sum := sha256.Sum256([]byte(owner + "/" + key))
	return "sc-" + hex.EncodeToString(sum[:10])
}

func cloneHandle(h provider.Handle) provider.Handle {
	cp := h
	if len(h.Connect) > 0 {
		cp.Connect = make(map[string]string, len(h.Connect))
		for k, v := range h.Connect {
			cp.Connect[k] = v
		}
	}
	return cp
}
