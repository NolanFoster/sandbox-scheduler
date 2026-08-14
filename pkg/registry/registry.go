// Package registry keeps the capacity view that scheduling reads.
//
// This is the half of the design that makes the other half possible. The
// scheduling pipeline (pkg/framework) is a pure function over a snapshot; it
// performs no I/O and takes no locks on anything that could block. That is only
// achievable if something else keeps the capacity picture current, off the
// decision path. This is that something.
//
// The shape mirrors the Kubernetes scheduler's informer cache: refresh happens
// on its own schedule, decisions read local memory, and a decision never waits
// on a provider. Sandbox start is a 90–150ms game — a round trip per provider
// at decision time would dominate it, and a slow provider would make every
// placement slow, including placements that were never going to choose it.
//
// Two rules govern everything here:
//
//  1. **A failed refresh never erases what we knew.** It marks it stale. A
//     transient blip must not read as "this provider has no capacity", because
//     that silently redirects an entire fleet.
//  2. **A stale provider is never removed from the snapshot.** It is reported
//     with Reachable=false and its last known capacity, leaving the decision to
//     policy. Dropping it would make "why wasn't civo considered?"
//     unanswerable, which is the failure mode operators hate most.
package registry

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
)

// Report is what a provider says about itself at a point in time.
type Report struct {
	// WarmCapacity is the number of pre-warmed sandboxes ready to claim.
	WarmCapacity int

	// Attributes are provider facts filters and scorers match on:
	// runtime=gvisor, gpu=true, region=nyc1.
	Attributes map[string]string

	// CostPerHour is the relative price of a sandbox-hour. Units are the
	// operator's own; only the ordering is used.
	CostPerHour float64
}

// Source fetches a Report for one provider.
//
// Implementations are adapters: a Kubernetes cluster running agent-sandbox, a
// hosted sandbox API, anything that can answer "how much warm capacity do you
// have". They are called off the decision path, so a slow implementation costs
// freshness, never placement latency.
type Source interface {
	// ProviderID is the stable identifier recorded on placed sandboxes.
	ProviderID() string
	// Fetch returns current capacity. It must respect ctx cancellation; the
	// registry imposes its own timeout regardless.
	Fetch(ctx context.Context) (Report, error)
}

// entry is the registry's per-provider state.
type entry struct {
	report    Report
	updatedAt time.Time
	// everReported distinguishes "we have never heard from this provider" from
	// "we heard once and it went quiet". Both are unreachable, but only the
	// second has capacity worth remembering.
	everReported bool
	lastErr      error
}

// Registry holds current capacity for every known provider.
//
// Safe for concurrent use. Snapshot is the read path and is designed to be
// called on every scheduling decision.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*entry
	sources map[string]Source

	// staleAfter is how long a report stays trusted. Past it, the provider is
	// reported unreachable but keeps its last known capacity.
	staleAfter time.Duration

	// fetchTimeout bounds a single Source.Fetch.
	fetchTimeout time.Duration

	now func() time.Time
}

// Options configure a Registry. The zero value of each field selects a default.
type Options struct {
	// StaleAfter defaults to 30s: long enough to ride out a missed refresh,
	// short enough that a dead provider stops attracting traffic quickly.
	StaleAfter time.Duration
	// FetchTimeout defaults to 5s. Generous, because it is off the hot path —
	// the cost of a slow provider is stale data, not a slow placement.
	FetchTimeout time.Duration
	// Now is injectable for tests.
	Now func() time.Time
}

// New builds an empty Registry.
func New(opts Options) *Registry {
	r := &Registry{
		entries:      map[string]*entry{},
		sources:      map[string]Source{},
		staleAfter:   opts.StaleAfter,
		fetchTimeout: opts.FetchTimeout,
		now:          opts.Now,
	}
	if r.staleAfter <= 0 {
		r.staleAfter = 30 * time.Second
	}
	if r.fetchTimeout <= 0 {
		r.fetchTimeout = 5 * time.Second
	}
	if r.now == nil {
		r.now = time.Now
	}
	return r
}

// AddSource registers a provider to be polled. Adding the same ProviderID twice
// replaces the source and keeps any capacity already known for it, so
// re-registering a provider (a CRD update, say) does not blind the scheduler to
// it in the interim.
func (r *Registry) AddSource(s Source) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := s.ProviderID()
	r.sources[id] = s
	if _, ok := r.entries[id]; !ok {
		// Known but never heard from: appears in snapshots as unreachable with
		// no capacity, so a misconfigured provider is visible rather than
		// absent.
		r.entries[id] = &entry{}
	}
}

// RemoveSource stops polling a provider and drops its capacity.
//
// This is deliberately the *only* way a provider leaves the snapshot. Going
// quiet is not enough — that is staleness, and staleness is policy's problem.
func (r *Registry) RemoveSource(providerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sources, providerID)
	delete(r.entries, providerID)
}

// Report records capacity for a provider directly, for push-based providers
// that call the scheduler rather than being polled.
func (r *Registry) Report(providerID string, rep Report) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[providerID]
	if !ok {
		e = &entry{}
		r.entries[providerID] = e
	}
	e.report = rep
	e.updatedAt = r.now()
	e.everReported = true
	e.lastErr = nil
}

// Refresh polls every registered source once, concurrently.
//
// Sources are polled in parallel and independently: one slow or broken provider
// must not delay or invalidate the refresh of the others. A source that fails
// leaves the previous report in place, marked by its age rather than erased.
func (r *Registry) Refresh(ctx context.Context) {
	r.mu.RLock()
	sources := make([]Source, 0, len(r.sources))
	for _, s := range r.sources {
		sources = append(sources, s)
	}
	r.mu.RUnlock()

	var wg sync.WaitGroup
	for _, s := range sources {
		wg.Add(1)
		go func(s Source) {
			defer wg.Done()
			fetchCtx, cancel := context.WithTimeout(ctx, r.fetchTimeout)
			defer cancel()
			rep, err := s.Fetch(fetchCtx)

			r.mu.Lock()
			defer r.mu.Unlock()
			e, ok := r.entries[s.ProviderID()]
			if !ok {
				// Removed while the fetch was in flight; drop the result rather
				// than resurrecting a provider the operator deleted.
				return
			}
			if err != nil {
				// Keep the last good report. Ageing it out is staleness, which
				// policy can reason about; zeroing it is a lie.
				e.lastErr = err
				return
			}
			e.report = rep
			e.updatedAt = r.now()
			e.everReported = true
			e.lastErr = nil
		}(s)
	}
	wg.Wait()
}

// Run refreshes on an interval until ctx is cancelled. It refreshes once
// immediately so the scheduler is not blind for a whole interval at startup.
func (r *Registry) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	r.Refresh(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Refresh(ctx)
		}
	}
}

// Snapshot returns the current candidates for scheduling.
//
// The read path. Takes a read lock, copies, and returns — no I/O, no waiting,
// no calls into providers. Callers may mutate the result freely; the attribute
// maps are copied too, so a scorer cannot corrupt registry state.
//
// Ordering is stable (by provider id) so scheduling is reproducible and log
// diffs are readable.
func (r *Registry) Snapshot() []framework.Candidate {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := r.now()
	out := make([]framework.Candidate, 0, len(r.entries))
	for id, e := range r.entries {
		c := framework.Candidate{
			Provider:     id,
			WarmCapacity: e.report.WarmCapacity,
			CostPerHour:  e.report.CostPerHour,
			// Fresh only if we have heard from it at all, and recently.
			Reachable: e.everReported && now.Sub(e.updatedAt) <= r.staleAfter,
		}
		if len(e.report.Attributes) > 0 {
			c.Attributes = make(map[string]string, len(e.report.Attributes))
			for k, v := range e.report.Attributes {
				c.Attributes[k] = v
			}
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Provider < out[j].Provider })
	return out
}

// Status describes what the registry knows about one provider, for operators
// and for the CRD status this will eventually write.
type Status struct {
	Provider  string
	Reachable bool
	Stale     bool
	Age       time.Duration
	Report    Report
	LastError error
}

// Statuses returns the registry's view of every provider, ordered by id.
func (r *Registry) Statuses() []Status {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := r.now()
	out := make([]Status, 0, len(r.entries))
	for id, e := range r.entries {
		age := time.Duration(0)
		if e.everReported {
			age = now.Sub(e.updatedAt)
		}
		out = append(out, Status{
			Provider:  id,
			Reachable: e.everReported && age <= r.staleAfter,
			Stale:     e.everReported && age > r.staleAfter,
			Age:       age,
			Report:    e.report,
			LastError: e.lastErr,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Provider < out[j].Provider })
	return out
}
