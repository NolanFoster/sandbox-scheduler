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
//
// One correction is layered on top of the polled reports: placements made since
// the last poll are subtracted from the capacity a snapshot shows. See
// TryReserveWarm.
package registry

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
)

// Report is what a provider says about *itself* at a point in time.
//
// Deliberately narrow: capacity, and facts that are informational only. Nothing
// a provider reports here is used for placement decisions, because a provider
// must not be able to influence its own selection. See ProviderConfig.
type Report struct {
	// WarmCapacity is the number of pre-warmed sandboxes ready to claim.
	// The one self-reported value placement does trust — a provider
	// overstating it only hurts itself, by attracting claims it cannot serve.
	WarmCapacity int

	// Attributes are facts the provider reports about itself. Surfaced in
	// status for humans; never matched on by filters. A provider able to
	// assert runtime=gvisor about itself could claim an isolation property it
	// does not have, and the filter that keeps untrusted workloads off weak
	// providers would wave them through.
	Attributes map[string]string
}

// ProviderConfig is what the operator declares about a provider.
//
// This is the authoritative half. Attributes and cost come from here — from a
// SandboxProvider spec, written by someone with permission to write it — and
// never from the provider itself. Cost especially: a provider that could report
// its own price could report zero and win every placement.
type ProviderConfig struct {
	// Attributes are matched by filters and scorers: runtime=gvisor, gpu=true.
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
	config    ProviderConfig
	report    Report
	updatedAt time.Time
	// everReported distinguishes "we have never heard from this provider" from
	// "we heard once and it went quiet". Both are unreachable, but only the
	// second has capacity worth remembering.
	everReported bool
	lastErr      error

	// ledger counts placements made against this provider's warm capacity
	// that its report may not yet reflect. See TryReserveWarm.
	ledger *ledger
}

// setReport records a report and drops the reservations it accounts for:
// those made before cutoff.
func (e *entry) setReport(rep Report, at, cutoff time.Time) {
	e.report = rep
	e.updatedAt = at
	e.everReported = true
	e.lastErr = nil
	// Capacity first: until the old reservations are cleared, the new report
	// is understated, never overstated.
	e.ledger.setWarm(rep.WarmCapacity)
	e.ledger.clearBefore(cutoff)
}

// Registry holds current capacity for every known provider.
//
// Safe for concurrent use. Snapshot is the read path and is designed to be
// called on every scheduling decision.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*entry
	sources map[string]Source

	// ledgers mirrors each entry's ledger in an immutable map, replaced
	// whenever a provider is added or removed, so that reserving finds one
	// without the lock.
	ledgers atomic.Pointer[map[string]*ledger]

	// staleAfter is how long a report stays trusted. Past it, the provider is
	// reported unreachable but keeps its last known capacity.
	staleAfter time.Duration

	// fetchTimeout bounds a single Source.Fetch.
	fetchTimeout time.Duration

	// reservationGrace is how long a claim may take to show up in a provider's
	// report. A poll keeps reservations made within this long of its start.
	reservationGrace time.Duration

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
	// ReservationGrace defaults to 5s: the time a claim may take to show up in
	// a provider's report. Too short and a poll that has not yet seen a claim
	// clears its reservation, letting a burst over-place again; too long and a
	// claim is counted twice until the next poll, which only spills early.
	ReservationGrace time.Duration
	// Now is injectable for tests.
	Now func() time.Time
}

// New builds an empty Registry.
func New(opts Options) *Registry {
	r := &Registry{
		entries:          map[string]*entry{},
		sources:          map[string]Source{},
		staleAfter:       opts.StaleAfter,
		fetchTimeout:     opts.FetchTimeout,
		reservationGrace: opts.ReservationGrace,
		now:              opts.Now,
	}
	if r.staleAfter <= 0 {
		r.staleAfter = 30 * time.Second
	}
	if r.fetchTimeout <= 0 {
		r.fetchTimeout = 5 * time.Second
	}
	if r.reservationGrace <= 0 {
		r.reservationGrace = 5 * time.Second
	}
	if r.now == nil {
		r.now = time.Now
	}
	r.ledgers.Store(&map[string]*ledger{})
	return r
}

// entry returns the entry for a provider, creating it if needed. Callers hold
// the write lock.
func (r *Registry) entry(providerID string) *entry {
	if e, ok := r.entries[providerID]; ok {
		return e
	}
	e := &entry{ledger: newLedger(r.reservationGrace, r.fetchTimeout)}
	r.entries[providerID] = e
	r.publishLedgers()
	return e
}

// publishLedgers replaces the lock-free view of ledgers after the set of
// entries changes. Callers hold the write lock.
func (r *Registry) publishLedgers() {
	m := make(map[string]*ledger, len(r.entries))
	for id, e := range r.entries {
		m[id] = e.ledger
	}
	r.ledgers.Store(&m)
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
	// Known but never heard from: appears in snapshots as unreachable with no
	// capacity, so a misconfigured provider is visible rather than absent.
	r.entry(id)
}

// SetConfig records what the operator declared about a provider.
//
// Separate from Report on purpose: this is the trusted half. Called by the
// controller from a SandboxProvider spec, and read by Snapshot for everything
// policy matches on.
func (r *Registry) SetConfig(providerID string, cfg ProviderConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entry(providerID).config = cfg
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
	r.publishLedgers()
}

// Report records capacity for a provider directly, for push-based providers
// that call the scheduler rather than being polled.
func (r *Registry) Report(providerID string, rep Report) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	r.entry(providerID).setReport(rep, now, now.Add(-r.reservationGrace))
}

// TryReserveWarm claims one of a provider's warm sandboxes for a placement,
// so the snapshots that follow show one fewer warm sandbox there. It reports
// false, reserving nothing, when the provider has no warm capacity left
// unreserved or is no longer registered.
//
// Without reservations, every placement between two polls reads the same
// capacity and reaches the same answer: a burst of a thousand requests all
// lands on the provider that looked best at the last poll, long after its warm
// pool is drained, while warm capacity elsewhere sits idle.
//
// Checking and claiming are one atomic step, which is what makes the result
// trustworthy under concurrency: two requests that both saw the last warm
// sandbox in their snapshots cannot both get it. The loser should schedule
// again against a fresh snapshot. The call takes no lock, so placements do
// not queue behind each other or behind a refresh.
//
// A reservation lasts until a poll that started more than ReservationGrace
// after it succeeds, by which point the report itself reflects the claim.
// While polls fail, reservations are kept: the claims they stand for are most
// likely real, and the stale report would otherwise overstate capacity. They
// are bounded regardless, since no more can be outstanding than the report
// showed warm.
//
// The error this can make is double counting a claim the report already
// reflects, for at most one poll. That shows a provider emptier than it is,
// which spills placements early rather than piling them onto a drained pool.
func (r *Registry) TryReserveWarm(providerID string) bool {
	l, ok := (*r.ledgers.Load())[providerID]
	if !ok {
		// Removed since the snapshot was taken; nothing left to claim.
		return false
	}
	return l.tryReserve(r.now())
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
			started := r.now()
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
			// Only claims made a grace period before the fetch began are
			// certain to be in its answer; later ones keep their reservation.
			e.setReport(rep, r.now(), started.Add(-r.reservationGrace))
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
			Provider: id,
			// Observed: the provider's own count of what is warm, less what
			// has been placed there since.
			WarmCapacity: e.ledger.available(),
			// Declared: never the provider's own claim. See ProviderConfig.
			CostPerHour: e.config.CostPerHour,
			// Fresh only if we have heard from it at all, and recently.
			Reachable: e.everReported && now.Sub(e.updatedAt) <= r.staleAfter,
		}
		if len(e.config.Attributes) > 0 {
			c.Attributes = make(map[string]string, len(e.config.Attributes))
			for k, v := range e.config.Attributes {
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
	// Reserved is how many placements since the last poll are subtracted
	// from Report.WarmCapacity in snapshots.
	Reserved  int
	Config    ProviderConfig
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
			Reserved:  e.ledger.reserved(),
			Config:    e.config,
			LastError: e.lastErr,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Provider < out[j].Provider })
	return out
}
