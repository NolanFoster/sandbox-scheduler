// Package reaper cleans up expired sandboxes on providers that do not enforce
// TTL deletion natively.
//
// Providers with native lifecycle enforcement (such as agent-sandbox via
// spec.lifecycle.shutdownTime + shutdownPolicy: Delete, or hosted APIs that
// terminate sandboxes at TTL expiry) do not need background reaping. For
// providers that implement provider.OwnedLister without native TTL cleanup,
// Reaper runs on the leader replica, pages through ListOwned(owner), and
// deletes expired handles using Handle.Version preconditions so a background
// reap never deletes a newly created replacement that reused the same ID.
package reaper

import (
	"context"
	"errors"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/NolanFoster/sandbox-scheduler/pkg/metrics"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
)

// Default settings for the background reaper.
const (
	DefaultInterval       = 30 * time.Second
	DefaultPageSize       = 500
	DefaultMaxPages       = 100
	DefaultMaxPerCycle    = 200
	DefaultRatePerSecond  = 10
	DefaultBurstPerSecond = 10
	DefaultAttemptTimeout = 5 * time.Second
)

// Metric outcome labels for sandbox_scheduler_reap_total.
const (
	OutcomeDeleted        = "deleted"
	OutcomeDryRun         = "dry_run"
	OutcomeError          = "error"
	OutcomeCircuitBreaker = "circuit_breaker"
)

// Logger is the minimal structured logging interface used by Reaper.
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Error(err error, msg string, keysAndValues ...any)
}

type noopLogger struct{}

func (noopLogger) Info(string, ...any)         {}
func (noopLogger) Error(error, string, ...any) {}

// TargetConfig configures reaping for a single provider.
type TargetConfig struct {
	Enabled bool
	Timeout time.Duration
}

type target struct {
	id      string
	prov    provider.Provider
	cfg     TargetConfig
	limiter *rate.Limiter
}

// Options configure a Reaper. Zero values select defaults.
type Options struct {
	Owner         string
	Interval      time.Duration
	PageSize      int
	MaxPages      int
	MaxPerCycle   int
	RatePerSecond float64
	Burst         int
	DryRun        bool
	Now           func() time.Time
	Log           Logger
	// OnReleased is called after a sandbox is confirmed deleted (or already
	// gone) so the binder can evict any cached completed flight for it.
	OnReleased func(providerID, sandboxID string)
	// OnDryRun is called once per provider per cycle when DryRun is true and at
	// least one expired sandbox was found, allowing the caller to emit a
	// Kubernetes Event on the SandboxProvider without per-sandbox event storms.
	OnDryRun func(ctx context.Context, providerID string, handles []provider.Handle)
}

// Reaper periodically lists and releases expired sandboxes owned by this
// scheduler installation.
type Reaper struct {
	owner       string
	interval    time.Duration
	pageSize    int
	maxPages    int
	maxPerCycle int
	ratePerSec  float64
	burst       int
	dryRun      bool
	now         func() time.Time
	log         Logger
	onReleased  func(providerID, sandboxID string)
	onDryRun    func(ctx context.Context, providerID string, handles []provider.Handle)

	targetsMu sync.Mutex
	targets   atomic.Pointer[map[string]*target]
}

// New constructs a Reaper.
func New(opts Options) *Reaper {
	r := &Reaper{
		owner:       opts.Owner,
		interval:    opts.Interval,
		pageSize:    opts.PageSize,
		maxPages:    opts.MaxPages,
		maxPerCycle: opts.MaxPerCycle,
		ratePerSec:  opts.RatePerSecond,
		burst:       opts.Burst,
		dryRun:      opts.DryRun,
		now:         opts.Now,
		log:         opts.Log,
		onReleased:  opts.OnReleased,
		onDryRun:    opts.OnDryRun,
	}
	if r.owner == "" {
		r.owner = provider.OwnerID("sandbox-scheduler", "sandbox-scheduler", "default")
	}
	if r.interval <= 0 {
		r.interval = DefaultInterval
	}
	if r.pageSize <= 0 {
		r.pageSize = DefaultPageSize
	}
	if r.maxPages <= 0 {
		r.maxPages = DefaultMaxPages
	}
	if r.maxPerCycle <= 0 {
		r.maxPerCycle = DefaultMaxPerCycle
	}
	if r.ratePerSec <= 0 {
		r.ratePerSec = DefaultRatePerSecond
	}
	if r.burst <= 0 {
		r.burst = int(math.Ceil(r.ratePerSec))
		if r.burst < 1 {
			r.burst = 1
		}
	}
	if r.now == nil {
		r.now = time.Now
	}
	if r.log == nil {
		r.log = noopLogger{}
	}
	empty := map[string]*target{}
	r.targets.Store(&empty)
	return r
}

// SetTarget registers or updates a provider in the reaper. Providers without
// both Binder and OwnedLister (or with cfg.Enabled == false) are removed.
func (r *Reaper) SetTarget(providerID string, prov provider.Provider, cfg TargetConfig) {
	if !cfg.Enabled || prov.Binder == nil || prov.Lister == nil {
		r.RemoveTarget(providerID)
		return
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultAttemptTimeout
	}

	r.targetsMu.Lock()
	defer r.targetsMu.Unlock()

	cur := *r.targets.Load()
	prev := cur[providerID]

	var lim *rate.Limiter
	if prev != nil && prev.limiter != nil {
		lim = prev.limiter
	} else {
		lim = rate.NewLimiter(rate.Limit(r.ratePerSec), r.burst)
	}

	next := make(map[string]*target, len(cur)+1)
	for k, v := range cur {
		next[k] = v
	}
	next[providerID] = &target{
		id:      providerID,
		prov:    prov,
		cfg:     cfg,
		limiter: lim,
	}
	r.targets.Store(&next)
}

// RemoveTarget removes a provider from the reaper.
func (r *Reaper) RemoveTarget(providerID string) {
	r.targetsMu.Lock()
	defer r.targetsMu.Unlock()

	cur := *r.targets.Load()
	if _, ok := cur[providerID]; !ok {
		return
	}
	next := make(map[string]*target, len(cur)-1)
	for k, v := range cur {
		if k != providerID {
			next[k] = v
		}
	}
	r.targets.Store(&next)
}

// Run executes the periodic reap loop until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.RunOnce(ctx)
		}
	}
}

// RunOnce executes a single reap pass across all registered providers that
// implement OwnedLister. Each provider pass runs concurrently under a cycle
// deadline of r.interval so one slow or unhealthy provider never stalls
// reaping on other providers.
func (r *Reaper) RunOnce(ctx context.Context) {
	snapshot := *r.targets.Load()
	if len(snapshot) == 0 || ctx.Err() != nil {
		return
	}

	passCtx, passCancel := context.WithTimeout(ctx, r.interval)
	defer passCancel()

	ids := make([]string, 0, len(snapshot))
	for id := range snapshot {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var wg sync.WaitGroup
	for _, id := range ids {
		t := snapshot[id]
		wg.Add(1)
		go func(t *target) {
			defer wg.Done()
			r.reapProvider(passCtx, t)
		}(t)
	}
	wg.Wait()
}

func (r *Reaper) reapProvider(ctx context.Context, t *target) {
	if t == nil || !t.cfg.Enabled || t.prov.Binder == nil || t.prov.Lister == nil {
		return
	}

	var (
		pageToken         string
		expired           []provider.Handle
		hitCircuitBreaker bool
	)

	// Collect up to maxPerCycle+1 expired handles before issuing Release calls
	// so providers with offset-based pagination never shift subsequent pages
	// while a reap pass is paging through ListOwned.
	for page := 0; page < r.maxPages; page++ {
		if ctx.Err() != nil {
			return
		}

		listCtx, listCancel := context.WithTimeout(ctx, t.cfg.Timeout)
		handles, nextToken, err := t.prov.Lister.ListOwned(listCtx, r.owner, pageToken, r.pageSize)
		listCancel()
		if err != nil {
			metrics.RecordReap(t.id, OutcomeError)
			r.log.Error(err, "listing owned sandboxes for reap", "provider", t.id)
			return
		}

		now := r.now()
		for _, h := range handles {
			if h.ExpiresAt.IsZero() || !now.After(h.ExpiresAt) {
				continue
			}
			if len(expired) >= r.maxPerCycle {
				hitCircuitBreaker = true
				break
			}
			expired = append(expired, h)
		}

		if hitCircuitBreaker || nextToken == "" || nextToken == pageToken {
			break
		}
		pageToken = nextToken
	}

	var dryRunHandles []provider.Handle
	for _, h := range expired {
		if ctx.Err() != nil {
			break
		}
		if !r.waitToken(ctx, t.limiter) {
			break
		}

		if r.dryRun {
			metrics.RecordReap(t.id, OutcomeDryRun)
			r.log.Info("dry-run: would reap expired sandbox",
				"provider", t.id, "sandboxID", h.ID, "version", h.Version, "expiredAt", h.ExpiresAt)
			dryRunHandles = append(dryRunHandles, h)
			continue
		}

		relCtx, relCancel := context.WithTimeout(ctx, t.cfg.Timeout)
		relErr := t.prov.Binder.Release(relCtx, provider.Handle{
			Provider: t.id,
			ID:       h.ID,
			Version:  h.Version,
		})
		relCancel()

		if relErr != nil && !errors.Is(relErr, provider.ErrNotFound) {
			metrics.RecordReap(t.id, OutcomeError)
			r.log.Error(relErr, "releasing expired sandbox",
				"provider", t.id, "sandboxID", h.ID, "version", h.Version)
			continue
		}

		if relErr == nil {
			metrics.RecordReap(t.id, OutcomeDeleted)
		}
		if r.onReleased != nil {
			r.onReleased(t.id, h.ID)
		}
	}

	if hitCircuitBreaker && ctx.Err() == nil {
		metrics.RecordReap(t.id, OutcomeCircuitBreaker)
		r.log.Info("reaper circuit breaker tripped; deferring remaining expired sandboxes to next cycle",
			"provider", t.id, "maxPerCycle", r.maxPerCycle)
	}
	if len(dryRunHandles) > 0 && r.onDryRun != nil {
		r.onDryRun(ctx, t.id, dryRunHandles)
	}
}

func (r *Reaper) waitToken(ctx context.Context, lim *rate.Limiter) bool {
	now := r.now()
	res := lim.ReserveN(now, 1)
	if !res.OK() {
		return false
	}
	delay := res.DelayFrom(now)
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		res.CancelAt(r.now())
		return false
	}
}
