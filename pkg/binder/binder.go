// Package binder executes sandbox claims against placed providers.
//
// Scheduling (pkg/framework) is a pure function over a capacity snapshot.
// Binding is the optional second step: taking that decision, calling the
// winning provider's Binder.Claim, and spilling to the next-best candidate on
// a confirmed refusal — while guaranteeing that one idempotency key never
// produces two sandboxes across concurrent requests, retries, or leader
// failover.
package binder

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/NolanFoster/sandbox-scheduler/api/v1alpha1"
	"github.com/NolanFoster/sandbox-scheduler/pkg/framework"
	"github.com/NolanFoster/sandbox-scheduler/pkg/metrics"
	"github.com/NolanFoster/sandbox-scheduler/pkg/plugins"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
	"github.com/NolanFoster/sandbox-scheduler/pkg/registry"
)

// Default settings for the binder loop.
const (
	DefaultClaimTimeout        = 15 * time.Second
	DefaultAttemptTimeout      = 3 * time.Second
	DefaultProviderMaxInFlight = 512
	DefaultGlobalMaxInFlight   = 4096
	DefaultMaxJoinersPerKey    = 8
	DefaultMaxSpill            = 3
	DefaultRefusalTTL          = time.Second
	DefaultQueueTimeout        = time.Second
	DefaultMaxTTL              = 24 * time.Hour

	DefaultClusterMaxRate    = 50
	DefaultElasticMaxRate    = 100
	DefaultClusterLookupRate = 200
	DefaultElasticLookupRate = 20

	// RemoteTimeoutSkewGrace is added to the largest provider attempt timeout
	// during post-election lease handover hold-off so any server-side create
	// sent by the deposed leader has finished or timed out on the remote API
	// before the new leader accepts claims or Lookups.
	RemoteTimeoutSkewGrace = 500 * time.Millisecond
)

// Machine-readable error reasons returned in HTTP error bodies.
const (
	ReasonInvalidRequest     = "InvalidRequest"
	ReasonInvalidPolicy      = "InvalidPolicy"
	ReasonNoProviders        = "NoProviders"
	ReasonNoCapacity         = "NoCapacity"
	ReasonSpecMismatch       = "SpecMismatch"
	ReasonConstraintExcluded = "ConstraintExcluded"
	ReasonRejected           = "Rejected"
	ReasonBusy               = "Busy"
	ReasonKeyBusy            = "KeyBusy"
	ReasonRateLimited        = "RateLimited"
	ReasonSlotsFull          = "SlotsFull"
	ReasonTooManyJoiners     = "TooManyJoiners"
	ReasonUnavailable        = "Unavailable"
	ReasonStandby            = "Standby"
	ReasonHoldOff            = "HoldOff"
	ReasonUnknownOutcome     = "UnknownOutcome"
	ReasonClaimsDisabled     = "ClaimsDisabled"
)

// TargetConfig describes the operator-configured claim parameters for one
// provider.
type TargetConfig struct {
	Enabled          bool
	MaxInFlight      int
	Timeout          time.Duration
	MaxRate          int
	LookupRate       int
	Elastic          bool
	ElasticWarmScore *int
	Templates        map[string]string
}

// target is the binder's runtime state for one provider, published
// copy-on-write via an atomic.Pointer map so the request path takes no lock.
type target struct {
	id          string
	prov        provider.Provider
	cfg         TargetConfig
	slots       *slotLimiter
	claimRate   *rate.Limiter
	lookupRate  *rate.Limiter
	retryBudget *rate.Limiter
	coalescer   *lookupCoalescer
	health      *healthTracker
}

type refusalKey struct {
	provider string
	template string
}

// Request is a provider-neutral sandbox claim request.
type Request struct {
	Template       string
	Key            string
	Retry          bool
	Labels         map[string]string
	Requires       map[string]string
	PreferProvider string
	PinProvider    string
	WarmOnly       bool
	TTL            time.Duration
	Timeout        time.Duration
}

// Result is a completed sandbox claim or replay.
type Result struct {
	Handle      provider.Handle
	Replay      bool
	Warm        bool
	Policy      string
	Score       int64
	Explanation string
}

// Error is a structured failure from Bind or Release.
type Error struct {
	Status      int
	Reason      string
	Message     string
	Providers   []string
	RetryAfter  time.Duration
	Explanation string
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	return e.Message
}

// Options configure a Binder. Zero values select defaults.
type Options struct {
	Registry           *registry.Registry
	Owner              string
	AllowedLabelDomain string
	ClaimTimeout       time.Duration
	QueueTimeout       time.Duration
	RefusalTTL         time.Duration
	MaxTTL             time.Duration
	MaxSpill           int
	GlobalMaxInFlight  int
	MaxJoinersPerKey   int
	CoalesceWindow     time.Duration
	Now                func() time.Time
	RandFloat64        func() float64
}

// flight tracks an in-flight or recently completed claim for a single
// idempotency key so concurrent and overlapping requests for the same key
// share one execution on the leader.
type flight struct {
	specHash       string
	joiners        atomic.Int32
	done           chan struct{}
	res            *Result
	err            *Error
	completed      bool
	completedUntil time.Time
}

// Binder coordinates sandbox claims across providers.
type Binder struct {
	reg                *registry.Registry
	owner              string
	allowedLabelDomain string
	claimTimeout       time.Duration
	queueTimeout       time.Duration
	refusalTTL         time.Duration
	maxTTL             time.Duration
	maxSpill           int
	globalMaxInFlight  int
	maxJoinersPerKey   int
	coalesceWindow     time.Duration
	now                func() time.Time
	randFloat          func() float64

	// targets is a copy-on-write map replaced under targetsMu on provider
	// registration changes; read lock-free on the claim path.
	targetsMu sync.Mutex
	targets   atomic.Pointer[map[string]*target]

	// Global in-flight request counter (active flights + joiners/waiters).
	inFlight atomic.Int64

	// Singleflight per idempotency key.
	flightsMu sync.Mutex
	flights   map[string]*flight

	// Global refusal cache keyed by (provider, template) and per-provider
	// Busy cooldowns.
	stateMu   sync.RWMutex
	refusals  map[refusalKey]time.Time
	cooldowns map[string]time.Time

	// Post-election lease-handover hold-off timestamp.
	electedMu sync.RWMutex
	electedAt time.Time

	// Detached-attempt shutdown synchronization: closing and activeAttempts
	// share one mutex so no attempt starts after Shutdown begins and none
	// races closing the done channel.
	shutdownMu     sync.Mutex
	closing        bool
	activeAttempts int
	shutdownDone   chan struct{}
}

// New constructs a Binder.
func New(opts Options) *Binder {
	b := &Binder{
		reg:                opts.Registry,
		owner:              opts.Owner,
		allowedLabelDomain: opts.AllowedLabelDomain,
		claimTimeout:       opts.ClaimTimeout,
		queueTimeout:       opts.QueueTimeout,
		refusalTTL:         opts.RefusalTTL,
		maxTTL:             opts.MaxTTL,
		maxSpill:           opts.MaxSpill,
		globalMaxInFlight:  opts.GlobalMaxInFlight,
		maxJoinersPerKey:   opts.MaxJoinersPerKey,
		coalesceWindow:     opts.CoalesceWindow,
		now:                opts.Now,
		randFloat:          opts.RandFloat64,
		flights:            map[string]*flight{},
		refusals:           map[refusalKey]time.Time{},
		cooldowns:          map[string]time.Time{},
		shutdownDone:       make(chan struct{}),
	}
	if b.owner == "" {
		b.owner = provider.OwnerID("sandbox-scheduler", "sandbox-scheduler", "default")
	}
	if b.allowedLabelDomain == "" {
		b.allowedLabelDomain = provider.DefaultAllowedLabelDomain
	}
	if b.claimTimeout <= 0 {
		b.claimTimeout = DefaultClaimTimeout
	}
	if b.queueTimeout <= 0 {
		b.queueTimeout = DefaultQueueTimeout
	}
	if b.refusalTTL <= 0 {
		b.refusalTTL = DefaultRefusalTTL
	}
	if b.maxTTL <= 0 {
		b.maxTTL = DefaultMaxTTL
	}
	if b.maxSpill <= 0 {
		b.maxSpill = DefaultMaxSpill
	}
	if b.globalMaxInFlight <= 0 {
		b.globalMaxInFlight = DefaultGlobalMaxInFlight
	}
	if b.maxJoinersPerKey <= 0 {
		b.maxJoinersPerKey = DefaultMaxJoinersPerKey
	}
	if b.now == nil {
		b.now = time.Now
	}
	if b.randFloat == nil {
		b.randFloat = rand.Float64
	}
	empty := map[string]*target{}
	b.targets.Store(&empty)
	return b
}

// SetTarget registers or updates a provider's claim capabilities and limits.
// When maxInFlight changes, the provider's slotLimiter is replaced rather than
// resized so in-flight attempts drain from the limiter they acquired from.
func (b *Binder) SetTarget(providerID string, prov provider.Provider, cfg TargetConfig) {
	b.targetsMu.Lock()
	defer b.targetsMu.Unlock()

	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = DefaultProviderMaxInFlight
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultAttemptTimeout
	}
	if cfg.MaxRate <= 0 {
		if cfg.Elastic {
			cfg.MaxRate = DefaultElasticMaxRate
		} else {
			cfg.MaxRate = DefaultClusterMaxRate
		}
	}
	if cfg.LookupRate <= 0 {
		if cfg.Elastic {
			cfg.LookupRate = DefaultElasticLookupRate
		} else {
			cfg.LookupRate = DefaultClusterLookupRate
		}
	}

	cur := *b.targets.Load()
	prev := cur[providerID]

	var slots *slotLimiter
	if prev != nil && prev.cfg.MaxInFlight == cfg.MaxInFlight {
		slots = prev.slots
	} else {
		slots = newSlotLimiter(providerID, cfg.MaxInFlight)
	}

	var health *healthTracker
	if prev != nil {
		health = prev.health
	} else {
		health = newHealthTracker()
	}

	var claimLim, retryLim *rate.Limiter
	if prev != nil && prev.cfg.MaxRate == cfg.MaxRate && prev.claimRate != nil && prev.retryBudget != nil {
		claimLim = prev.claimRate
		retryLim = prev.retryBudget
	} else {
		claimLim = rate.NewLimiter(rate.Limit(cfg.MaxRate), cfg.MaxRate)
		retryRate := cfg.MaxRate / 10
		if retryRate < 1 {
			retryRate = 1
		}
		retryBurst := retryRate
		if retryBurst < 2 {
			retryBurst = 2
		}
		retryLim = rate.NewLimiter(rate.Limit(retryRate), retryBurst)
	}

	var lookupLim *rate.Limiter
	if prev != nil && prev.cfg.LookupRate == cfg.LookupRate && prev.lookupRate != nil {
		lookupLim = prev.lookupRate
	} else {
		lookupLim = rate.NewLimiter(rate.Limit(cfg.LookupRate), cfg.LookupRate)
	}

	var coalescer *lookupCoalescer
	if prov.BatchLookup != nil {
		coalescer = newLookupCoalescer(prov.BatchLookup, lookupLim, slots, b.startAttempt, b.finishAttempt, b.coalesceWindow, b.now)
	}

	templatesCopy := make(map[string]string, len(cfg.Templates))
	for k, v := range cfg.Templates {
		templatesCopy[k] = v
	}
	cfg.Templates = templatesCopy

	next := make(map[string]*target, len(cur)+1)
	for k, v := range cur {
		next[k] = v
	}
	next[providerID] = &target{
		id:          providerID,
		prov:        prov,
		cfg:         cfg,
		slots:       slots,
		claimRate:   claimLim,
		lookupRate:  lookupLim,
		retryBudget: retryLim,
		coalescer:   coalescer,
		health:      health,
	}
	b.targets.Store(&next)
}

// RemoveTarget removes a provider from the binder.
func (b *Binder) RemoveTarget(providerID string) {
	b.targetsMu.Lock()
	defer b.targetsMu.Unlock()

	cur := *b.targets.Load()
	if _, ok := cur[providerID]; !ok {
		return
	}
	next := make(map[string]*target, len(cur)-1)
	for k, v := range cur {
		if k != providerID {
			next[k] = v
		}
	}
	b.targets.Store(&next)
	metrics.ClaimInflight.DeleteLabelValues(providerID)
	b.evictCompletedFlight(providerID, "")
}

// HealthStatus returns the current claim-path health of providerID.
func (b *Binder) HealthStatus(providerID string) (ClaimHealthStatus, bool) {
	t, ok := (*b.targets.Load())[providerID]
	if !ok {
		return ClaimHealthStatus{}, false
	}
	return t.health.status(), true
}

// NotifyElected records that this replica just acquired the leader lease,
// starting the post-election hold-off window.
func (b *Binder) NotifyElected() {
	b.electedMu.Lock()
	b.electedAt = b.now()
	b.electedMu.Unlock()
}

// holdOffRemaining returns how much post-election hold-off time remains at
// now. The hold-off is sized to the largest per-attempt timeout across
// claim-capable providers plus RemoteTimeoutSkewGrace, capped by claimTimeout.
func (b *Binder) holdOffRemaining(now time.Time) time.Duration {
	b.electedMu.RLock()
	electedAt := b.electedAt
	b.electedMu.RUnlock()
	if electedAt.IsZero() {
		return 0
	}

	maxTimeout := DefaultAttemptTimeout
	for _, t := range *b.targets.Load() {
		if t.cfg.Enabled && t.prov.Binder != nil && t.cfg.Timeout > maxTimeout {
			maxTimeout = t.cfg.Timeout
		}
	}
	if maxTimeout > b.claimTimeout {
		maxTimeout = b.claimTimeout
	}
	holdOff := maxTimeout + RemoteTimeoutSkewGrace
	until := electedAt.Add(holdOff)
	if !now.Before(until) {
		return 0
	}
	return until.Sub(now)
}

// Shutdown stops new claim attempts from starting and waits until all detached
// in-flight attempts finish or ctx expires.
func (b *Binder) Shutdown(ctx context.Context) error {
	b.shutdownMu.Lock()
	if !b.closing {
		b.closing = true
		if b.activeAttempts == 0 {
			close(b.shutdownDone)
		}
	}
	done := b.shutdownDone
	b.shutdownMu.Unlock()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Binder) startAttempt() bool {
	b.shutdownMu.Lock()
	defer b.shutdownMu.Unlock()
	if b.closing {
		return false
	}
	b.activeAttempts++
	return true
}

func (b *Binder) finishAttempt() {
	b.shutdownMu.Lock()
	defer b.shutdownMu.Unlock()
	b.activeAttempts--
	if b.closing && b.activeAttempts == 0 {
		close(b.shutdownDone)
	}
}

// Bind places and claims a sandbox, spilling across candidates on confirmed
// refusals while preserving single-sandbox-per-key idempotency.
func (b *Binder) Bind(ctx context.Context, policy *v1alpha1.SandboxPlacementPolicy, req Request) (retRes *Result, retErr *Error) {
	// Global cap covers active flights and waiters alike so a burst parks at
	// most globalMaxInFlight goroutines.
	cur := b.inFlight.Add(1)
	defer b.inFlight.Add(-1)
	if int(cur) > b.globalMaxInFlight {
		return nil, &Error{
			Status:     429,
			Reason:     ReasonSlotsFull,
			Message:    fmt.Sprintf("global in-flight claim limit (%d) exceeded", b.globalMaxInFlight),
			Providers:  []string{},
			RetryAfter: b.jitterDelay(time.Second),
		}
	}

	now := b.now()
	if rem := b.holdOffRemaining(now); rem > 0 {
		return nil, &Error{
			Status:     503,
			Reason:     ReasonHoldOff,
			Message:    "newly elected leader is holding off claims while pre-failover attempts settle",
			Providers:  []string{},
			RetryAfter: rem + time.Duration(b.randFloat()*float64(3*time.Second)),
		}
	}

	ttl := req.TTL
	if ttl <= 0 || ttl > b.maxTTL {
		ttl = b.maxTTL
	}
	claimReq := provider.ClaimRequest{
		Template: req.Template,
		Key:      req.Key,
		SpecHash: provider.SpecHashOf(req.Template, req.Labels, ttl),
		Owner:    b.owner,
		Labels:   req.Labels,
		TTL:      ttl,
	}
	if err := claimReq.ValidateWithDomain(b.maxTTL, b.allowedLabelDomain); err != nil {
		return nil, &Error{
			Status:    400,
			Reason:    ReasonInvalidRequest,
			Message:   err.Error(),
			Providers: []string{},
		}
	}

	// De-duplicate concurrent and repeat requests for the same key across the
	// sandbox's TTL window on the leader.
	b.flightsMu.Lock()
	now = b.now()
	if existing, ok := b.flights[req.Key]; ok {
		if existing.completed && !now.Before(existing.completedUntil) {
			delete(b.flights, req.Key)
		} else {
			b.flightsMu.Unlock()
			return b.joinFlight(ctx, existing, policy, req, claimReq)
		}
	}
	if len(b.flights) >= 64 {
		for k, f := range b.flights {
			if f.completed && !now.Before(f.completedUntil) {
				delete(b.flights, k)
			}
		}
	}
	fl := &flight{
		specHash: claimReq.SpecHash,
		done:     make(chan struct{}),
	}
	b.flights[req.Key] = fl
	b.flightsMu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			fl.res = nil
			fl.err = &Error{
				Status:    504,
				Reason:    ReasonUnknownOutcome,
				Message:   fmt.Sprintf("panic in claim flight: %v", r),
				Providers: []string{},
			}
			retRes = nil
			retErr = fl.err
		}
		b.flightsMu.Lock()
		if fl.err == nil && fl.res != nil {
			finishNow := b.now()
			until := finishNow.Add(ttl)
			if !fl.res.Handle.ExpiresAt.IsZero() && fl.res.Handle.ExpiresAt.After(finishNow) && fl.res.Handle.ExpiresAt.Before(until) {
				until = fl.res.Handle.ExpiresAt
			}
			fl.completed = true
			fl.completedUntil = until
		} else if b.flights[req.Key] == fl {
			delete(b.flights, req.Key)
		}
		close(fl.done)
		b.flightsMu.Unlock()
	}()

	fl.res, fl.err = b.executeFlight(ctx, policy, req, claimReq)
	return fl.res, fl.err
}

func (b *Binder) joinFlight(
	ctx context.Context,
	fl *flight,
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
	claimReq provider.ClaimRequest,
) (*Result, *Error) {
	if fl.specHash != claimReq.SpecHash {
		providers := []string{}
		select {
		case <-fl.done:
			if fl.res != nil && fl.res.Handle.Provider != "" {
				providers = []string{fl.res.Handle.Provider}
			}
		default:
		}
		return nil, &Error{
			Status:    409,
			Reason:    ReasonSpecMismatch,
			Message:   fmt.Sprintf("key %q is already being claimed with a different spec", req.Key),
			Providers: providers,
		}
	}
	if int(fl.joiners.Add(1)) > b.maxJoinersPerKey {
		fl.joiners.Add(-1)
		return nil, &Error{
			Status:     429,
			Reason:     ReasonTooManyJoiners,
			Message:    fmt.Sprintf("key %q has reached the maximum concurrent joiners (%d)", req.Key, b.maxJoinersPerKey),
			Providers:  []string{},
			RetryAfter: b.jitterDelay(time.Second),
		}
	}
	defer fl.joiners.Add(-1)

	select {
	case <-fl.done:
	default:
		select {
		case <-fl.done:
		case <-ctx.Done():
			return nil, &Error{
				Status:    504,
				Reason:    ReasonUnknownOutcome,
				Message:   "client context cancelled while waiting on in-flight claim for the same key",
				Providers: []string{},
			}
		}
	}

	if fl.err != nil {
		cp := *fl.err
		cp.Providers = append([]string(nil), fl.err.Providers...)
		return nil, &cp
	}

	// Verify the shared result satisfies this joiner's own constraints.
	if !b.passesJoinerConstraints(fl.res.Handle.Provider, fl.res.Warm, policy, req) {
		return nil, &Error{
			Status:    409,
			Reason:    ReasonConstraintExcluded,
			Message:   fmt.Sprintf("key %q was claimed on provider %q, which does not satisfy this request's constraints", req.Key, fl.res.Handle.Provider),
			Providers: []string{fl.res.Handle.Provider},
		}
	}

	joined := *fl.res
	joined.Replay = true
	return &joined, nil
}

func (b *Binder) executeFlight(
	reqCtx context.Context,
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
	claimReq provider.ClaimRequest,
) (*Result, *Error) {
	now := b.now()
	timeout := b.claimTimeout
	if req.Timeout > 0 && req.Timeout < timeout {
		timeout = req.Timeout
	}
	reqDeadline := now.Add(timeout)

	if policy == nil {
		policy = v1alpha1.DefaultPolicy()
	}

	targets := *b.targets.Load()
	pinned := req.PinProvider

	// 1. Lookup rule for pinned or retry requests.
	if pinned != "" {
		res, berr, shouldClaim := b.lookupPinned(reqCtx, targets, pinned, policy, req, claimReq, reqDeadline)
		if !shouldClaim {
			return res, berr
		}
	} else if req.Retry {
		res, berr, pinFromProbe, shouldClaim := b.lookupRetryAll(reqCtx, targets, policy, req, claimReq, reqDeadline)
		if !shouldClaim {
			return res, berr
		}
		if pinFromProbe != "" {
			pinned = pinFromProbe
		}
	}

	// 2. Claim loop.
	return b.claimLoop(reqCtx, targets, pinned, policy, req, claimReq, reqDeadline)
}

func (b *Binder) lookupPinned(
	reqCtx context.Context,
	targets map[string]*target,
	pinProvider string,
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
	claimReq provider.ClaimRequest,
	reqDeadline time.Time,
) (*Result, *Error, bool) {
	t, ok := targets[pinProvider]
	if !ok || !t.cfg.Enabled || t.prov.Binder == nil {
		return nil, &Error{
			Status:    409,
			Reason:    ReasonNoCapacity,
			Message:   fmt.Sprintf("pinned provider %q is not registered or not claim-capable", pinProvider),
			Providers: []string{pinProvider},
		}, false
	}

	wait, ok := b.reserveLookupTokens(reqDeadline, []*target{t})
	if !ok {
		return nil, &Error{
			Status:     429,
			Reason:     ReasonRateLimited,
			Message:    fmt.Sprintf("lookup rate limit exceeded on provider %q", pinProvider),
			Providers:  []string{pinProvider},
			RetryAfter: b.jitterDelay(wait),
		}, false
	}
	if wait > 0 {
		if !b.sleepUntil(reqCtx, b.now().Add(wait)) {
			return nil, &Error{
				Status:    504,
				Reason:    ReasonUnknownOutcome,
				Message:   "request cancelled while waiting for lookup token",
				Providers: []string{pinProvider},
			}, false
		}
	}

	h, err := b.callLookup(reqCtx, t, claimReq, reqDeadline)
	if err == nil {
		if !b.passesCandidateFilters(pinProvider, policy, req) {
			return nil, &Error{
				Status:    409,
				Reason:    ReasonConstraintExcluded,
				Message:   fmt.Sprintf("key %q exists on provider %q, which is excluded by this request's constraints", req.Key, pinProvider),
				Providers: []string{pinProvider},
			}, false
		}
		return &Result{
			Handle: h,
			Replay: true,
			Policy: policy.Name,
		}, nil, false
	}
	if errors.Is(err, provider.ErrNotFound) {
		return nil, nil, true
	}
	switch provider.KindOf(err) {
	case provider.KindKeyConflict:
		return nil, &Error{
			Status:    409,
			Reason:    ReasonSpecMismatch,
			Message:   err.Error(),
			Providers: []string{pinProvider},
		}, false
	case provider.KindKeyBusy, provider.KindBusy:
		reason := ReasonBusy
		if provider.KindOf(err) == provider.KindKeyBusy {
			reason = ReasonKeyBusy
		}
		return nil, &Error{
			Status:     429,
			Reason:     reason,
			Message:    err.Error(),
			Providers:  []string{pinProvider},
			RetryAfter: b.jitterDelay(provider.RetryAfterOf(err)),
		}, false
	default:
		if provider.KindOf(err) == provider.KindRejected {
			t.health.recordRejected(err)
		}
		return nil, &Error{
			Status:    504,
			Reason:    ReasonUnknownOutcome,
			Message:   fmt.Sprintf("lookup on pinned provider %q was inconclusive: %v", pinProvider, err),
			Providers: []string{pinProvider},
		}, false
	}
}

func (b *Binder) lookupRetryAll(
	reqCtx context.Context,
	targets map[string]*target,
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
	claimReq provider.ClaimRequest,
	reqDeadline time.Time,
) (*Result, *Error, string, bool) {
	// Probe every claim-capable provider unfiltered: the provider holding the
	// key is often the one that just flaked and would otherwise be skipped by
	// a reachability or health filter.
	var all []*target
	var nonBatched []*target
	for _, t := range targets {
		if t.cfg.Enabled && t.prov.Binder != nil {
			all = append(all, t)
			if t.coalescer == nil {
				nonBatched = append(nonBatched, t)
			}
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].id < all[j].id })
	if len(all) == 0 {
		return nil, nil, "", true
	}

	// All-or-nothing reservation across non-batched providers; batched
	// providers reserve 1 token per batch inside their coalescer.
	if len(nonBatched) > 0 {
		wait, ok := b.reserveLookupTokens(reqDeadline, nonBatched)
		if !ok {
			names := make([]string, len(all))
			for i, t := range all {
				names[i] = t.id
			}
			return nil, &Error{
				Status:     429,
				Reason:     ReasonRateLimited,
				Message:    "lookup rate limit exceeded during retry probe",
				Providers:  names,
				RetryAfter: b.jitterDelay(wait),
			}, "", false
		}
		if wait > 0 {
			if !b.sleepUntil(reqCtx, b.now().Add(wait)) {
				return nil, &Error{
					Status:    504,
					Reason:    ReasonUnknownOutcome,
					Message:   "request cancelled while waiting for retry lookup tokens",
					Providers: []string{},
				}, "", false
			}
		}
	}

	type probeOutcome struct {
		target *target
		handle provider.Handle
		err    error
	}
	outcomes := make([]probeOutcome, len(all))
	var wg sync.WaitGroup
	for i, t := range all {
		wg.Add(1)
		go func(i int, t *target) {
			defer wg.Done()
			if t.coalescer != nil {
				attemptTimeout := b.attemptDuration(t.cfg.Timeout, reqDeadline)
				pCtx, cancel := context.WithTimeout(context.WithoutCancel(reqCtx), attemptTimeout)
				defer cancel()
				h, err, _ := t.coalescer.lookup(pCtx, claimReq, attemptTimeout)
				outcomes[i] = probeOutcome{target: t, handle: h, err: err}
				return
			}
			h, err := b.callLookup(reqCtx, t, claimReq, reqDeadline)
			outcomes[i] = probeOutcome{target: t, handle: h, err: err}
		}(i, t)
	}
	wg.Wait()

	var (
		busyProviders         []string
		maxBusyWait           time.Duration
		hasKeyBusy            bool
		inconclusiveProviders []string
		lastInconclusiveErr   error
	)

	// 1. Any found -> 200 if it passes request filters, else 409 ConstraintExcluded.
	for _, o := range outcomes {
		if o.err == nil {
			if !b.passesCandidateFilters(o.target.id, policy, req) {
				return nil, &Error{
					Status:    409,
					Reason:    ReasonConstraintExcluded,
					Message:   fmt.Sprintf("key %q exists on provider %q, which is excluded by this request's constraints", req.Key, o.target.id),
					Providers: []string{o.target.id},
				}, "", false
			}
			return &Result{
				Handle: o.handle,
				Replay: true,
				Policy: policy.Name,
			}, nil, "", false
		}
	}

	// 2. Any KeyConflict -> 409 SpecMismatch.
	for _, o := range outcomes {
		if provider.KindOf(o.err) == provider.KindKeyConflict {
			return nil, &Error{
				Status:    409,
				Reason:    ReasonSpecMismatch,
				Message:   o.err.Error(),
				Providers: []string{o.target.id},
			}, "", false
		}
	}

	// 3. Classify remaining non-ErrNotFound outcomes.
	for _, o := range outcomes {
		if errors.Is(o.err, provider.ErrNotFound) {
			continue
		}
		switch provider.KindOf(o.err) {
		case provider.KindBusy, provider.KindKeyBusy:
			busyProviders = append(busyProviders, o.target.id)
			if provider.KindOf(o.err) == provider.KindKeyBusy {
				hasKeyBusy = true
			}
			if d := provider.RetryAfterOf(o.err); d > maxBusyWait {
				maxBusyWait = d
			}
		default:
			if provider.KindOf(o.err) == provider.KindRejected {
				o.target.health.recordRejected(o.err)
			}
			inconclusiveProviders = append(inconclusiveProviders, o.target.id)
			lastInconclusiveErr = o.err
		}
	}

	// 4. All ErrNotFound -> safe to proceed as a fresh claim attempt.
	if len(busyProviders) == 0 && len(inconclusiveProviders) == 0 {
		return nil, nil, "", true
	}

	// 5. Any Busy or KeyBusy -> 429 with Retry-After (not 504, so Lookup rate
	// limits never loop into more Lookups).
	if len(busyProviders) > 0 {
		reason := ReasonBusy
		if hasKeyBusy {
			reason = ReasonKeyBusy
		}
		return nil, &Error{
			Status:     429,
			Reason:     reason,
			Message:    "one or more providers rate-limited or reported key busy during retry lookup",
			Providers:  busyProviders,
			RetryAfter: b.jitterDelay(maxBusyWait),
		}, "", false
	}

	// 6. Exactly one inconclusive -> pin to it if it passes request filters,
	// else 504.
	if len(inconclusiveProviders) == 1 {
		pin := inconclusiveProviders[0]
		if b.passesCandidateFilters(pin, policy, req) {
			return nil, nil, pin, true
		}
		return nil, &Error{
			Status:    504,
			Reason:    ReasonUnknownOutcome,
			Message:   fmt.Sprintf("retry lookup on provider %q was inconclusive: %v", pin, lastInconclusiveErr),
			Providers: inconclusiveProviders,
		}, "", false
	}

	// 7. More than one inconclusive -> 504 {providers}.
	return nil, &Error{
		Status:    504,
		Reason:    ReasonUnknownOutcome,
		Message:   fmt.Sprintf("retry lookup was inconclusive across multiple providers: %v", lastInconclusiveErr),
		Providers: inconclusiveProviders,
	}, "", false
}

// reserveLookupTokens reserves one token on every target in list, all or
// nothing. If the longest wait exceeds reqDeadline, every reservation is
// cancelled so slow buckets never drain fast ones.
func (b *Binder) reserveLookupTokens(reqDeadline time.Time, list []*target) (time.Duration, bool) {
	now := b.now()
	remaining := reqDeadline.Sub(now)
	if remaining <= 0 {
		return time.Second, false
	}
	reservations := make([]*rate.Reservation, 0, len(list))
	var maxDelay time.Duration
	for _, t := range list {
		r := t.lookupRate.ReserveN(now, 1)
		if !r.OK() {
			for _, prev := range reservations {
				prev.CancelAt(now)
			}
			return time.Second, false
		}
		reservations = append(reservations, r)
		if d := r.DelayFrom(now); d > maxDelay {
			maxDelay = d
		}
	}
	if maxDelay > remaining {
		for _, prev := range reservations {
			prev.CancelAt(now)
		}
		return maxDelay, false
	}
	return maxDelay, true
}

func (b *Binder) callLookup(
	reqCtx context.Context,
	t *target,
	claimReq provider.ClaimRequest,
	reqDeadline time.Time,
) (h provider.Handle, err error) {
	if !b.startAttempt() {
		return provider.Handle{}, provider.Unavailable(errors.New("scheduler is shutting down"))
	}
	defer b.finishAttempt()

	// Lookup watches waiting for binding count against maxInFlight just like
	// Claim attempts.
	lim := t.slots
	if !lim.tryAcquire() {
		return provider.Handle{}, provider.Busy(100*time.Millisecond, errors.New("provider in-flight slots full"))
	}
	defer lim.release()

	attemptTimeout := b.attemptDuration(t.cfg.Timeout, reqDeadline)
	attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(reqCtx), attemptTimeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			h = provider.Handle{}
			err = fmt.Errorf("panic in adapter Lookup: %v", r)
		}
	}()

	return t.prov.Binder.Lookup(attemptCtx, claimReq)
}

func (b *Binder) claimLoop(
	reqCtx context.Context,
	targets map[string]*target,
	pinned string,
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
	claimReq provider.ClaimRequest,
	reqDeadline time.Time,
) (*Result, *Error) {
	var (
		tried             = map[string]bool{}
		slotsFull         = map[string]*slotLimiter{}
		overRate          = map[string]time.Duration{}
		cooldownProviders = map[string]time.Duration{}
		maxCooldownWait   time.Duration
		spills            = 0
		waitedForSlot     = false
		rejectedCount     = 0
		unavailableCount  = 0
		busyCount         = 0
		maxBusyWait       time.Duration
		lastErr           error
		lastExplanation   string
	)

	for spills < b.maxSpill && b.now().Before(reqDeadline) {
		if reqCtx.Err() != nil && len(tried) > 0 {
			// Client disconnected: stop spilling to further providers.
			break
		}

		allSnapshots := b.reg.Snapshot()
		if len(allSnapshots) == 0 {
			return nil, &Error{
				Status:    503,
				Reason:    ReasonNoProviders,
				Message:   "no providers are configured; create a SandboxProvider",
				Providers: []string{},
			}
		}

		filterProfile, schedReq, err := b.buildScheduleProfile(policy, req, allSnapshots)
		if err != nil {
			return nil, &Error{
				Status:    422,
				Reason:    ReasonInvalidPolicy,
				Message:   "policy " + policy.Name + " is not usable: " + err.Error(),
				Providers: []string{},
			}
		}

		now := b.now()
		candidates := make([]framework.Candidate, 0, len(allSnapshots))
		for _, c := range allSnapshots {
			if pinned != "" && c.Provider != pinned {
				continue
			}
			if tried[c.Provider] {
				continue
			}
			if _, full := slotsFull[c.Provider]; full {
				continue
			}
			t, ok := targets[c.Provider]
			if !ok || !t.cfg.Enabled || t.prov.Binder == nil {
				continue
			}
			if !t.health.isUsable(now, t.prov.Binder, t.slots, b.startAttempt, b.finishAttempt, t.cfg.Timeout, b.owner, probeTemplateForTarget(req.Template, t.cfg.Templates)) {
				continue
			}
			if b.isRefused(c.Provider, req.Template, now) {
				continue
			}
			if len(t.cfg.Templates) > 0 && len(c.Templates) == 0 {
				c.Templates = t.cfg.Templates
			}
			// Only treat a provider as rate-limited or in cooldown when it
			// actually satisfies the policy and request hard filters; otherwise
			// let profile.Schedule reject it and record the filter explanation.
			if passesHardFilters(reqCtx, filterProfile.Filters, schedReq, &c) {
				if wait := b.cooldownRemaining(c.Provider, now); wait > 0 {
					cooldownProviders[c.Provider] = wait
					if wait > maxCooldownWait {
						maxCooldownWait = wait
					}
					continue
				}
				// Cold cluster claims and all elastic claims are bounded by maxRate.
				if t.cfg.Elastic || c.WarmCapacity <= 0 {
					if t.claimRate.TokensAt(now) < 1 {
						overRate[c.Provider] = b.timeUntilNextToken(t.claimRate, now)
						continue
					}
				}
			}
			candidates = append(candidates, c)
		}

		profile, schedReq, err := b.buildScheduleProfile(policy, req, candidates)
		if err != nil {
			return nil, &Error{
				Status:    422,
				Reason:    ReasonInvalidPolicy,
				Message:   "policy " + policy.Name + " is not usable: " + err.Error(),
				Providers: []string{},
			}
		}

		var decision *framework.Decision
		if len(candidates) > 0 {
			decision, err = profile.Schedule(reqCtx, schedReq, candidates)
			if err != nil {
				var noCand *framework.ErrNoCandidates
				if errors.As(err, &noCand) {
					lastExplanation = framework.ExplainResults(noCand.Results)
				}
			}
		}

		if decision == nil {
			// If candidates were excluded only because their in-flight slots
			// were full, wait once up to queueTimeout for any slot to open.
			if len(slotsFull) > 0 && !waitedForSlot {
				waitedForSlot = true
				lims := make([]*slotLimiter, 0, len(slotsFull))
				for _, l := range slotsFull {
					lims = append(lims, l)
				}
				waitDur := b.queueTimeout
				if rem := reqDeadline.Sub(b.now()); rem < waitDur {
					waitDur = rem
				}
				if waitDur > 0 {
					wCtx, cancel := context.WithTimeout(reqCtx, waitDur)
					opened := waitAny(wCtx, lims)
					cancel()
					if opened {
						slotsFull = map[string]*slotLimiter{}
						continue
					}
				}
				return nil, &Error{
					Status:     429,
					Reason:     ReasonSlotsFull,
					Message:    "all viable providers have reached maxInFlight capacity",
					Providers:  sortedKeysFromSlotMap(slotsFull),
					RetryAfter: b.jitterDelay(b.queueTimeout),
				}
			}

			if len(overRate) > 0 {
				earliest := time.Second
				first := true
				names := make([]string, 0, len(overRate))
				for p, w := range overRate {
					names = append(names, p)
					if first || w < earliest {
						earliest = w
						first = false
					}
				}
				sort.Strings(names)
				return nil, &Error{
					Status:     429,
					Reason:     ReasonRateLimited,
					Message:    "all viable providers exceeded spec.claims.maxRate",
					Providers:  names,
					RetryAfter: b.jitterDelay(earliest),
				}
			}

			break
		}

		chosen := decision.Provider
		t := targets[chosen]
		lim := t.slots
		if !lim.tryAcquire() {
			// Non-blocking: re-decide away from a full provider immediately
			// without spending a spill.
			slotsFull[chosen] = lim
			continue
		}

		res, berr, action := b.attemptProvider(
			reqCtx, t, lim, decision, candidates, policy, req, claimReq, reqDeadline,
		)
		switch action {
		case attemptDone:
			return res, berr
		case attemptRetryDecision:
			// Lost warm reservation race or rate check before calling the
			// provider; re-run Schedule without counting a spill.
			continue
		case attemptSpill:
			tried[chosen] = true
			lastErr = berr
			lastExplanation = decision.Explain()
			switch berr.Reason {
			case ReasonRejected:
				rejectedCount++
			case ReasonUnavailable:
				unavailableCount++
			case ReasonBusy:
				busyCount++
				if berr.RetryAfter > maxBusyWait {
					maxBusyWait = berr.RetryAfter
				}
			}
			spills++
			if spills < b.maxSpill {
				metrics.RecordClaimSpill()
			}
		}
	}

	triedList := make([]string, 0, len(tried))
	for p := range tried {
		triedList = append(triedList, p)
	}
	sort.Strings(triedList)

	if len(tried) > 0 {
		if rejectedCount == len(tried) {
			msg := "all candidate providers rejected the claim"
			if lastErr != nil {
				msg = lastErr.Error()
			}
			return nil, &Error{
				Status:      422,
				Reason:      ReasonRejected,
				Message:     msg,
				Providers:   triedList,
				Explanation: lastExplanation,
			}
		}
		if busyCount > 0 && unavailableCount == 0 && rejectedCount+busyCount == len(tried) {
			return nil, &Error{
				Status:      429,
				Reason:      ReasonBusy,
				Message:     "candidate providers rate-limited the claim",
				Providers:   triedList,
				RetryAfter:  b.jitterDelay(maxBusyWait),
				Explanation: lastExplanation,
			}
		}
		if unavailableCount > 0 && unavailableCount+busyCount+rejectedCount == len(tried) {
			msg := "candidate providers were unavailable"
			if lastErr != nil {
				msg = lastErr.Error()
			}
			var retryAfter time.Duration
			if maxBusyWait > 0 {
				retryAfter = b.jitterDelay(maxBusyWait)
			}
			return nil, &Error{
				Status:      503,
				Reason:      ReasonUnavailable,
				Message:     msg,
				Providers:   triedList,
				RetryAfter:  retryAfter,
				Explanation: lastExplanation,
			}
		}
	}

	if len(cooldownProviders) > 0 {
		names := make([]string, 0, len(cooldownProviders))
		for p := range cooldownProviders {
			names = append(names, p)
		}
		sort.Strings(names)
		return nil, &Error{
			Status:     429,
			Reason:     ReasonBusy,
			Message:    "viable providers are in rate-limit cooldown",
			Providers:  names,
			RetryAfter: b.jitterDelay(maxCooldownWait),
		}
	}

	msg := "no provider had capacity to serve the claim"
	if lastErr != nil {
		msg = lastErr.Error()
	}
	return nil, &Error{
		Status:      409,
		Reason:      ReasonNoCapacity,
		Message:     msg,
		Providers:   triedList,
		Explanation: lastExplanation,
	}
}

func passesHardFilters(
	ctx context.Context,
	filters []framework.Filter,
	req *framework.Request,
	cand *framework.Candidate,
) bool {
	for _, f := range filters {
		if ok, _ := f.Filter(ctx, req, cand); !ok {
			return false
		}
	}
	return true
}

type attemptAction int

const (
	attemptDone attemptAction = iota
	attemptRetryDecision
	attemptSpill
)

// attemptProvider runs a single provider attempt in its own function so defer
// Releases the provider slot at the end of the attempt rather than the end of
// the request.
func (b *Binder) attemptProvider(
	reqCtx context.Context,
	t *target,
	lim *slotLimiter,
	decision *framework.Decision,
	candidates []framework.Candidate,
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
	claimReq provider.ClaimRequest,
	reqDeadline time.Time,
) (*Result, *Error, attemptAction) {
	defer lim.release()

	chosen := t.id
	hadWarm := warmCandidate(candidates, chosen)
	var warmTicket registry.ReservationTicket

	// Slot is acquired before reserving capacity so refusals that never reach
	// a provider do not drain its ledger.
	if !t.cfg.Elastic && hadWarm {
		ticket, ok := b.reg.TryReserveWarmTicket(chosen)
		if !ok {
			// Another concurrent request took the last warm sandbox on this
			// provider; re-run scheduling against the updated ledger.
			return nil, nil, attemptRetryDecision
		}
		warmTicket = ticket
	} else {
		now := b.now()
		if !t.claimRate.AllowN(now, 1) {
			return nil, nil, attemptRetryDecision
		}
		b.reg.RecordColdPlacement(chosen)
	}

	h, replay, err := b.invokeClaim(reqCtx, t, claimReq, reqDeadline)
	if err == nil {
		t.health.recordSuccess(b.now())
		return &Result{
			Handle:      h,
			Replay:      replay,
			Warm:        warmTicket.Valid(),
			Policy:      policy.Name,
			Score:       decision.Score,
			Explanation: decision.Explain(),
		}, nil, attemptDone
	}

	kind := provider.KindOf(err)
	switch kind {
	case provider.KindKeyConflict:
		b.reg.ReleaseReservationTicket(warmTicket)
		return nil, &Error{
			Status:    409,
			Reason:    ReasonSpecMismatch,
			Message:   err.Error(),
			Providers: []string{chosen},
		}, attemptDone

	case provider.KindKeyBusy:
		// The key's previous sandbox is still being deleted on this provider.
		// Pin here and retry within reqDeadline; never spill, or a second
		// sandbox would be created while the first still exists.
		b.reg.ReleaseReservationTicket(warmTicket)
		return b.retryPinnedKeyBusy(reqCtx, t, decision, policy, req, claimReq, reqDeadline, err)

	case provider.KindUnknownOutcome:
		// Outcome is unknown: keep the warm reservation and pin to this
		// provider. Probe Lookup and retry within the per-provider retry
		// budget; never spill unless Lookup proves nothing was created.
		return b.recoverUnknownOutcome(reqCtx, t, decision, policy, req, claimReq, warmTicket, reqDeadline, err, true)

	case provider.KindNoCapacity:
		// Provider confirmed it has no capacity for this template. Keep the
		// reservation consumed and record a global refusal for refusalTTL
		// scoped to (provider, template).
		b.recordRefusal(chosen, req.Template, b.now())
		return nil, &Error{
			Status:    409,
			Reason:    ReasonNoCapacity,
			Message:   err.Error(),
			Providers: []string{chosen},
		}, attemptSpill

	case provider.KindBusy:
		b.reg.ReleaseReservationTicket(warmTicket)
		wait := provider.RetryAfterOf(err)
		if wait <= 0 {
			wait = b.refusalTTL
		}
		b.recordCooldown(chosen, b.now().Add(wait))
		return nil, &Error{
			Status:     429,
			Reason:     ReasonBusy,
			Message:    err.Error(),
			Providers:  []string{chosen},
			RetryAfter: wait,
		}, attemptSpill

	case provider.KindUnavailable:
		b.reg.ReleaseReservationTicket(warmTicket)
		t.health.recordUnavailable(b.now(), err)
		return nil, &Error{
			Status:    503,
			Reason:    ReasonUnavailable,
			Message:   err.Error(),
			Providers: []string{chosen},
		}, attemptSpill

	case provider.KindRejected:
		b.reg.ReleaseReservationTicket(warmTicket)
		t.health.recordRejected(err)
		return nil, &Error{
			Status:    422,
			Reason:    ReasonRejected,
			Message:   err.Error(),
			Providers: []string{chosen},
		}, attemptSpill

	default:
		return b.recoverUnknownOutcome(reqCtx, t, decision, policy, req, claimReq, warmTicket, reqDeadline, err, true)
	}
}

func (b *Binder) invokeClaim(
	reqCtx context.Context,
	t *target,
	claimReq provider.ClaimRequest,
	reqDeadline time.Time,
) (h provider.Handle, replay bool, err error) {
	if !b.startAttempt() {
		return provider.Handle{}, false, provider.Unavailable(errors.New("scheduler is shutting down"))
	}
	defer b.finishAttempt()

	start := b.now()
	attemptTimeout := b.attemptDuration(t.cfg.Timeout, reqDeadline)
	// Detached from reqCtx cancellation so a client disconnect stops spilling
	// without abandoning an in-flight provider create mid-flight.
	attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(reqCtx), attemptTimeout)
	defer cancel()

	defer func() {
		took := b.now().Sub(start)
		if r := recover(); r != nil {
			h = provider.Handle{}
			replay = false
			err = fmt.Errorf("panic in adapter Claim: %v", r)
			metrics.RecordClaimAttempt(t.id, provider.KindUnknownOutcome.String(), took)
			return
		}
		if err == nil {
			metrics.RecordClaimAttempt(t.id, "ok", took)
		} else {
			metrics.RecordClaimAttempt(t.id, provider.KindOf(err).String(), took)
		}
	}()

	h, err = t.prov.Binder.Claim(attemptCtx, claimReq)
	return h, false, err
}

func (b *Binder) retryPinnedKeyBusy(
	reqCtx context.Context,
	t *target,
	decision *framework.Decision,
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
	claimReq provider.ClaimRequest,
	reqDeadline time.Time,
	initialErr error,
) (*Result, *Error, attemptAction) {
	lastErr := initialErr
	requiresWarm := req.WarmOnly
	for b.now().Before(reqDeadline) && reqCtx.Err() == nil {
		wait := provider.RetryAfterOf(lastErr)
		if wait <= 0 {
			wait = 100 * time.Millisecond
		}
		nextTry := b.now().Add(wait)
		if !nextTry.Before(reqDeadline) {
			break
		}
		if !b.sleepUntil(reqCtx, nextTry) {
			break
		}

		// Reacquire warm capacity (or account a cold placement) for this retry
		// attempt so the ledger stays accurate and warmOnly is enforced.
		var retryTicket registry.ReservationTicket
		if !t.cfg.Elastic {
			retryTicket, _ = b.reg.TryReserveWarmTicket(t.id)
		}
		if !retryTicket.Valid() {
			if requiresWarm {
				return nil, &Error{
					Status:    409,
					Reason:    ReasonNoCapacity,
					Message:   fmt.Sprintf("provider %q has no warm capacity remaining while retrying key %q", t.id, req.Key),
					Providers: []string{t.id},
				}, attemptDone
			}
			now := b.now()
			if !t.claimRate.AllowN(now, 1) {
				return nil, &Error{
					Status:     429,
					Reason:     ReasonRateLimited,
					Message:    fmt.Sprintf("provider %q exceeded spec.claims.maxRate while retrying key %q", t.id, req.Key),
					Providers:  []string{t.id},
					RetryAfter: b.jitterDelay(b.timeUntilNextToken(t.claimRate, now)),
				}, attemptDone
			}
			b.reg.RecordColdPlacement(t.id)
		}

		h, replay, err := b.invokeClaim(reqCtx, t, claimReq, reqDeadline)
		if err == nil {
			t.health.recordSuccess(b.now())
			return &Result{
				Handle:      h,
				Replay:      replay,
				Warm:        retryTicket.Valid(),
				Policy:      policy.Name,
				Score:       decision.Score,
				Explanation: decision.Explain(),
			}, nil, attemptDone
		}

		switch provider.KindOf(err) {
		case provider.KindKeyBusy:
			b.reg.ReleaseReservationTicket(retryTicket)
			lastErr = err
			continue
		case provider.KindKeyConflict:
			b.reg.ReleaseReservationTicket(retryTicket)
			return nil, &Error{
				Status:    409,
				Reason:    ReasonSpecMismatch,
				Message:   err.Error(),
				Providers: []string{t.id},
			}, attemptDone
		case provider.KindNoCapacity:
			b.recordRefusal(t.id, req.Template, b.now())
			return nil, &Error{
				Status:    409,
				Reason:    ReasonNoCapacity,
				Message:   err.Error(),
				Providers: []string{t.id},
			}, attemptDone
		case provider.KindBusy:
			b.reg.ReleaseReservationTicket(retryTicket)
			bw := provider.RetryAfterOf(err)
			if bw <= 0 {
				bw = b.refusalTTL
			}
			b.recordCooldown(t.id, b.now().Add(bw))
			return nil, &Error{
				Status:     429,
				Reason:     ReasonBusy,
				Message:    err.Error(),
				Providers:  []string{t.id},
				RetryAfter: b.jitterDelay(bw),
			}, attemptDone
		case provider.KindUnavailable:
			b.reg.ReleaseReservationTicket(retryTicket)
			t.health.recordUnavailable(b.now(), err)
			return nil, &Error{
				Status:    503,
				Reason:    ReasonUnavailable,
				Message:   err.Error(),
				Providers: []string{t.id},
			}, attemptDone
		case provider.KindRejected:
			b.reg.ReleaseReservationTicket(retryTicket)
			t.health.recordRejected(err)
			return nil, &Error{
				Status:    422,
				Reason:    ReasonRejected,
				Message:   err.Error(),
				Providers: []string{t.id},
			}, attemptDone
		default:
			return b.recoverUnknownOutcome(reqCtx, t, decision, policy, req, claimReq, retryTicket, reqDeadline, err, false)
		}
	}
	return nil, &Error{
		Status:     429,
		Reason:     ReasonKeyBusy,
		Message:    lastErr.Error(),
		Providers:  []string{t.id},
		RetryAfter: b.jitterDelay(provider.RetryAfterOf(lastErr)),
	}, attemptDone
}

func (b *Binder) recoverUnknownOutcome(
	reqCtx context.Context,
	t *target,
	decision *framework.Decision,
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
	claimReq provider.ClaimRequest,
	warmTicket registry.ReservationTicket,
	reqDeadline time.Time,
	initialErr error,
	allowSpill bool,
) (*Result, *Error, attemptAction) {
	base := 100 * time.Millisecond
	capBackoff := 2 * time.Second
	lastErr := initialErr
	spillOrDone := attemptDone
	if allowSpill {
		spillOrDone = attemptSpill
	}

	for step := 0; b.now().Before(reqDeadline) && reqCtx.Err() == nil; step++ {
		if !t.retryBudget.AllowN(b.now(), 1) {
			break
		}
		ceil := base << step
		if ceil > capBackoff || ceil <= 0 {
			ceil = capBackoff
		}
		sleepDur := time.Duration(b.randFloat() * float64(ceil))
		if sleepDur <= 0 {
			sleepDur = 10 * time.Millisecond
		}
		wakeAt := b.now().Add(sleepDur)
		if !wakeAt.Before(reqDeadline) {
			break
		}
		if !b.sleepUntil(reqCtx, wakeAt) {
			break
		}

		// Recovery Lookups must also respect the provider's lookupRate bound.
		now := b.now()
		lres := t.lookupRate.ReserveN(now, 1)
		if !lres.OK() {
			break
		}
		if ldelay := lres.DelayFrom(now); ldelay > 0 {
			lwake := now.Add(ldelay)
			if !lwake.Before(reqDeadline) {
				lres.CancelAt(now)
				break
			}
			if !b.sleepUntil(reqCtx, lwake) {
				lres.CancelAt(b.now())
				break
			}
		}

		// Probe Lookup on the pinned provider first.
		h, lerr := b.callLookupNoSlot(reqCtx, t, claimReq, reqDeadline)
		if lerr == nil {
			t.health.recordSuccess(b.now())
			return &Result{
				Handle:      h,
				Replay:      true,
				Warm:        warmTicket.Valid(),
				Policy:      policy.Name,
				Score:       decision.Score,
				Explanation: decision.Explain(),
			}, nil, attemptDone
		}
		if provider.KindOf(lerr) == provider.KindKeyConflict {
			b.reg.ReleaseReservationTicket(warmTicket)
			return nil, &Error{
				Status:    409,
				Reason:    ReasonSpecMismatch,
				Message:   lerr.Error(),
				Providers: []string{t.id},
			}, attemptDone
		}
		if errors.Is(lerr, provider.ErrNotFound) {
			// Confirmed not created: retry Claim on this same provider within
			// the deadline. Cold cluster claims and all elastic claims must
			// also respect the provider's claimRate bound.
			if !b.now().Before(reqDeadline) || reqCtx.Err() != nil {
				break
			}
			if t.cfg.Elastic || !warmTicket.Valid() {
				cnow := b.now()
				cres := t.claimRate.ReserveN(cnow, 1)
				if !cres.OK() {
					break
				}
				if cdelay := cres.DelayFrom(cnow); cdelay > 0 {
					cwake := cnow.Add(cdelay)
					if !cwake.Before(reqDeadline) {
						cres.CancelAt(cnow)
						break
					}
					if !b.sleepUntil(reqCtx, cwake) {
						cres.CancelAt(b.now())
						break
					}
				}
			}
			ch, _, cerr := b.invokeClaim(reqCtx, t, claimReq, reqDeadline)
			if cerr == nil {
				t.health.recordSuccess(b.now())
				return &Result{
					Handle:      ch,
					Replay:      false,
					Warm:        warmTicket.Valid(),
					Policy:      policy.Name,
					Score:       decision.Score,
					Explanation: decision.Explain(),
				}, nil, attemptDone
			}
			lastErr = cerr
			switch provider.KindOf(cerr) {
			case provider.KindKeyConflict:
				b.reg.ReleaseReservationTicket(warmTicket)
				return nil, &Error{
					Status:    409,
					Reason:    ReasonSpecMismatch,
					Message:   cerr.Error(),
					Providers: []string{t.id},
				}, attemptDone
			case provider.KindKeyBusy:
				b.reg.ReleaseReservationTicket(warmTicket)
				return b.retryPinnedKeyBusy(reqCtx, t, decision, policy, req, claimReq, reqDeadline, cerr)
			case provider.KindNoCapacity:
				b.recordRefusal(t.id, req.Template, b.now())
				return nil, &Error{
					Status:    409,
					Reason:    ReasonNoCapacity,
					Message:   cerr.Error(),
					Providers: []string{t.id},
				}, spillOrDone
			case provider.KindBusy:
				b.reg.ReleaseReservationTicket(warmTicket)
				bw := provider.RetryAfterOf(cerr)
				if bw <= 0 {
					bw = b.refusalTTL
				}
				b.recordCooldown(t.id, b.now().Add(bw))
				retryWait := bw
				if !allowSpill {
					retryWait = b.jitterDelay(bw)
				}
				return nil, &Error{
					Status:     429,
					Reason:     ReasonBusy,
					Message:    cerr.Error(),
					Providers:  []string{t.id},
					RetryAfter: retryWait,
				}, spillOrDone
			case provider.KindUnavailable:
				b.reg.ReleaseReservationTicket(warmTicket)
				t.health.recordUnavailable(b.now(), cerr)
				return nil, &Error{
					Status:    503,
					Reason:    ReasonUnavailable,
					Message:   cerr.Error(),
					Providers: []string{t.id},
				}, spillOrDone
			case provider.KindRejected:
				b.reg.ReleaseReservationTicket(warmTicket)
				t.health.recordRejected(cerr)
				return nil, &Error{
					Status:    422,
					Reason:    ReasonRejected,
					Message:   cerr.Error(),
					Providers: []string{t.id},
				}, spillOrDone
			}
		} else {
			lastErr = lerr
		}
	}

	return nil, &Error{
		Status:    504,
		Reason:    ReasonUnknownOutcome,
		Message:   fmt.Sprintf("claim outcome on provider %q is unknown: %v", t.id, lastErr),
		Providers: []string{t.id},
	}, attemptDone
}

func (b *Binder) callLookupNoSlot(
	reqCtx context.Context,
	t *target,
	claimReq provider.ClaimRequest,
	reqDeadline time.Time,
) (h provider.Handle, err error) {
	if !b.startAttempt() {
		return provider.Handle{}, provider.Unavailable(errors.New("scheduler is shutting down"))
	}
	defer b.finishAttempt()

	attemptTimeout := b.attemptDuration(t.cfg.Timeout, reqDeadline)
	attemptCtx, cancel := context.WithTimeout(context.WithoutCancel(reqCtx), attemptTimeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			h = provider.Handle{}
			err = fmt.Errorf("panic in adapter Lookup: %v", r)
		}
	}()
	return t.prov.Binder.Lookup(attemptCtx, claimReq)
}

// Release deletes a sandbox on providerID by ID. Returns nil when the sandbox
// is already gone.
func (b *Binder) Release(ctx context.Context, providerID, sandboxID string) *Error {
	if rem := b.holdOffRemaining(b.now()); rem > 0 {
		return &Error{
			Status:     503,
			Reason:     ReasonHoldOff,
			Message:    "newly elected leader is holding off requests while pre-failover attempts settle",
			Providers:  []string{providerID},
			RetryAfter: rem + time.Duration(b.randFloat()*float64(3*time.Second)),
		}
	}

	t, ok := (*b.targets.Load())[providerID]
	if !ok || !t.cfg.Enabled || t.prov.Binder == nil {
		return &Error{
			Status:    404,
			Reason:    ReasonNoProviders,
			Message:   fmt.Sprintf("provider %q is not registered or not claim-capable", providerID),
			Providers: []string{providerID},
		}
	}

	if !b.startAttempt() {
		return &Error{
			Status:    503,
			Reason:    ReasonUnavailable,
			Message:   "scheduler is shutting down",
			Providers: []string{providerID},
		}
	}
	defer b.finishAttempt()

	rCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), t.cfg.Timeout)
	defer cancel()

	err := t.prov.Binder.Release(rCtx, provider.Handle{Provider: providerID, ID: sandboxID})
	if err == nil || errors.Is(err, provider.ErrNotFound) {
		b.evictCompletedFlight(providerID, sandboxID)
		return nil
	}
	switch provider.KindOf(err) {
	case provider.KindBusy, provider.KindKeyBusy:
		return &Error{
			Status:     429,
			Reason:     ReasonBusy,
			Message:    err.Error(),
			Providers:  []string{providerID},
			RetryAfter: b.jitterDelay(provider.RetryAfterOf(err)),
		}
	case provider.KindRejected:
		return &Error{
			Status:    422,
			Reason:    ReasonRejected,
			Message:   err.Error(),
			Providers: []string{providerID},
		}
	default:
		return &Error{
			Status:    504,
			Reason:    ReasonUnknownOutcome,
			Message:   err.Error(),
			Providers: []string{providerID},
		}
	}
}

func (b *Binder) evictCompletedFlight(providerID, sandboxID string) {
	b.flightsMu.Lock()
	defer b.flightsMu.Unlock()
	for k, fl := range b.flights {
		if !fl.completed || fl.res == nil {
			continue
		}
		if fl.res.Handle.Provider == providerID && (sandboxID == "" || fl.res.Handle.ID == sandboxID) {
			delete(b.flights, k)
		}
	}
}

func probeTemplateForTarget(reqTemplate string, templates map[string]string) string {
	if len(templates) == 0 {
		return reqTemplate
	}
	if reqTemplate != "" {
		if _, ok := templates[reqTemplate]; ok {
			return reqTemplate
		}
	}
	var first string
	for k := range templates {
		if first == "" || k < first {
			first = k
		}
	}
	return first
}

func (b *Binder) buildScheduleProfile(
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
	candidates []framework.Candidate,
) (*framework.Profile, *framework.Request, error) {
	profile, err := policy.BuildProfile(candidates)
	if err != nil {
		return nil, nil, err
	}
	if req.Template != "" && !hasFilter(profile.Filters, "TemplateAvailable") {
		profile.Filters = append(profile.Filters, plugins.TemplateAvailable{})
	}
	if req.WarmOnly && !hasFilter(profile.Filters, "WarmOnly") {
		profile.Filters = append(profile.Filters, plugins.WarmOnly{})
	}
	schedReq := policy.BuildRequest(req.Key, req.Requires, req.PreferProvider)
	schedReq.Template = req.Template
	schedReq.WarmOnly = req.WarmOnly
	return profile, schedReq, nil
}

// passesCandidateFilters checks whether providerID satisfies the hard filters
// of policy and req for a sandbox that already exists on providerID. WarmOnly
// is not checked here because a sandbox found by Lookup is already running and
// cand.WarmCapacity reflects remaining pool capacity after the sandbox was
// claimed.
func (b *Binder) passesCandidateFilters(
	providerID string,
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
) bool {
	if policy == nil {
		policy = v1alpha1.DefaultPolicy()
	}
	var cand *framework.Candidate
	allSnapshots := b.reg.Snapshot()
	for _, c := range allSnapshots {
		if c.Provider == providerID {
			cp := c
			cand = &cp
			break
		}
	}
	t := (*b.targets.Load())[providerID]
	if cand == nil {
		if t == nil {
			return false
		}
		cand = &framework.Candidate{
			Provider:  providerID,
			Templates: t.cfg.Templates,
		}
	} else if t != nil && len(cand.Templates) == 0 {
		cand.Templates = t.cfg.Templates
	}

	profile, schedReq, err := b.buildScheduleProfile(policy, req, allSnapshots)
	if err != nil {
		return false
	}
	for _, f := range profile.Filters {
		if f.Name() == "WarmOnly" {
			continue
		}
		if ok, _ := f.Filter(context.Background(), schedReq, cand); !ok {
			return false
		}
	}
	return true
}

func (b *Binder) passesJoinerConstraints(
	providerID string,
	wasWarm bool,
	policy *v1alpha1.SandboxPlacementPolicy,
	req Request,
) bool {
	if req.PinProvider != "" && req.PinProvider != providerID {
		return false
	}
	if req.WarmOnly && !wasWarm {
		return false
	}
	return b.passesCandidateFilters(providerID, policy, req)
}

func (b *Binder) recordRefusal(providerID, template string, now time.Time) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	b.refusals[refusalKey{provider: providerID, template: template}] = now.Add(b.refusalTTL)
}

func (b *Binder) isRefused(providerID, template string, now time.Time) bool {
	b.stateMu.RLock()
	until, ok := b.refusals[refusalKey{provider: providerID, template: template}]
	if !ok && template != "" {
		until, ok = b.refusals[refusalKey{provider: providerID, template: ""}]
	}
	b.stateMu.RUnlock()
	return ok && now.Before(until)
}

func (b *Binder) recordCooldown(providerID string, until time.Time) {
	b.stateMu.Lock()
	defer b.stateMu.Unlock()
	if cur, ok := b.cooldowns[providerID]; !ok || until.After(cur) {
		b.cooldowns[providerID] = until
	}
}

func (b *Binder) cooldownRemaining(providerID string, now time.Time) time.Duration {
	b.stateMu.RLock()
	until, ok := b.cooldowns[providerID]
	b.stateMu.RUnlock()
	if !ok || !now.Before(until) {
		return 0
	}
	return until.Sub(now)
}

func (b *Binder) attemptDuration(targetTimeout time.Duration, reqDeadline time.Time) time.Duration {
	d := targetTimeout
	if d <= 0 {
		d = DefaultAttemptTimeout
	}
	if rem := reqDeadline.Sub(b.now()); rem > 0 && rem < d {
		d = rem
	}
	if d <= 0 {
		d = time.Millisecond
	}
	return d
}

func (b *Binder) timeUntilNextToken(lim *rate.Limiter, now time.Time) time.Duration {
	r := lim.ReserveN(now, 1)
	if !r.OK() {
		return time.Second
	}
	d := r.DelayFrom(now)
	r.CancelAt(now)
	if d <= 0 {
		return 10 * time.Millisecond
	}
	return d
}

func (b *Binder) jitterDelay(base time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	jitter := time.Duration(b.randFloat() * float64(500*time.Millisecond))
	return base + jitter
}

func hasFilter(filters []framework.Filter, name string) bool {
	for _, f := range filters {
		if f.Name() == name {
			return true
		}
	}
	return false
}

func warmCandidate(candidates []framework.Candidate, providerID string) bool {
	for i := range candidates {
		if candidates[i].Provider == providerID {
			return candidates[i].WarmCapacity > 0
		}
	}
	return false
}

func sortedKeysFromSlotMap(m map[string]*slotLimiter) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (b *Binder) sleepUntil(ctx context.Context, target time.Time) bool {
	d := target.Sub(b.now())
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
