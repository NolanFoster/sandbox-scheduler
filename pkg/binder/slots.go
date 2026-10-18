package binder

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/NolanFoster/sandbox-scheduler/pkg/metrics"
	"github.com/NolanFoster/sandbox-scheduler/pkg/provider"
)

// slotLimiter bounds concurrent in-flight operations against one provider.
//
// When a provider's maxInFlight changes, the binder replaces its *slotLimiter
// rather than resizing it in place: each in-flight attempt holds a pointer to
// the limiter it acquired from and releases back into that same instance, so
// draining attempts never corrupt the new limit.
type slotLimiter struct {
	provider string
	limit    int
	inFlight atomic.Int64
	// notify is closed and replaced whenever a slot is freed so waiters
	// parked in waitAny wake immediately.
	mu     sync.Mutex
	notify chan struct{}
}

func newSlotLimiter(providerName string, limit int) *slotLimiter {
	if limit <= 0 {
		limit = DefaultProviderMaxInFlight
	}
	return &slotLimiter{
		provider: providerName,
		limit:    limit,
		notify:   make(chan struct{}),
	}
}

// tryAcquire takes one slot without blocking.
func (s *slotLimiter) tryAcquire() bool {
	for {
		cur := s.inFlight.Load()
		if int(cur) >= s.limit {
			return false
		}
		if s.inFlight.CompareAndSwap(cur, cur+1) {
			metrics.ClaimInflight.WithLabelValues(s.provider).Inc()
			return true
		}
	}
}

// release returns one slot to the limiter and wakes any parked waiters.
func (s *slotLimiter) release() {
	s.inFlight.Add(-1)
	metrics.ClaimInflight.WithLabelValues(s.provider).Dec()
	s.mu.Lock()
	ch := s.notify
	s.notify = make(chan struct{})
	close(ch)
	s.mu.Unlock()
}

func (s *slotLimiter) signalChan() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.notify
}

// waitAny blocks until at least one of limiters frees a slot or ctx expires.
func waitAny(ctx context.Context, limiters []*slotLimiter) bool {
	if len(limiters) == 0 {
		return false
	}
	// Fast check in case a slot opened between scheduling and waiting.
	for _, l := range limiters {
		if int(l.inFlight.Load()) < l.limit {
			return true
		}
	}
	merged := make(chan struct{}, 1)
	for _, l := range limiters {
		ch := l.signalChan()
		go func(c <-chan struct{}) {
			select {
			case <-c:
				select {
				case merged <- struct{}{}:
				default:
				}
			case <-ctx.Done():
			}
		}(ch)
	}
	select {
	case <-merged:
		return true
	case <-ctx.Done():
		return false
	}
}

// --- batch lookup coalescer ------------------------------------------------

const (
	defaultCoalesceWindow = 50 * time.Millisecond
	maxCoalesceBatchSize  = 100
)

type coalescedItem struct {
	req  provider.ClaimRequest
	done chan provider.LookupResult
}

// lookupCoalescer groups concurrent retry Lookups for a provider that
// implements BatchLookuper into batches of up to 100 keys over a 50ms window.
//
// Crucially, the batch leader reserves one Lookup token for the whole batch
// rather than each caller reserving its own token before coalescing. That is
// what lets a hosted provider rate-limited at 20 calls/s resolve up to 2,000
// retry keys/s during failover recovery.
type lookupCoalescer struct {
	batcher       provider.BatchLookuper
	limiter       *rate.Limiter
	slots         *slotLimiter
	startAttempt  func() bool
	finishAttempt func()
	window        time.Duration
	now           func() time.Time
	mu            sync.Mutex
	pending       []coalescedItem
	flushing      bool
	batchTimer    *time.Timer
}

func newLookupCoalescer(
	batcher provider.BatchLookuper,
	limiter *rate.Limiter,
	slots *slotLimiter,
	startAttempt func() bool,
	finishAttempt func(),
	window time.Duration,
	now func() time.Time,
) *lookupCoalescer {
	if window <= 0 {
		window = defaultCoalesceWindow
	}
	return &lookupCoalescer{
		batcher:       batcher,
		limiter:       limiter,
		slots:         slots,
		startAttempt:  startAttempt,
		finishAttempt: finishAttempt,
		window:        window,
		now:           now,
	}
}

func (c *lookupCoalescer) lookup(ctx context.Context, req provider.ClaimRequest, attemptTimeout time.Duration) (provider.Handle, error, time.Duration) {
	item := coalescedItem{
		req:  req,
		done: make(chan provider.LookupResult, 1),
	}

	c.mu.Lock()
	c.pending = append(c.pending, item)
	shouldFlushNow := len(c.pending) >= maxCoalesceBatchSize
	if !shouldFlushNow && c.batchTimer == nil {
		c.batchTimer = time.AfterFunc(c.window, func() {
			c.flush(attemptTimeout)
		})
	}
	if shouldFlushNow && c.batchTimer != nil {
		c.batchTimer.Stop()
		c.batchTimer = nil
	}
	c.mu.Unlock()

	if shouldFlushNow {
		go c.flush(attemptTimeout)
	}

	select {
	case res := <-item.done:
		return res.Handle, res.Err, provider.RetryAfterOf(res.Err)
	case <-ctx.Done():
		return provider.Handle{}, ctx.Err(), 0
	}
}

func (c *lookupCoalescer) flush(attemptTimeout time.Duration) {
	c.mu.Lock()
	if len(c.pending) == 0 {
		c.batchTimer = nil
		c.mu.Unlock()
		return
	}
	n := len(c.pending)
	if n > maxCoalesceBatchSize {
		n = maxCoalesceBatchSize
	}
	batch := make([]coalescedItem, n)
	copy(batch, c.pending[:n])
	c.pending = c.pending[n:]
	hasMore := len(c.pending) > 0
	if !hasMore {
		c.batchTimer = nil
	} else {
		c.batchTimer = time.AfterFunc(c.window, func() {
			c.flush(attemptTimeout)
		})
	}
	c.mu.Unlock()

	failBatch := func(err error) {
		for _, it := range batch {
			it.done <- provider.LookupResult{Err: err}
		}
	}

	// Reserve one Lookup token for the entire batch.
	now := c.now()
	res := c.limiter.ReserveN(now, 1)
	if !res.OK() {
		failBatch(provider.Busy(time.Second, errors.New("lookup rate limit exceeded")))
		return
	}
	delay := res.DelayFrom(now)
	if delay > attemptTimeout {
		res.CancelAt(now)
		failBatch(provider.Busy(delay, errors.New("lookup rate limit wait exceeds deadline")))
		return
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		<-timer.C
	}

	// Track detached attempt for graceful shutdown and bound concurrency via
	// the provider's slotLimiter just like ordinary Lookup calls.
	if c.startAttempt != nil && !c.startAttempt() {
		failBatch(provider.Unavailable(errors.New("scheduler is shutting down")))
		return
	}
	if c.finishAttempt != nil {
		defer c.finishAttempt()
	}

	if c.slots != nil {
		if !c.slots.tryAcquire() {
			failBatch(provider.Busy(100*time.Millisecond, errors.New("provider in-flight slots full")))
			return
		}
		defer c.slots.release()
	}

	reqs := make([]provider.ClaimRequest, len(batch))
	for i := range batch {
		reqs[i] = batch[i].req
	}
	ctx, cancel := context.WithTimeout(context.Background(), attemptTimeout)
	defer cancel()

	var results []provider.LookupResult
	func() {
		defer func() {
			if r := recover(); r != nil {
				results = nil
				failBatch(errors.New("panic in adapter LookupMany"))
			}
		}()
		results = c.batcher.LookupMany(ctx, reqs)
		for i, it := range batch {
			if i < len(results) {
				it.done <- results[i]
			} else {
				it.done <- provider.LookupResult{Err: errors.New("batch lookup omitted result")}
			}
		}
	}()
}

// --- claim health tracker --------------------------------------------------

const (
	healthWindowSize    = 20
	healthDemoteRatio   = 0.5
	probeInitialBackoff = time.Second
	probeMaxBackoff     = 30 * time.Second
)

// ClaimHealthStatus summarizes a provider's claim-path health for status
// reporting.
type ClaimHealthStatus struct {
	Healthy           bool
	CredentialsError  bool
	LastSuccessTime   time.Time
	LastFailureReason string
}

// healthTracker tracks windowed claim health separately from capacity polling.
//
// When at least 50% of the last 20 claim attempts fail with KindUnavailable,
// the provider's claim path is demoted. Recovery happens through half-open
// Lookup probes of a random never-used key with exponential backoff rather
// than on the next capacity poll, so a healthy /warmpool endpoint cannot flap
// a broken claim path every 10s.
type healthTracker struct {
	mu                sync.Mutex
	window            [healthWindowSize]bool // true = Unavailable
	count             int
	idx               int
	demoted           bool
	credentialsErr    bool
	lastSuccess       time.Time
	lastFailureReason string
	nextProbeAt       time.Time
	probeBackoff      time.Duration
	probing           bool
}

func newHealthTracker() *healthTracker {
	return &healthTracker{
		probeBackoff: probeInitialBackoff,
	}
}

func (h *healthTracker) recordSuccess(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pushOutcomeLocked(false)
	h.demoted = false
	h.credentialsErr = false
	h.lastSuccess = now
	h.lastFailureReason = ""
	h.probeBackoff = probeInitialBackoff
	h.probing = false
}

func (h *healthTracker) recordUnavailable(now time.Time, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pushOutcomeLocked(true)
	if err != nil {
		h.lastFailureReason = err.Error()
	}
	if h.count >= healthWindowSize && h.unavailableRatioLocked() >= healthDemoteRatio {
		if !h.demoted {
			h.demoted = true
			h.probeBackoff = probeInitialBackoff
			h.nextProbeAt = now.Add(h.probeBackoff)
		}
	}
}

func (h *healthTracker) recordRejected(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.credentialsErr = true
	if err != nil {
		h.lastFailureReason = err.Error()
	}
}

func (h *healthTracker) pushOutcomeLocked(unavailable bool) {
	h.window[h.idx] = unavailable
	h.idx = (h.idx + 1) % healthWindowSize
	if h.count < healthWindowSize {
		h.count++
	}
}

func (h *healthTracker) unavailableRatioLocked() float64 {
	if h.count == 0 {
		return 0
	}
	unavail := 0
	for i := 0; i < h.count; i++ {
		if h.window[i] {
			unavail++
		}
	}
	return float64(unavail) / float64(h.count)
}

// isUsable reports whether the provider's claim path is currently healthy.
// When demoted and due for a half-open probe, it launches an asynchronous
// Lookup of a random never-used key bounded by the provider's slotLimiter and
// tracked for graceful shutdown.
func (h *healthTracker) isUsable(
	now time.Time,
	b provider.Binder,
	slots *slotLimiter,
	startAttempt func() bool,
	finishAttempt func(),
	timeout time.Duration,
	owner string,
) bool {
	h.mu.Lock()
	if !h.demoted {
		h.mu.Unlock()
		return true
	}
	if h.probing || now.Before(h.nextProbeAt) || b == nil {
		h.mu.Unlock()
		return false
	}
	if startAttempt != nil && !startAttempt() {
		h.mu.Unlock()
		return false
	}
	if slots != nil && !slots.tryAcquire() {
		if finishAttempt != nil {
			finishAttempt()
		}
		h.mu.Unlock()
		return false
	}
	h.probing = true
	h.mu.Unlock()

	go h.runHalfOpenProbe(now, b, slots, finishAttempt, timeout, owner)
	return false
}

func (h *healthTracker) runHalfOpenProbe(
	started time.Time,
	b provider.Binder,
	slots *slotLimiter,
	finishAttempt func(),
	timeout time.Duration,
	owner string,
) {
	if finishAttempt != nil {
		defer finishAttempt()
	}
	if slots != nil {
		defer slots.release()
	}

	if timeout <= 0 {
		timeout = DefaultAttemptTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	key := randomProbeKey()
	req := provider.ClaimRequest{
		Template: "probe",
		Key:      key,
		SpecHash: provider.SpecHashOf("probe", nil, time.Minute),
		Owner:    owner,
		TTL:      time.Minute,
	}
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = errors.New("panic in adapter Lookup during half-open probe")
			}
		}()
		_, err = b.Lookup(ctx, req)
	}()

	h.mu.Lock()
	defer h.mu.Unlock()
	h.probing = false
	if err == nil || errors.Is(err, provider.ErrNotFound) {
		// ErrNotFound proves the provider's claim API is reachable and our
		// credentials work, with zero side effects.
		h.demoted = false
		h.credentialsErr = false
		h.count = 0
		h.idx = 0
		h.lastSuccess = started
		h.lastFailureReason = ""
		h.probeBackoff = probeInitialBackoff
		return
	}
	if provider.KindOf(err) == provider.KindRejected {
		h.credentialsErr = true
	}
	h.lastFailureReason = err.Error()
	h.probeBackoff *= 2
	if h.probeBackoff > probeMaxBackoff {
		h.probeBackoff = probeMaxBackoff
	}
	h.nextProbeAt = started.Add(h.probeBackoff)
}

func (h *healthTracker) status() ClaimHealthStatus {
	h.mu.Lock()
	defer h.mu.Unlock()
	return ClaimHealthStatus{
		Healthy:           !h.demoted && !h.credentialsErr,
		CredentialsError:  h.credentialsErr,
		LastSuccessTime:   h.lastSuccess,
		LastFailureReason: h.lastFailureReason,
	}
}

func randomProbeKey() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "probe-" + hex.EncodeToString(b[:])
}
