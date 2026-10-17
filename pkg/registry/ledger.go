package registry

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// ledger counts one provider's warm reservations without taking a lock.
//
// Reservations are taken on every warm placement, so they must not serialize
// requests behind the registry's mutex. Instead each reservation is counted in
// a ring of one-second buckets, each a single atomic word packing the second it
// belongs to with a count. A poll clears the buckets its report accounts for by
// compare-and-swap, which can only zero a bucket still holding the second it
// inspected, so a reservation landing concurrently is never lost.
//
// state packs the report's warm capacity with the number of reservations
// outstanding against it, the sum the scheduler subtracts. They share one word
// so that a claim is a compare-and-swap on everything it was decided from: if
// they were separate, a report lowering capacity and clearing reservations
// could return the count to a value a stalled claim had already read, and the
// claim would land against capacity that no longer exists. The count is raised
// before a bucket and lowered after one, so between the two it can only
// overstate reservations, which spills placements early rather than
// overplacing.
type ledger struct {
	state atomic.Uint64

	// reservations holds each reservation under the second it was made.
	reservations ring

	// carry holds reservations whose bucket was reused for a later second
	// before any poll cleared them, packed with the latest second among them.
	// Without it, a provider whose polls kept failing would forget its
	// reservations after one trip round the ring, and its stale report would
	// overstate capacity.
	carry atomic.Uint64

	// cold holds each cold placement under the second it was made. Unlike a
	// reservation, nothing a provider reports says when a cold start is done,
	// so these simply age out of the window; see Options.ColdStartWindow.
	cold       ring
	coldWindow uint32
}

// newLedger sizes the reservation ring to outlast the longest a reservation
// should wait for a poll that clears it: the grace period plus one fetch, plus
// a second for the bucket straddling the cutoff. A smaller ring is still
// correct, but sends more reservations to carry, which clears later. The cold
// ring covers the window plus the second in progress.
func newLedger(grace, fetchTimeout, coldWindow time.Duration) *ledger {
	window := uint32(coldWindow / time.Second)
	if window < 1 {
		window = 1
	}
	return &ledger{
		reservations: newRing(uint((grace+fetchTimeout)/time.Second) + 2),
		cold:         newRing(uint(window) + 1),
		coldWindow:   window,
	}
}

// ring is a power-of-two ring of one-second buckets, each an atomic word
// packing the second it counts with the count.
type ring struct {
	buckets []atomic.Uint64
	mask    uint32
}

// newRing builds a ring covering at least seconds, and never fewer than 8.
func newRing(seconds uint) ring {
	size := uint32(1) << bits.Len(seconds-1)
	if size < 8 {
		size = 8
	}
	return ring{buckets: make([]atomic.Uint64, size), mask: size - 1}
}

// add counts one in the bucket for sec. Whatever the ring cannot keep is
// returned as a count with its latest second: the bucket's previous contents
// when the slot is reused for sec, or this one itself when the slot has
// already moved past sec because the clock stepped back or the caller stalled
// for a whole ring.
func (r *ring) add(sec uint32) (spilledSec, spilled uint32) {
	b := &r.buckets[sec&r.mask]
	for {
		old := b.Load()
		e, c := unpack(old)
		switch {
		case e == sec:
			if b.CompareAndSwap(old, pack(sec, c+1)) {
				return 0, 0
			}
		case e > sec:
			return sec, 1
		default:
			if b.CompareAndSwap(old, pack(sec, 1)) {
				return e, c
			}
		}
	}
}

// sumSince is the count across buckets for second since and later.
func (r *ring) sumSince(since uint32) int {
	n := 0
	for i := range r.buckets {
		if e, c := unpack(r.buckets[i].Load()); c > 0 && e >= since {
			n += int(c)
		}
	}
	return n
}

// epoch is the one-second bucket t falls in. Seconds since the Unix epoch fit
// in 32 bits until 2106.
func epoch(t time.Time) uint32 { return uint32(t.Unix()) }

// pack and unpack split a word into two 32-bit halves: (second, count) for a
// bucket or carry, (warm, outstanding) for state.
func pack(hi, lo uint32) uint64 { return uint64(hi)<<32 | uint64(lo) }

func unpack(w uint64) (hi, lo uint32) { return uint32(w >> 32), uint32(w) }

// setWarm records the report's warm capacity, keeping outstanding reservations.
func (l *ledger) setWarm(n int) {
	warm := uint32(0)
	switch {
	case int64(n) > math.MaxUint32:
		warm = math.MaxUint32
	case n > 0:
		warm = uint32(n)
	}
	for {
		old := l.state.Load()
		_, out := unpack(old)
		if l.state.CompareAndSwap(old, pack(warm, out)) {
			return
		}
	}
}

// reserved is the number of reservations outstanding.
func (l *ledger) reserved() int {
	_, out := unpack(l.state.Load())
	return int(out)
}

// tryReserve claims one warm sandbox if the report has any left unreserved.
func (l *ledger) tryReserve(now time.Time) bool {
	for {
		old := l.state.Load()
		warm, out := unpack(old)
		if out >= warm {
			return false
		}
		if l.state.CompareAndSwap(old, pack(warm, out+1)) {
			break
		}
	}
	l.record(epoch(now))
	return true
}

// record counts one reservation in the bucket for sec, carrying whatever the
// ring cannot keep.
func (l *ledger) record(sec uint32) {
	if e, c := l.reservations.add(sec); c > 0 {
		l.addCarry(e, c)
	}
}

// recordCold counts one cold placement. Nothing is carried: by the time the
// ring reuses a bucket, its second is outside the window.
func (l *ledger) recordCold(now time.Time) { l.cold.add(epoch(now)) }

// inFlightCold is the number of cold placements made within the window.
func (l *ledger) inFlightCold(now time.Time) int {
	return l.cold.sumSince(epoch(now) - l.coldWindow + 1)
}

func (l *ledger) addCarry(sec, n uint32) {
	for {
		old := l.carry.Load()
		e, c := unpack(old)
		if sec > e {
			e = sec
		}
		if l.carry.CompareAndSwap(old, pack(e, c+n)) {
			return
		}
	}
}

// clearBefore drops reservations made before cutoff, which a report fetched
// after cutoff already accounts for. A bucket is dropped only once all of its
// second is before cutoff, and carry only once its latest second is; anything
// straddling is kept until the next poll.
func (l *ledger) clearBefore(cutoff time.Time) {
	sec := epoch(cutoff)
	var freed uint32
	drop := func(w *atomic.Uint64) {
		for {
			old := w.Load()
			e, c := unpack(old)
			if c == 0 || e >= sec {
				return
			}
			if w.CompareAndSwap(old, 0) {
				freed += c
				return
			}
		}
	}
	for i := range l.reservations.buckets {
		drop(&l.reservations.buckets[i])
	}
	drop(&l.carry)
	if freed == 0 {
		return
	}
	for {
		// Never below zero: every count in a bucket was first added here.
		old := l.state.Load()
		warm, out := unpack(old)
		if l.state.CompareAndSwap(old, pack(warm, out-freed)) {
			return
		}
	}
}

// available is the report's warm capacity less outstanding reservations.
func (l *ledger) available() int {
	warm, out := unpack(l.state.Load())
	if out >= warm {
		return 0
	}
	return int(warm - out)
}
