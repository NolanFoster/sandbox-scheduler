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

	// buckets is a power-of-two ring indexed by epoch second.
	buckets []atomic.Uint64
	mask    uint32

	// carry holds reservations whose bucket was reused for a later second
	// before any poll cleared them, packed with the latest second among them.
	// Without it, a provider whose polls kept failing would forget its
	// reservations after one trip round the ring, and its stale report would
	// overstate capacity.
	carry atomic.Uint64
}

// newLedger sizes the ring to outlast the longest a reservation should wait
// for a poll that clears it: the grace period plus one fetch, plus a second
// for the bucket straddling the cutoff. A smaller ring is still correct, but
// sends more reservations to carry, which clears later.
func newLedger(grace, fetchTimeout time.Duration) *ledger {
	need := uint((grace+fetchTimeout)/time.Second) + 2
	size := uint32(1) << bits.Len(need-1)
	if size < 8 {
		size = 8
	}
	return &ledger{buckets: make([]atomic.Uint64, size), mask: size - 1}
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

// record counts one reservation in the bucket for sec.
func (l *ledger) record(sec uint32) {
	b := &l.buckets[sec&l.mask]
	for {
		old := b.Load()
		e, c := unpack(old)
		switch {
		case e == sec:
			if b.CompareAndSwap(old, pack(sec, c+1)) {
				return
			}
		case e > sec:
			// The slot has moved on to a later second: the clock stepped back,
			// or this goroutine stalled for a whole ring. Carry it instead.
			l.addCarry(sec, 1)
			return
		default:
			if b.CompareAndSwap(old, pack(sec, 1)) {
				if c > 0 {
					l.addCarry(e, c)
				}
				return
			}
		}
	}
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
	for i := range l.buckets {
		drop(&l.buckets[i])
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
