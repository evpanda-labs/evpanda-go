package evpanda

// Byte-bounded, drop-oldest queue. It holds no I/O: drain copies the live
// messages out and resets; the worker does the POST.
//
// The bound is bytes rather than a slot count because captured messages
// vary by two orders of magnitude — an OCPP heartbeat is a few hundred
// bytes, an OCPI CDR batch can be tens of kilobytes. A slot count is a
// proxy for the thing an operator actually has to provision, and a poor
// one: the same 10 000 slots is ~3 MiB of OCPP heartbeats or 625 MiB of
// capped bodies. MaxBufferBytes is that number directly.
//
// The backing slice grows on demand, so a low-traffic host holds a small
// buffer rather than pre-allocating for its ceiling.

import (
	"errors"
	"sync"
	"time"
)

// Per-message accounting overhead, deliberately generous: the envelope,
// the interface box, struct fields, string and slice headers, and the
// slot itself. Over-counting keeps the configured budget a true ceiling —
// under-counting would quietly break it.
const (
	envelopeOverhead    = 256
	headerEntryOverhead = 48
)

// bufferedMessage is the internal envelope: the SDK-stamped receive time,
// the captured message, and its accounted footprint.
type bufferedMessage struct {
	capturedAt string
	message    message
	// size is filled in by enqueue; producers never set it.
	size int
}

// ringBuffer is a byte-bounded, drop-oldest queue, safe for concurrent
// use. Producers enqueue from any goroutine; the worker drains.
//
// buf[head:] are the live messages, oldest first.
type ringBuffer struct {
	mu       sync.Mutex
	buf      []bufferedMessage
	head     int
	bytes    int
	maxBytes int
	// stats counts evictions; eviction is otherwise invisible, and it is
	// the one drop that means data is being lost right now.
	stats *stats
}

func newRingBuffer(maxBytes int, st *stats) (*ringBuffer, error) {
	if maxBytes < 1 {
		return nil, errors.New("evpanda: buffer byte budget must be a positive integer")
	}
	return &ringBuffer{maxBytes: maxBytes, stats: st}, nil
}

// enqueue appends a message, evicting the oldest until it fits, and
// returns the resulting message count. It never blocks and never grows
// past the budget.
//
// A message larger than the whole budget is dropped outright rather than
// emptying the buffer for something that still would not fit. The
// chokepoint's MaxCaptureBytes cap makes that unreachable unless the two
// are misconfigured relative to each other, which config resolution warns
// about.
func (r *ringBuffer) enqueue(env bufferedMessage) int {
	env.size = env.message.size()

	r.mu.Lock()
	defer r.mu.Unlock()

	if env.size > r.maxBytes {
		r.stats.countDrop(dropOversize, 1)
		return len(r.buf) - r.head
	}
	for r.bytes+env.size > r.maxBytes {
		r.evictOldest()
	}
	r.compact()
	r.buf = append(r.buf, env)
	r.bytes += env.size
	r.stats.countCaptured()
	return len(r.buf) - r.head
}

// evictOldest drops the front message. The caller holds the lock, and the
// enqueue loop only calls it while the budget is still exceeded — which
// cannot be true of an empty buffer.
func (r *ringBuffer) evictOldest() {
	r.bytes -= r.buf[r.head].size
	r.buf[r.head] = bufferedMessage{} // release the reference
	r.head++
	r.stats.countDrop(dropEvicted, 1)
}

// compact slides the live messages to the front once the evicted prefix
// is half the slice, so eviction-heavy traffic cannot grow the backing
// array without bound. Amortized O(1) per enqueue.
func (r *ringBuffer) compact() {
	if r.head == 0 || r.head*2 < len(r.buf) {
		return
	}
	n := copy(r.buf, r.buf[r.head:])
	clear(r.buf[n:])
	r.buf = r.buf[:n]
	r.head = 0
}

// drain removes and returns all buffered messages, oldest first.
func (r *ringBuffer) drain() []bufferedMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := len(r.buf) - r.head
	if n == 0 {
		return nil
	}
	out := make([]bufferedMessage, n)
	copy(out, r.buf[r.head:])
	clear(r.buf)      // release every reference the slots still hold
	r.buf = r.buf[:0] // keep the capacity for reuse
	r.head = 0
	r.bytes = 0
	return out
}

// length returns the number of buffered messages.
func (r *ringBuffer) length() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.buf) - r.head
}

// byteLen returns the accounted footprint of everything buffered. It is
// always ≤ the configured budget.
func (r *ringBuffer) byteLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bytes
}

// ── Per-message accounting ───────────────────────────────────────────────
//
// size lives here rather than with the types because it is buffer
// bookkeeping, the same way record lives in transport.go with the wire.

// size reports the accounted footprint of an OCPI capture.
func (m ocpiMessage) size() int {
	return envelopeOverhead +
		len(m.Direction) +
		len(m.Identity.ID) + len(m.Identity.Name) +
		len(m.Identity.TenantID) + len(m.Identity.TenantName) +
		len(m.Data.Method) + len(m.Data.URL) +
		len(m.Data.RequestBody) + len(m.Data.ResponseBody) +
		headerSize(m.Data.RequestHeaders) + headerSize(m.Data.ResponseHeaders)
}

// size reports the accounted footprint of an OCPP capture.
func (m ocppMessage) size() int {
	return envelopeOverhead +
		len(m.Identity.ID) +
		len(m.Identity.TenantID) + len(m.Identity.TenantName) +
		len(m.ConnectionID) + len(m.Direction) + len(m.Payload)
}

// headerSize charges each entry its key and value plus a share of the
// map's own bucket overhead.
func headerSize(h map[string]string) int {
	n := 0
	for k, v := range h {
		n += len(k) + len(v) + headerEntryOverhead
	}
	return n
}

// timestampLayout is RFC3339 with millisecond precision and a UTC "Z",
// matching the captured_at example in the ingestion spec.
const timestampLayout = "2006-01-02T15:04:05.000Z07:00"

// nowISO returns the current time as a wire timestamp.
func nowISO() string {
	return time.Now().UTC().Format(timestampLayout)
}
