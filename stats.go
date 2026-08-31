package evpanda

// Delivery counters. The SDK discards data in five places by design, and
// four of them are otherwise invisible: a customer whose identity
// resolution is misconfigured sees no traffic and no explanation. These
// counters are what make the best-effort trade auditable.
//
// They are always on — there is no log level that turns them off, because
// an atomic add nobody reads costs nothing and the alternative is a
// support conversation that cannot be answered.

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// Stats is a point-in-time snapshot of one client's delivery counters.
// The Dropped* fields are monotonic totals since the client started;
// Buffered* are instantaneous.
//
// Each counter maps to exactly one root cause, which is what makes them
// worth reading during an integration problem:
//
//	Captured 0                  the capture path is not wired in
//	DroppedInvalid high         identity resolution is failing — for the
//	                            adapters, usually mounted before the
//	                            host's auth layer
//	DroppedOversize high        bodies exceed MaxCaptureBytes
//	DroppedEvicted high         upstream can't keep up, or the buffer is
//	                            undersized for the traffic
//	DroppedUndeliverable high   network, API key, or ingestion fault
//	DroppedFault > 0            a bug in the SDK; please report it
type Stats struct {
	// Captured is the number of messages that passed the chokepoint and
	// entered the buffer.
	Captured uint64
	// DroppedInvalid counts messages whose identity failed validation.
	DroppedInvalid uint64
	// DroppedOversize counts messages whose body or frame exceeded
	// MaxCaptureBytes, or which lacked a field the wire contract requires.
	DroppedOversize uint64
	// DroppedEvicted counts messages evicted from the buffer under
	// pressure, oldest first.
	DroppedEvicted uint64
	// DroppedUndeliverable counts messages in batches the transport could
	// not deliver — retries exhausted, or a permanent rejection.
	DroppedUndeliverable uint64
	// DroppedFault counts captures lost to a recovered panic. Any value
	// above zero is a bug in the SDK.
	//
	// It is named for the concept rather than the mechanism so the three
	// SDKs report the same counter: Node and Python have exceptions where
	// Go has panics, and an operator reading a dashboard should not have to
	// know which runtime produced the number.
	DroppedFault uint64

	// BodiesDropped counts payloads omitted because they were not valid
	// UTF-8, which the wire contract requires.
	//
	// It is not part of TotalDropped, because it does not count lost
	// messages: an OCPI exchange still ships without the offending body,
	// carrying its method, URL, status and headers. An OCPP frame is the
	// exception, since event_type 2 requires one, so that message is
	// dropped as well and counted in DroppedOversize.
	//
	// Both protocols are JSON over UTF-8, so any value above zero means
	// something upstream is sending payloads the protocol does not allow.
	BodiesDropped uint64

	// BufferedMessages is how many messages are awaiting delivery now.
	BufferedMessages int
	// BufferBytes is their accounted footprint, always at or below
	// MaxBufferBytes.
	BufferBytes int
}

// TotalDropped is the sum of every Dropped* counter.
func (s Stats) TotalDropped() uint64 {
	return s.DroppedInvalid + s.DroppedOversize + s.DroppedEvicted +
		s.DroppedUndeliverable + s.DroppedFault
}

// stats is the live counter set, shared by the chokepoint, the buffer and
// the transport. Safe for concurrent use.
type stats struct {
	captured             atomic.Uint64
	droppedInvalid       atomic.Uint64
	droppedOversize      atomic.Uint64
	droppedEvicted       atomic.Uint64
	droppedUndeliverable atomic.Uint64
	droppedFault         atomic.Uint64
	bodiesDropped        atomic.Uint64
}

// snapshot reads the counters. The reads are not atomic as a set, so a
// snapshot taken mid-flight can be internally inconsistent by a message
// or two — which is fine for the rate reporting and monitoring this
// feeds, and not worth a lock on the capture path to avoid.
func (s *stats) snapshot() Stats {
	if s == nil {
		return Stats{}
	}
	return Stats{
		Captured:             s.captured.Load(),
		DroppedInvalid:       s.droppedInvalid.Load(),
		DroppedOversize:      s.droppedOversize.Load(),
		DroppedEvicted:       s.droppedEvicted.Load(),
		DroppedUndeliverable: s.droppedUndeliverable.Load(),
		DroppedFault:         s.droppedFault.Load(),
		BodiesDropped:        s.bodiesDropped.Load(),
	}
}

// Both counter methods are nil-safe, so a component can hold a nil
// *stats — which keeps unit tests free of bookkeeping they don't assert.

func (s *stats) countCaptured() {
	if s != nil {
		s.captured.Add(1)
	}
}

// countBodiesDropped charges n omitted bodies. It is separate from
// countDrop because the reasons there all cost a whole message.
func (s *stats) countBodiesDropped(n int) {
	if s != nil && n > 0 {
		s.bodiesDropped.Add(uint64(n))
	}
}

// countDrop charges n messages to the counter for reason. It takes a
// count because the transport loses a whole batch at once.
func (s *stats) countDrop(reason dropReason, n int) {
	if s == nil || n <= 0 {
		return
	}
	if c := s.counterFor(reason); c != nil {
		c.Add(uint64(n))
	}
}

// counterFor is the single mapping from a drop reason to its counter.
func (s *stats) counterFor(reason dropReason) *atomic.Uint64 {
	switch reason {
	case dropInvalidIdentity:
		return &s.droppedInvalid
	case dropOversize:
		return &s.droppedOversize
	case dropEvicted:
		return &s.droppedEvicted
	case dropUndeliverable:
		return &s.droppedUndeliverable
	case dropFault:
		return &s.droppedFault
	case dropNone:
		return nil
	}
	return nil
}

// sub returns the counters accumulated between prev and s. Only the
// monotonic fields are differenced; the buffer gauges are carried across
// from s, since a delta of an instantaneous value is meaningless.
func (s Stats) sub(prev Stats) Stats {
	return Stats{
		Captured:             s.Captured - prev.Captured,
		DroppedInvalid:       s.DroppedInvalid - prev.DroppedInvalid,
		DroppedOversize:      s.DroppedOversize - prev.DroppedOversize,
		DroppedEvicted:       s.DroppedEvicted - prev.DroppedEvicted,
		DroppedUndeliverable: s.DroppedUndeliverable - prev.DroppedUndeliverable,
		DroppedFault:         s.DroppedFault - prev.DroppedFault,
		BodiesDropped:        s.BodiesDropped - prev.BodiesDropped,
		BufferedMessages:     s.BufferedMessages,
		BufferBytes:          s.BufferBytes,
	}
}

// logAttrs renders the snapshot as slog key/value pairs, omitting the
// counters that are zero. Keeping the line to what actually happened is
// what makes it readable at a glance in a production log.
func (s Stats) logAttrs() []any {
	attrs := make([]any, 0, 16)
	add := func(k string, v uint64) {
		if v != 0 {
			attrs = append(attrs, k, v)
		}
	}
	add("captured", s.Captured)
	add("invalid_identity", s.DroppedInvalid)
	add("oversize", s.DroppedOversize)
	add("evicted", s.DroppedEvicted)
	add("undeliverable", s.DroppedUndeliverable)
	add("fault", s.DroppedFault)
	add("bodies_dropped", s.BodiesDropped)
	return append(attrs, "buffered", s.BufferedMessages, "buffer_bytes", s.BufferBytes)
}

// dropReason is the single taxonomy of why a message was lost. The
// chokepoint returns one (and only ever uses the first three); the
// buffer, the transport and the panic guard charge theirs directly.
type dropReason int

const (
	dropNone dropReason = iota
	dropInvalidIdentity
	dropOversize
	dropEvicted
	dropUndeliverable
	dropFault
)

// ── Health reporting ─────────────────────────────────────────────────────

// reportInterval bounds how often the health line can appear. It is a
// ceiling on log volume, not a sampling rate: the line reports everything
// that happened in the window, so nothing is hidden by the delay.
//
// This is why drops are summarized rather than logged per event. The
// common integration fault — an adapter mounted before the host's auth
// layer — drops on every single request, so per-event logging would emit
// at request rate and cost the host real money in log ingestion.
const reportInterval = time.Minute

// snapshot is the counters plus the live buffer gauges — the one place
// the two halves of [Stats] are put together, shared by Stats() and both
// reporters.
func (w *worker) snapshot() Stats {
	s := w.stats.snapshot()
	s.BufferedMessages = w.buffer.length()
	s.BufferBytes = w.buffer.byteLen()
	return s
}

// reportHealth emits at most one line per reportInterval, and only when
// something was dropped in that window. A healthy client is silent.
func (w *worker) reportHealth() {
	if w.cfg.logger == nil {
		return
	}
	cur := w.snapshot()
	delta := cur.sub(w.lastReport)
	w.lastReport = cur

	if delta.TotalDropped() == 0 {
		return
	}
	w.cfg.logger.Warn("evpanda: captures dropped",
		append([]any{"window", reportInterval.String()}, delta.logAttrs()...)...)
}

// reportShutdown logs the client's lifetime totals as it closes. In
// LogModeDebug it always logs; otherwise only when something was dropped,
// so a clean run leaves no trace.
func (w *worker) reportShutdown(ctx context.Context, drainErr error) {
	if w.cfg.logger == nil {
		return
	}
	total := w.snapshot()

	if total.TotalDropped() == 0 && drainErr == nil && w.cfg.logMode != LogModeDebug {
		return
	}
	attrs := total.logAttrs()
	if drainErr != nil {
		attrs = append(attrs, "drain", drainErr.Error())
	}
	level := slog.LevelWarn
	if total.TotalDropped() == 0 && drainErr == nil {
		level = slog.LevelInfo
	}
	w.cfg.logger.Log(ctx, level, "evpanda: client closed", attrs...)
}
