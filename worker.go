package evpanda

// The worker is the SDK's only background goroutine. It owns every flush:
// one select loop waits on four things — the flush interval, a size
// trigger a producer raised, an explicit Flush request, and the quit
// signal — so flushes are serialized by construction rather than by a
// lock, and an idle SDK costs one sleeping goroutine.
//
// The worker is also the producer chokepoint. captureOCPI / captureOCPP
// run the pure prepareOCPI / prepareOCPP helpers at the bottom of the
// file, which are the single place a message is validated, capped, and
// redacted before it reaches the queue.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrDrainIncomplete is returned by Close and Shutdown when the drain
// deadline elapses with messages still buffered, meaning some captured
// data was dropped on shutdown.
var ErrDrainIncomplete = errors.New("evpanda: close drain deadline exceeded with messages still buffered")

// batchCap is the ingestion API's per-request maximum and, equally, the
// size-based flush trigger.
const batchCap = 1000

// ocpiRedactor and ocppRedactor are the pure transforms applied to a
// message right before enqueue. The concrete redactors live with their
// protocol (ocpi_redact.go / ocpp_redact.go); the worker only needs the
// contract.
type (
	ocpiRedactor func(ocpiMessage) ocpiMessage
	ocppRedactor func(ocppMessage) ocppMessage
)

type worker struct {
	buffer    *ringBuffer
	transport *transport
	cfg       resolvedConfig
	stats     *stats

	// lastReport is the counter snapshot the previous health line
	// reported. Only the loop goroutine touches it, so it needs no lock.
	lastReport Stats

	// wake carries the size trigger. It has one slot and producers send
	// non-blocking, so a burst collapses into a single flush.
	wake chan struct{}
	// flushReq carries an explicit Flush; the worker closes the acked
	// channel once that flush has settled.
	flushReq chan chan struct{}
	// quit stops the loop; done is closed when the loop has returned.
	quit chan struct{}
	done chan struct{}

	// cancel aborts the loop's in-flight POST when a shutdown deadline
	// expires. The context it belongs to is passed into loop rather than
	// stored here — a struct is not where a Context belongs.
	cancel context.CancelFunc

	stopOnce  sync.Once
	closeOnce sync.Once
	closeErr  error
}

func newWorker(b *ringBuffer, t *transport, c resolvedConfig, st *stats) *worker {
	return &worker{
		buffer:    b,
		transport: t,
		cfg:       c,
		stats:     st,
		wake:      make(chan struct{}, 1),
		flushReq:  make(chan chan struct{}),
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// start launches the loop goroutine, handing it the context that bounds
// its deliveries and keeping only the cancel func to stop them.
func (w *worker) start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	go w.loop(ctx)
}

func (w *worker) loop(ctx context.Context) {
	defer close(w.done)
	// The SDK's only goroutine, and the host does not own it — an
	// unrecovered panic here would take their process down with it, which
	// is exactly what the SDK exists not to do. Delivery stops, the loss
	// is counted, and it is the one thing logged at Error level; a client
	// that panicked its worker is broken and should be noisy about it.
	defer func() {
		if r := recover(); r != nil {
			w.stats.countDrop(dropFault, 1)
			if w.cfg.logger != nil {
				w.cfg.logger.Error("evpanda: delivery stopped by a panic — please report this",
					"error", fmt.Sprint(r))
			}
		}
	}()

	timer := time.NewTimer(w.cfg.flushInterval)
	defer timer.Stop()

	// The health line rides the same goroutine, so reporting costs no
	// extra scheduling and can never overlap a flush.
	report := time.NewTicker(reportInterval)
	defer report.Stop()

	// Every flush restarts the interval: whatever the reason, the buffer
	// is empty afterwards, so a tick that was already pending would be a
	// no-op. (Go 1.23+ makes Stop-then-Reset safe without draining.)
	flush := func() {
		w.flush(ctx)
		timer.Stop()
		timer.Reset(w.cfg.flushInterval)
	}

	for {
		select {
		case <-w.quit:
			return

		case <-report.C:
			w.reportHealth()

		case <-timer.C:
			flush()

		case <-w.wake:
			flush()

		case ack := <-w.flushReq:
			flush()
			close(ack)
		}
	}
}

// captureOCPI is the producer entry point for OCPI; see prepareOCPI.
func (w *worker) captureOCPI(msg ocpiMessage, redact ocpiRedactor) {
	env, reason := prepareOCPI(msg, redact, w.cfg.maxCaptureBytes)
	if reason != dropNone {
		w.stats.countDrop(reason, 1)
		return
	}
	w.enqueue(env)
}

// captureOCPP is the producer entry point for OCPP; see prepareOCPP.
func (w *worker) captureOCPP(msg ocppMessage, redact ocppRedactor) {
	env, reason := prepareOCPP(msg, redact, w.cfg.maxCaptureBytes)
	if reason != dropNone {
		w.stats.countDrop(reason, 1)
		return
	}
	w.enqueue(env)
}

// enqueue buffers the envelope and raises the size trigger once a full
// batch is waiting. The send is non-blocking, so a producer never waits
// on the worker.
func (w *worker) enqueue(env bufferedMessage) {
	if w.buffer.enqueue(env) < batchCap {
		return
	}
	select {
	case w.wake <- struct{}{}:
	default: // a trigger is already pending; one flush covers both
	}
}

// flushOnce asks the worker for a flush and waits for it to settle. A
// concurrent caller queues behind the in-flight flush rather than
// starting a second one. It returns immediately once the worker has
// stopped — close owns the final drain from that point on.
func (w *worker) flushOnce() {
	ack := make(chan struct{})
	select {
	case w.flushReq <- ack:
		select {
		case <-ack:
		case <-w.done:
		}
	case <-w.done:
	}
}

// close stops the loop and drains what is left, bounded by ctx. It is
// idempotent: later calls return the first call's result.
func (w *worker) close(ctx context.Context) error {
	w.closeOnce.Do(func() { w.closeErr = w.shutdown(ctx) })
	return w.closeErr
}

func (w *worker) shutdown(ctx context.Context) (err error) {
	defer w.cancel()
	defer w.transport.close()
	// Named return so the summary reports the drain outcome. It runs
	// before the two defers above, while ctx and the transport are still
	// live.
	defer func() { w.reportShutdown(ctx, err) }()

	w.stopOnce.Do(func() { close(w.quit) })

	// Wait for the loop to return, which also waits out a flush it has in
	// flight. Past the deadline, cancelling the loop aborts that POST so the
	// goroutine cannot outlive this call.
	select {
	case <-w.done:
	case <-ctx.Done():
		w.cancel()
		<-w.done
		return ErrDrainIncomplete
	}

	// The loop is gone, so the drain runs here, on the caller's goroutine.
	for w.buffer.length() > 0 {
		if ctx.Err() != nil {
			return ErrDrainIncomplete
		}
		w.flush(ctx)
	}
	return nil
}

// flush drains the buffer and hands it to the transport in batchCap-sized
// requests. The transport owns retry; the worker sends once and moves on.
func (w *worker) flush(ctx context.Context) {
	batch := w.buffer.drain()
	if len(batch) == 0 {
		return
	}
	// A client serves one protocol, so the whole batch goes to one route.
	for i := 0; i < len(batch); i += batchCap {
		w.transport.send(ctx, w.cfg.protocol, batch[i:min(i+batchCap, len(batch))])
	}
}

// ── Producer chokepoints ─────────────────────────────────────────────────
//
// The one place messages are validated, capped, and redacted before the
// queue. Pure: they return the envelope to enqueue, or false to drop.
// Callers go through captureOCPI / captureOCPP.

// prepareOCPI validates the identity, enforces the body cap, and redacts.
// An oversize body on either side drops the whole message — half a body
// is broken JSON, and it would defeat the credentials redactor.
//
// It returns dropNone when the envelope is good; any other reason names
// the counter the drop belongs to.
func prepareOCPI(msg ocpiMessage, redact ocpiRedactor, maxCaptureBytes int) (bufferedMessage, dropReason) {
	if !msg.Identity.Valid() {
		return bufferedMessage{}, dropInvalidIdentity
	}
	if len(msg.Data.RequestBody) > maxCaptureBytes || len(msg.Data.ResponseBody) > maxCaptureBytes {
		return bufferedMessage{}, dropOversize
	}
	// Take ownership before redacting. From here the bytes are the SDK's,
	// so a redactor may rewrite a body in place, and the host may reuse
	// its own buffer the moment the capture call returns.
	msg.Data.own()
	if redact != nil {
		msg = redact(msg)
	}
	return bufferedMessage{capturedAt: nowISO(), message: msg}, dropNone
}

// prepareOCPP validates the identity and enforces the frame cap. A
// message event with no frame or no direction is dropped: the ingestion
// contract requires both on event_type 2.
func prepareOCPP(msg ocppMessage, redact ocppRedactor, maxCaptureBytes int) (bufferedMessage, dropReason) {
	if !msg.Identity.Valid() {
		return bufferedMessage{}, dropInvalidIdentity
	}
	if len(msg.Payload) > maxCaptureBytes {
		return bufferedMessage{}, dropOversize
	}
	if msg.EventType == ocppEventTypeMessage && (len(msg.Payload) == 0 || msg.Direction == "") {
		return bufferedMessage{}, dropOversize
	}
	msg.Payload = cloneBody(msg.Payload) // same ownership transfer as OCPI
	// nil is the normal case for OCPP: there is nothing to redact, so
	// there is no redactor rather than one that does nothing.
	if redact != nil {
		msg = redact(msg)
	}
	return bufferedMessage{capturedAt: nowISO(), message: msg}, dropNone
}
