package evpanda

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrDrainIncomplete is returned by [Client.Close] when the drain deadline
// elapses with messages still buffered, meaning some captured data was
// dropped on shutdown.
var ErrDrainIncomplete = errors.New("evpanda: close drain deadline exceeded with messages still buffered")

// batchCap is the maximum records per request and the size-based flush
// trigger.
const batchCap = 1000

// pollInterval is how often the worker checks the size-based flush trigger.
const pollInterval = 200 * time.Millisecond

type worker struct {
	buffer    *ringBuffer
	transport *transport
	config    resolvedConfig

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{} // closed when the loop goroutine exits

	// flushMu serializes flushes so they never overlap.
	flushMu sync.Mutex

	lastMu    sync.Mutex
	lastFlush time.Time
}

func newWorker(b *ringBuffer, t *transport, c resolvedConfig) *worker {
	return &worker{
		buffer:    b,
		transport: t,
		config:    c,
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// start launches the polling goroutine.
func (w *worker) start() {
	w.setLastFlush(time.Now())
	go w.loop()
}

func (w *worker) loop() {
	defer close(w.doneCh)
	t := time.NewTicker(pollInterval)
	defer t.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-t.C:
			if w.shouldFlush() {
				w.flushOnce()
			}
		}
	}
}

// flushOnce runs one flush, serialized so it never overlaps another.
func (w *worker) flushOnce() {
	w.flushMu.Lock()
	defer w.flushMu.Unlock()
	w.runFlush(context.Background())
}

// stop halts the polling loop. An in-flight flush is not cancelled; close
// only bounds how long it waits for the loop to exit.
func (w *worker) stop() {
	w.stopOnce.Do(func() { close(w.stopCh) })
}

// close stops the loop and drains the buffer, bounded by deadline (≤ 0
// uses the configured DrainTimeout). It is idempotent and returns
// ErrDrainIncomplete if the deadline elapsed before the buffer emptied.
func (w *worker) close(deadline time.Duration) error {
	select {
	case <-w.stopCh:
		return nil // already closed
	default:
	}
	w.stop()

	if deadline <= 0 {
		deadline = w.config.drainTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	// Wait for the loop to exit (which also waits out an in-flight flush),
	// bounded by the deadline.
	select {
	case <-w.doneCh:
	case <-ctx.Done():
		return ErrDrainIncomplete
	}

	for w.buffer.length() > 0 {
		select {
		case <-ctx.Done():
			return ErrDrainIncomplete
		default:
		}
		w.runFlush(ctx)
	}
	return nil
}

func (w *worker) setLastFlush(t time.Time) {
	w.lastMu.Lock()
	w.lastFlush = t
	w.lastMu.Unlock()
}

func (w *worker) shouldFlush() bool {
	n := w.buffer.length()
	if n == 0 {
		return false
	}
	w.lastMu.Lock()
	since := time.Since(w.lastFlush)
	w.lastMu.Unlock()
	return n >= batchCap || since >= w.config.flushInterval
}

func (w *worker) runFlush(ctx context.Context) {
	w.setLastFlush(time.Now())
	batch := w.buffer.drain()
	if len(batch) == 0 {
		return
	}

	// A Client serves one protocol, so the whole batch goes to one
	// endpoint, chunked at batchCap.
	protocol := w.config.protocol
	for i := 0; i < len(batch); i += batchCap {
		end := min(i+batchCap, len(batch))
		w.transport.send(ctx, protocol, batch[i:end])
	}
}
