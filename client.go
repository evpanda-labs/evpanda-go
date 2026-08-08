package evpanda

import (
	"fmt"
	"log/slog"
	"sync"
)

// engine owns the delivery pipeline for one client: the worker (with its
// ring buffer and transport) plus the resolved capture settings.
type engine struct {
	worker          *worker
	maxCaptureBytes int
	// logger is the effective debug logger; nil means silent.
	logger *slog.Logger
}

// newEngine builds and starts the pipeline for a resolved config.
func newEngine(resolved resolvedConfig) (*engine, error) {
	buffer, err := newRingBuffer(resolved.bufferCapacity)
	if err != nil {
		return nil, err
	}
	w := newWorker(buffer, newTransport(resolved), resolved)
	w.start()
	return &engine{
		worker:          w,
		maxCaptureBytes: resolved.maxCaptureBytes,
		logger:          resolved.logger,
	}, nil
}

// client is the shared lifecycle core embedded by [OCPIClient] and
// [OCPPClient]: it holds the engine and swaps it for nil (inert) on
// Close. A nil engine makes every capture a no-op.
type client struct {
	mu  sync.RWMutex
	eng *engine
}

// current returns the live engine, or nil when the client is inert.
func (c *client) current() *engine {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.eng
}

// Flush triggers immediate delivery of buffered messages. It never
// panics; the returned error is non-nil only if an internal panic was
// recovered. Transport delivery failures are retried and dropped by
// design and are not reported here.
func (c *client) Flush() error {
	return guard("Flush", func() error {
		if eng := c.current(); eng != nil {
			eng.worker.flushOnce()
		}
		return nil
	})
}

// Close stops capture and drains buffered messages within DrainTimeout.
// It is idempotent and never panics. It returns [ErrDrainIncomplete] if
// the deadline elapsed with messages still buffered, a wrapped error if
// an internal panic was recovered, or nil on a clean drain.
func (c *client) Close() error {
	return guard("Close", func() error {
		c.mu.Lock()
		eng := c.eng
		c.eng = nil
		c.mu.Unlock()
		if eng == nil {
			return nil
		}
		return eng.worker.close(0)
	})
}

// logFault surfaces a swallowed capture fault when a debug logger is
// configured.
func (c *client) logFault(op string, r any) {
	eng := c.current()
	if eng == nil || eng.logger == nil {
		return
	}
	eng.logger.Warn("evpanda: capture failed", "op", op, "error", fmt.Sprint(r))
}

// guard runs fn and converts a recovered panic into an error so the SDK
// never panics into the caller.
func guard(op string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("evpanda: recovered panic in %s: %v", op, r)
		}
	}()
	return fn()
}

// guardCapture runs a capture path, swallowing any panic and logging it
// when a debug logger is configured — the SDK never panics into the host.
func (c *client) guardCapture(op string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			// logFault itself must not panic the host either.
			defer func() { _ = recover() }()
			c.logFault(op, r)
		}
	}()
	fn()
}

// enqueue stamps the capture time and buffers the message.
func (e *engine) enqueue(msg anyMessage) {
	e.worker.buffer.enqueue(bufferedMessage{capturedAt: nowISO(), message: msg})
}
