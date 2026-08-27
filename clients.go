package evpanda

// The three clients: the shared lifecycle core, and the two protocol
// front ends built on it.
//
// The protocol is the client — there is no network-type switch.
// [StartOCPI] returns an [OCPIClient], [StartOCPP] an [OCPPClient], and a
// client instance serves exactly one protocol for its whole life. Both
// embed the unexported `client` below, which owns the worker and the
// counters and supplies Stats / Flush / Close / Shutdown.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
)

// ══ Shared lifecycle core ═══════════════════════════════════════════════

// Shared lifecycle for OCPIClient and OCPPClient: hold the worker, drop it
// on Close, and expose Stats / Flush / Close / Shutdown. The
// protocol-specific capture methods stay on the two clients.

// StartOCPI and StartOCPP never return nil, so every method here has a
// live receiver — including on an inert client, whose worker is nil but
// whose struct is not. Calling a method on a client variable that was
// declared and never assigned panics, as it would for any Go type; the
// adapters guard against that separately, since they take an interface
// where a typed nil can slip past an != nil check.
//
// client is the lifecycle core embedded by [OCPIClient] and [OCPPClient].
// It holds the running worker and swaps it for nil on Close; a nil worker
// makes every capture a no-op, which is also how an inert client (one
// built from a bad config) behaves.
type client struct {
	mu sync.RWMutex
	w  *worker
	// stats is held here rather than on the worker so the counters
	// survive Close — the final tally is often the thing worth reading.
	// Nil on an inert client.
	stats *stats
}

// start builds and launches the pipeline, leaving the client live. The
// counter set is created here, outside the worker, so it outlives Close.
func (c *client) start(resolved resolvedConfig) error {
	c.stats = &stats{}
	buffer, err := newRingBuffer(resolved.maxBufferBytes, c.stats)
	if err != nil {
		return err
	}
	w := newWorker(buffer, newTransport(resolved, c.stats), resolved, c.stats)
	w.start()
	c.w = w
	return nil
}

// current returns the running worker, or nil when the client is inert.
func (c *client) current() *worker {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.w
}

// detach takes the worker and leaves the client inert. It returns nil if
// the client was already inert, which is what makes Close idempotent.
func (c *client) detach() *worker {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := c.w
	c.w = nil
	return w
}

// Stats returns a snapshot of this client's delivery counters. It is
// always available: there is no log mode that turns the counters off, and
// it is safe to call on an inert or closed client (a closed one reports
// its final totals with an empty buffer).
//
// Use it to answer "why am I seeing no data?" without a redeploy — each
// counter maps to one root cause, documented on [Stats] — or to feed your
// own metrics system:
//
//	prometheus.MustRegister(prometheus.NewCounterFunc(opts, func() float64 {
//		return float64(panda.Stats().DroppedEvicted)
//	}))
func (c *client) Stats() Stats {
	if w := c.current(); w != nil {
		return w.snapshot()
	}
	return c.stats.snapshot() // inert or closed: counters only, no buffer
}

// CaptureLimits reports the resolved per-body capture cap and whether the
// client is currently capturing. It is false for an inert client (bad
// config) or one that has been closed.
//
// The shipped adapters use it to bound what they accumulate from a
// streaming body and to skip instrumentation entirely when there is
// nothing to capture into. It is exported for the same reason: writing an
// adapter for a framework the SDK does not ship — fasthttp, or a gRPC
// gateway — needs exactly these two facts.
func (c *client) CaptureLimits() (maxCaptureBytes int, active bool) {
	if w := c.current(); w != nil {
		return w.cfg.maxCaptureBytes, true
	}
	return 0, false
}

// Flush delivers everything currently buffered and waits for that
// delivery to settle. It never panics; the returned error is non-nil only
// if an internal panic was recovered. Delivery failures are retried and
// dropped by design and are not reported here.
//
// It blocks for as long as the transport's bounded retry takes, so it is
// a diagnostic and shutdown tool rather than something to call on a hot
// path — capture is already asynchronous.
func (c *client) Flush() error {
	return guard("Flush", func() error {
		if w := c.current(); w != nil {
			w.flushOnce()
		}
		return nil
	})
}

// Close stops capture and drains buffered messages within the configured
// DrainTimeout. It is idempotent and never panics. It returns
// [ErrDrainIncomplete] if the deadline elapsed with messages still
// buffered, a wrapped error if an internal panic was recovered, or nil on
// a clean drain.
//
// Captures made after Close are safe no-ops.
func (c *client) Close() error {
	return guard("Close", func() error {
		w := c.detach()
		if w == nil {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), w.cfg.drainTimeout)
		defer cancel()
		return w.close(ctx)
	})
}

// Shutdown is Close with a caller-supplied deadline: it stops capture and
// drains until ctx expires. Use it when the host already has a shutdown
// context; otherwise use Close, which applies DrainTimeout.
func (c *client) Shutdown(ctx context.Context) error {
	return guard("Shutdown", func() error {
		w := c.detach()
		if w == nil {
			return nil
		}
		return w.close(ctx)
	})
}

// logFault counts a swallowed capture fault and, in LogModeDebug, logs
// it. In the default mode the worker's health line reports it instead: a
// panic that repeats per message would otherwise log at message rate.
func (c *client) logFault(op string, r any) {
	c.stats.countDrop(dropPanic, 1)
	w := c.current()
	if w == nil || w.cfg.logger == nil || w.cfg.logMode != LogModeDebug {
		return
	}
	w.cfg.logger.Warn("evpanda: capture failed", "op", op, "error", fmt.Sprint(r))
}

// guard runs fn and converts a recovered panic into an error, so the SDK
// never panics into the caller.
func guard(op string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("evpanda: recovered panic in %s: %v", op, r)
		}
	}()
	return fn()
}

// guardCapture runs a capture path, swallowing any panic and logging it —
// the SDK never panics into the host, and a capture has no error to
// return.
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

// ══ OCPI ════════════════════════════════════════════════════════════════

// OCPIClient captures and ships OCPI roaming traffic. Build it with
// [StartOCPI]; it is safe for concurrent use.
//
// OCPI traffic flows both ways between roaming partners and the SDK
// records each direction separately. There is no direction argument — the
// method you call stamps it:
//
//   - [OCPIClient.CaptureInboundMessage] — a partner called your OCPI
//     server. You are the server, so you capture the request they sent
//     and the response you returned.
//   - [OCPIClient.CaptureOutboundMessage] — you called a partner's OCPI
//     server. You are the client, so you capture the request you sent and
//     the response they returned.
//
// In both cases Identity is the partner on the other side of the
// exchange, never your own platform.
type OCPIClient struct {
	client
	// redact is the header allowlist and credentials mask. StartOCPI
	// always sets it — a live OCPI client must never have a nil redactor,
	// since the chokepoint reads nil as "nothing to redact".
	redact ocpiRedactor
}

// StartOCPI validates cfg, builds the client, and launches its background
// worker.
//
// It always returns a non-nil, usable *OCPIClient. Endpoint and APIKey are
// hard-required: if either is missing or malformed the returned client is
// an inert no-op and the error describes the problem, so a bad config can
// never crash the host's boot. Every other field is tunable — a bad value
// falls back to its default and is reported through [LogMode].
func StartOCPI(cfg OCPIConfig) (*OCPIClient, error) {
	resolved, err := resolveOCPIConfig(cfg)
	if err != nil {
		return &OCPIClient{}, err
	}
	c := &OCPIClient{redact: defaultOCPIRedactor(resolved.allowedHeaders)}
	if err := c.start(resolved); err != nil {
		return &OCPIClient{}, err
	}
	return c, nil
}

// CaptureInboundMessage buffers an inbound OCPI message (partner → host)
// for delivery. Non-blocking and never panics; a message with an invalid
// identity or an oversize body is silently dropped.
func (c *OCPIClient) CaptureInboundMessage(msg OCPIMessageInput) {
	c.guardCapture("CaptureInboundMessage", func() { c.capture(msg, OCPIInbound) })
}

// CaptureOutboundMessage buffers an outbound OCPI message (host →
// partner) for delivery. Non-blocking and never panics; a message with an
// invalid identity or an oversize body is silently dropped.
func (c *OCPIClient) CaptureOutboundMessage(msg OCPIMessageInput) {
	c.guardCapture("CaptureOutboundMessage", func() { c.capture(msg, OCPIOutbound) })
}

// capture stamps the direction and hands the message to the worker, which
// runs the validate → cap → redact chokepoint.
func (c *OCPIClient) capture(msg OCPIMessageInput, direction OCPIDirection) {
	w := c.current()
	if w == nil {
		return
	}
	w.captureOCPI(ocpiMessage{
		Direction: direction,
		Identity:  msg.Identity,
		Data:      msg.Data,
	}, c.redact)
}

// ══ OCPP ════════════════════════════════════════════════════════════════

// OCPPClient captures and ships OCPP CSMS traffic. Build it with
// [StartOCPP]; it is safe for concurrent use.
//
// There are two ways to capture:
//
//   - [OCPPClient.Connection] — the recommended path: a session handle
//     that owns the connection ID and carries the identity. Attach it to
//     your WebSocket and call Message per frame, Disconnect on close.
//   - [OCPPClient.CaptureConnect] / [OCPPClient.CaptureMessage] /
//     [OCPPClient.CaptureDisconnect] — the flat primitives the session is
//     built on, for one-off capture.
//
// Identity is a [ChargerIdentity] value, not a resolver: OCPP identity is
// known at connect time. An invalid one drops the message.
type OCPPClient struct {
	client
	// redact is nil today: OCPP frames are captured verbatim, and the
	// chokepoint reads nil as "nothing to redact". See redact.go.
	redact ocppRedactor
}

// StartOCPP validates cfg, builds the client, and launches its background
// worker.
//
// It always returns a non-nil, usable *OCPPClient. Endpoint and APIKey are
// hard-required: if either is missing or malformed the returned client is
// an inert no-op and the error describes the problem, so a bad config can
// never crash the host's boot. Every other field is tunable — a bad value
// falls back to its default and is reported through [LogMode].
func StartOCPP(cfg OCPPConfig) (*OCPPClient, error) {
	resolved, err := resolveOCPPConfig(cfg)
	if err != nil {
		return &OCPPClient{}, err
	}
	// redact stays nil: OCPP frames are captured verbatim today.
	c := &OCPPClient{}
	if err := c.start(resolved); err != nil {
		return &OCPPClient{}, err
	}
	return c, nil
}

// OCPPSession is a live capture handle for one OCPP WebSocket connection.
// Returned by [OCPPClient.Connection], it owns the connection ID and the
// identity so per-frame calls carry neither. Attach it to your connection
// object and call Message per frame, Disconnect when the socket closes.
type OCPPSession struct {
	// ConnectionID is the SDK-minted ID for this connection — fresh per
	// Connection call, which is how the ingestion side separates one
	// charger's sessions across reconnects.
	ConnectionID string

	client   *OCPPClient
	identity ChargerIdentity
}

// Connection opens a capture session for one OCPP connection: it mints a
// connection ID, records the connect, and returns an [OCPPSession].
func (c *OCPPClient) Connection(identity ChargerIdentity) *OCPPSession {
	connectionID := newUUID()
	c.CaptureConnect(OCPPMessageInput{Identity: identity, ConnectionID: connectionID})
	return &OCPPSession{ConnectionID: connectionID, client: c, identity: identity}
}

// Message captures one OCPP frame. Oversize frames are dropped.
func (s *OCPPSession) Message(data []byte, direction OCPPDirection) {
	s.client.CaptureMessage(OCPPMessageInput{
		Identity:     s.identity,
		ConnectionID: s.ConnectionID,
		Data:         data,
		Direction:    direction,
	})
}

// Disconnect captures the connection closing.
func (s *OCPPSession) Disconnect() {
	s.client.CaptureDisconnect(OCPPMessageInput{
		Identity:     s.identity,
		ConnectionID: s.ConnectionID,
	})
}

// CaptureConnect records a new OCPP connection. It uses Identity and
// ConnectionID only. Non-blocking and never panics.
func (c *OCPPClient) CaptureConnect(msg OCPPMessageInput) {
	c.guardCapture("CaptureConnect", func() {
		c.capture(ocppMessage{
			EventType:    OCPPEventTypeConnect,
			Identity:     msg.Identity,
			ConnectionID: msg.ConnectionID,
		})
	})
}

// CaptureMessage records one OCPP frame. It requires Data and Direction
// and drops the message if either is missing; oversize frames are dropped
// too. Non-blocking and never panics.
func (c *OCPPClient) CaptureMessage(msg OCPPMessageInput) {
	c.guardCapture("CaptureMessage", func() {
		c.capture(ocppMessage{
			EventType:    OCPPEventTypeMessage,
			Identity:     msg.Identity,
			ConnectionID: msg.ConnectionID,
			Direction:    msg.Direction,
			Payload:      msg.Data,
		})
	})
}

// CaptureDisconnect records the connection closing. It uses Identity and
// ConnectionID only. Non-blocking and never panics.
func (c *OCPPClient) CaptureDisconnect(msg OCPPMessageInput) {
	c.guardCapture("CaptureDisconnect", func() {
		c.capture(ocppMessage{
			EventType:    OCPPEventTypeDisconnect,
			Identity:     msg.Identity,
			ConnectionID: msg.ConnectionID,
		})
	})
}

// capture hands the message to the worker, which runs the
// validate → cap → redact chokepoint.
func (c *OCPPClient) capture(msg ocppMessage) {
	w := c.current()
	if w == nil {
		return
	}
	w.captureOCPP(msg, c.redact)
}

// newUUID returns a random UUIDv4 string using crypto/rand.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing means a broken platform; still return a
		// usable (zeroed) id rather than panicking into the host.
		b = [16]byte{}
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	dst := make([]byte, 32)
	hex.Encode(dst, b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", dst[0:8], dst[8:12], dst[12:16], dst[16:20], dst[20:32])
}
