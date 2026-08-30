// Package evpanda provides passive OCPI/OCPP traffic capture for
// embedding in OCPI servers and OCPP CSMS. It records protocol messages,
// buffers them in-process, and ships them in batches to the EVPanda
// ingestion API.
//
// The SDK stays out of the host's way: capture calls are non-blocking and
// never panic, memory is bounded, and under stress or network failure it
// drops data rather than degrading the application.
//
// # The protocol is the client
//
// [StartOCPI] returns an [OCPIClient], [StartOCPP] an [OCPPClient] — pick
// the one your service speaks. Both always hand back a usable client: on
// a bad Endpoint or APIKey the client is an inert no-op and the error
// describes the problem, so a config typo can never crash the host's
// boot.
//
//	// APIKey omitted ⇒ read from the EVPANDA_API_KEY env var.
//	panda, err := evpanda.StartOCPI(evpanda.OCPIConfig{
//		BaseConfig: evpanda.BaseConfig{Endpoint: "https://ingest.evpanda.io"},
//	})
//	if err != nil {
//		log.Printf("evpanda: %v (running inert)", err)
//	}
//	defer func() { _ = panda.Close() }()
//
//	panda.CaptureInboundMessage(evpanda.OCPIMessageInput{ /* Identity + Data */ })
//
// For OCPP, prefer the session handle returned by
// [OCPPClient.Connection]: it mints the connection ID and carries the
// identity, so per-frame calls carry neither.
//
//	sess := panda.Connection(evpanda.Charger{ID: "CP-001"})
//	sess.Message(frame, evpanda.FromCP)
//	sess.Disconnect()
//
// # OCPI adapters
//
// Package github.com/evpanda-labs/evpanda-go/ocpi wraps stdlib HTTP so a
// host needs no capture code of its own: ocpi.Middleware for the requests
// partners make to you, ocpi.RoundTripper for the ones you make to them.
// A request with no resolvable identity is served exactly as it would
// have been, and simply is not captured.
//
// # Knowing whether it is working
//
// Every client exposes Stats, a snapshot of its delivery counters whose
// fields each map to one root cause — see [Stats]. Problems are also
// reported to your logger by default, summarized once a minute so a fault
// that recurs on every request still costs one line; [LogMode] and the
// EVPANDA_LOG environment variable control that.
//
// # Delivery
//
// Capture hands the message to a ring buffer and returns. One background
// goroutine owns delivery: it flushes when a full batch (1000) is waiting
// or when FlushInterval elapses, whichever comes first, and the transport
// retries with capped exponential backoff. If the upstream is slow or
// down the buffer evicts its oldest entries once MaxBufferBytes is
// reached — the host never blocks and memory never grows past that
// ceiling.
//
// Both clients carry the same lifecycle methods: Flush forces a delivery
// and waits for it, Close drains what is buffered within DrainTimeout,
// and Shutdown does the same against a context you supply. Close is
// idempotent, and captures made after it are safe no-ops.
package evpanda
