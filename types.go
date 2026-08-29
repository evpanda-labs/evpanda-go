package evpanda

import (
	"bytes"
	"strings"
)

// Message types. These must match apispec/ingestion-api.yaml.
// protocol and the capture timestamp are SDK-owned (they live on the
// internal envelope, see buffer.go) — deliberately not on these.

// protocol routes a batch to POST /v1/{protocol} on the ingestion API.
// One client serves exactly one protocol.
type protocol string

const (
	// protocolOCPI is the OCPI ingestion route.
	protocolOCPI protocol = "ocpi"
	// protocolOCPP is the OCPP ingestion route.
	protocolOCPP protocol = "ocpp"
)

// ocpiDirection is an OCPI message's direction relative to the host.
// The values are the exact wire strings the ingestion API validates.
type ocpiDirection string

const (
	// ocpiInbound is traffic received by the host (partner → host).
	ocpiInbound ocpiDirection = "IN"
	// ocpiOutbound is traffic sent by the host (host → partner).
	ocpiOutbound ocpiDirection = "OUT"
)

// OCPPDirection is an OCPP frame's direction relative to the charge point.
// The values are the exact wire strings the ingestion API validates.
type OCPPDirection string

const (
	// ToCP is a frame sent to the charge point (CSMS → CP).
	ToCP OCPPDirection = "TO_CP"
	// FromCP is a frame received from the charge point (CP → CSMS).
	FromCP OCPPDirection = "FROM_CP"
)

// ocppEventType is an OCPP WebSocket lifecycle event, mapped onto the
// ingestion API's event_type.
type ocppEventType int

const (
	// ocppEventTypeDisconnect indicates the WebSocket closed.
	ocppEventTypeDisconnect ocppEventType = 0
	// ocppEventTypeConnect indicates the WebSocket was established.
	ocppEventTypeConnect ocppEventType = 1
	// ocppEventTypeMessage indicates a message frame.
	ocppEventTypeMessage ocppEventType = 2
)

// HTTPExchange is a captured HTTP request/response pair. Bodies are raw
// bytes; a body larger than MaxCaptureBytes drops the whole message at
// capture rather than storing a truncated one.
//
// Set the bodies with [HTTPExchange.SetRequestBody] and
// [HTTPExchange.SetResponseBody] rather than assigning the fields. The
// setters copy, so the SDK owns what it buffers and you are free to reuse
// your own buffer the moment the capture call returns.
type HTTPExchange struct {
	// Method is the HTTP method, e.g. "POST".
	Method string
	// URL is the request URL as seen by the host.
	URL string
	// StatusCode is the response status; zero is sent as null.
	StatusCode int
	// RequestHeaders is filtered by the OCPI header allowlist.
	RequestHeaders map[string]string
	// ResponseHeaders is filtered by the OCPI header allowlist.
	ResponseHeaders map[string]string
	// RequestBody is the raw request body. Optional. Prefer
	// SetRequestBody, which copies.
	RequestBody []byte
	// ResponseBody is the raw response body. Optional. Prefer
	// SetResponseBody, which copies.
	ResponseBody []byte
}

// SetRequestBody records b as the captured request body, copying it so
// the exchange owns the bytes.
//
// Copying is what makes the field safe to hand over: a capture is held in
// memory until the next flush, so a body assigned straight from a pooled
// or reused buffer would be serialized after the host had already
// overwritten it. An empty or nil body is recorded as absent, which
// serializes as JSON null.
func (x *HTTPExchange) SetRequestBody(b []byte) {
	x.RequestBody = cloneBody(b)
}

// SetResponseBody records b as the captured response body, copying it so
// the exchange owns the bytes. See [HTTPExchange.SetRequestBody].
func (x *HTTPExchange) SetResponseBody(b []byte) {
	x.ResponseBody = cloneBody(b)
}

// own copies both bodies so the exchange no longer aliases the caller's
// memory. The chokepoint calls it before redaction, which is what lets a
// redactor rewrite a body in place and lets the host reuse its buffers as
// soon as the capture call returns.
func (x *HTTPExchange) own() {
	x.RequestBody = cloneBody(x.RequestBody)
	x.ResponseBody = cloneBody(x.ResponseBody)
}

// cloneBody copies b, normalizing empty to nil so an absent body
// serializes as JSON null rather than an empty string.
func cloneBody(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return bytes.Clone(b)
}

// OCPIMessageInput is the input to [OCPIClient.CaptureInboundMessage] and
// [OCPIClient.CaptureOutboundMessage]. There is no direction field — the
// method you call stamps it.
type OCPIMessageInput struct {
	// Identity attributes the message to a roaming partner. Invalid ⇒
	// message dropped.
	Identity RoamingIdentity
	// Data is the captured HTTP exchange.
	Data HTTPExchange
}

// OCPPMessageInput is the input shape for the three flat OCPP capture
// primitives. Data and Direction are only used by
// [OCPPClient.CaptureMessage], which requires both and drops the message
// if either is missing; connect and disconnect carry no frame.
type OCPPMessageInput struct {
	// Identity is the charge point this event belongs to. Invalid ⇒
	// message dropped.
	Identity ChargerIdentity
	// ConnectionID is stable for the lifetime of this connection. The
	// session handle returned by [OCPPClient.Connection] mints and
	// carries it for you.
	ConnectionID string
	// Data is the raw frame. Required by CaptureMessage.
	Data []byte
	// Direction is the frame direction. Required by CaptureMessage.
	Direction OCPPDirection
}

// ocpiMessage is the internal buffered form of an OCPI capture: the input
// plus the direction the capture method stamped.
type ocpiMessage struct {
	Direction ocpiDirection
	Identity  RoamingIdentity
	Data      HTTPExchange
}

// ocppMessage is the internal buffered form of an OCPP capture. Direction
// is empty for connect and disconnect events.
type ocppMessage struct {
	EventType    ocppEventType
	Identity     ChargerIdentity
	ConnectionID string
	Direction    OCPPDirection
	Payload      []byte
}

// message is the sealed interface the ring buffer holds, so it can carry
// either protocol without a tag. Implementations map themselves onto the
// ingestion wire record (record, in transport.go) and account for their
// own footprint against the buffer budget (size, in buffer.go).
type message interface {
	record(capturedAt string) any
	size() int
}

// ── Identity ─────────────────────────────────────────────────────────────
//
// Per-message identity: the two protocol shapes and their validation
// rules. Valid is the single rule source — every capture path goes
// through it, and adapters use it to decide whether instrumenting a
// request is worth the work. Nothing here fails loudly; an invalid
// identity means the caller drops the message.

// RoamingIdentity is the OCPI roaming context for a message. PlatformID
// and PlatformName are required; TenantID and TenantName are optional but
// all-or-nothing (supply both or neither).
type RoamingIdentity struct {
	PlatformID   string
	PlatformName string
	TenantID     string
	TenantName   string
}

// ChargerIdentity is the OCPP charger context for a message. ChargerID is
// required; TenantID and TenantName are optional but all-or-nothing.
type ChargerIdentity struct {
	ChargerID  string
	TenantID   string
	TenantName string
}

func isNonEmpty(v string) bool {
	return strings.TrimSpace(v) != ""
}

// isTenantPairValid reports whether tenant ID and name are both set or
// both empty.
func isTenantPairValid(tenantID, tenantName string) bool {
	return isNonEmpty(tenantID) == isNonEmpty(tenantName)
}

// Valid reports whether the identity can attribute a message: PlatformID
// and PlatformName present, and the tenant pair all-or-nothing. The SDK
// silently drops messages that fail it.
func (id RoamingIdentity) Valid() bool {
	return isNonEmpty(id.PlatformID) &&
		isNonEmpty(id.PlatformName) &&
		isTenantPairValid(id.TenantID, id.TenantName)
}

// Valid reports whether the identity can attribute a message: ChargerID
// present, and the tenant pair all-or-nothing.
func (id ChargerIdentity) Valid() bool {
	return isNonEmpty(id.ChargerID) &&
		isTenantPairValid(id.TenantID, id.TenantName)
}
