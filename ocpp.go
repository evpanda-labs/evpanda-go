package evpanda

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// OCPPClient captures and ships OCPP CSMS traffic. Build it with
// [StartOCPP]; it is safe for concurrent use.
//
// Two ways to capture:
//   - [OCPPClient.Connection] — the recommended path: a session handle
//     that owns the connection ID and carries the identity. Attach it to
//     your WebSocket and call Message / Disconnect.
//   - [OCPPClient.CaptureConnect] / [OCPPClient.CaptureMessage] /
//     [OCPPClient.CaptureDisconnect] — the flat primitives the session is
//     built on, for one-off capture.
type OCPPClient struct {
	client
}

// StartOCPP validates cfg, builds the client, and launches its background
// worker.
//
// It always returns a non-nil, usable *OCPPClient. On an invalid config
// the returned client is an inert no-op and the error describes the
// problem, so a bad config can never crash the host's boot — callers may
// surface the error or ignore it and keep a silent client.
func StartOCPP(cfg OCPPConfig) (*OCPPClient, error) {
	resolved, err := resolveOCPPConfig(cfg)
	if err != nil {
		return &OCPPClient{}, err
	}
	eng, err := newEngine(resolved)
	if err != nil {
		return &OCPPClient{}, err
	}
	c := &OCPPClient{}
	c.eng = eng
	return c, nil
}

// OCPPSession is a live capture handle for one OCPP WebSocket connection.
// Returned by [OCPPClient.Connection]; it owns the connection ID and the
// identity so per-frame calls carry neither. Attach it to your connection
// object and call Message per frame, Disconnect when the socket closes.
type OCPPSession struct {
	// ConnectionID is the SDK-minted ID for this connection — fresh per
	// Connection call.
	ConnectionID string

	client   *OCPPClient
	identity ChargerIdentity
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

// Connection opens a capture session for one OCPP connection: it mints a
// connection ID, records the connect, and returns an [OCPPSession].
// Attach the handle to your WebSocket.
func (c *OCPPClient) Connection(identity ChargerIdentity) *OCPPSession {
	connectionID := newUUID()
	c.CaptureConnect(OCPPMessageInput{Identity: identity, ConnectionID: connectionID})
	return &OCPPSession{ConnectionID: connectionID, client: c, identity: identity}
}

// CaptureConnect records a new OCPP connection. It uses Identity and
// ConnectionID only. Non-blocking and never panics.
func (c *OCPPClient) CaptureConnect(msg OCPPMessageInput) {
	c.guardCapture("CaptureConnect", func() {
		eng := c.current()
		if eng == nil || !validateChargerIdentity(msg.Identity) {
			return
		}
		eng.enqueue(redactOCPP(ocppMessage{
			EventType:    OCPPEventTypeConnect,
			Identity:     msg.Identity,
			ConnectionID: msg.ConnectionID,
		}))
	})
}

// CaptureMessage records one OCPP frame. It requires Data and Direction
// and drops the message if either is missing; oversize frames are
// dropped. Non-blocking and never panics.
func (c *OCPPClient) CaptureMessage(msg OCPPMessageInput) {
	c.guardCapture("CaptureMessage", func() {
		eng := c.current()
		if eng == nil {
			return
		}
		if len(msg.Data) == 0 || msg.Direction == "" {
			return
		}
		if !validateChargerIdentity(msg.Identity) {
			return
		}
		if len(msg.Data) > eng.maxCaptureBytes {
			return
		}
		direction := msg.Direction
		eng.enqueue(redactOCPP(ocppMessage{
			EventType:    OCPPEventTypeMessage,
			Identity:     msg.Identity,
			ConnectionID: msg.ConnectionID,
			Direction:    &direction,
			Payload:      msg.Data,
		}))
	})
}

// CaptureDisconnect records the connection closing. It uses Identity and
// ConnectionID only. Non-blocking and never panics.
func (c *OCPPClient) CaptureDisconnect(msg OCPPMessageInput) {
	c.guardCapture("CaptureDisconnect", func() {
		eng := c.current()
		if eng == nil || !validateChargerIdentity(msg.Identity) {
			return
		}
		eng.enqueue(redactOCPP(ocppMessage{
			EventType:    OCPPEventTypeDisconnect,
			Identity:     msg.Identity,
			ConnectionID: msg.ConnectionID,
		}))
	})
}

// newUUID returns a random UUIDv4 string using crypto/rand.
func newUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is a broken platform; still return a
		// usable (zeroed) id rather than panicking into the host.
		b = [16]byte{}
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	dst := make([]byte, 32)
	hex.Encode(dst, b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", dst[0:8], dst[8:12], dst[12:16], dst[16:20], dst[20:32])
}
