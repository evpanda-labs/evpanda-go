package evpanda

// OCPIDirection is an OCPI message's direction relative to the host.
// The values are the exact wire strings the ingestion API validates.
type OCPIDirection string

const (
	// OCPIInbound is traffic received by the host (partner → host).
	OCPIInbound OCPIDirection = "IN"
	// OCPIOutbound is traffic sent by the host (host → partner).
	OCPIOutbound OCPIDirection = "OUT"
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

// OCPPEventType is an OCPP WebSocket lifecycle event.
type OCPPEventType int

const (
	// OCPPEventTypeDisconnect indicates the WebSocket closed.
	OCPPEventTypeDisconnect OCPPEventType = 0
	// OCPPEventTypeConnect indicates the WebSocket was established.
	OCPPEventTypeConnect OCPPEventType = 1
	// OCPPEventTypeMessage indicates a message frame.
	OCPPEventTypeMessage OCPPEventType = 2
)

// CapturedHTTP is a captured HTTP exchange. Bodies larger than
// MaxCaptureBytes cause the whole message to be dropped at capture.
type CapturedHTTP struct {
	Method          string
	URL             string
	StatusCode      int
	RequestHeaders  map[string]string
	ResponseHeaders map[string]string
	RequestBody     []byte
	ResponseBody    []byte
}

// OCPIMessageInput is the input to [OCPIClient.CaptureInbound] and
// [OCPIClient.CaptureOutbound]; the client stamps the direction.
type OCPIMessageInput struct {
	// Identity attributes the message. Invalid ⇒ message dropped.
	Identity RoamingIdentity
	// HTTP is the captured exchange.
	HTTP CapturedHTTP
}

// OCPPMessageInput is the input shape for the three flat OCPP capture
// primitives. Data and Direction are only used by
// [OCPPClient.CaptureMessage], which requires both and drops the message
// if either is missing; connect/disconnect carry no frame.
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

// ocpiMessage is the internal buffered form of an OCPI capture.
type ocpiMessage struct {
	Direction OCPIDirection
	Identity  RoamingIdentity
	HTTP      CapturedHTTP
}

// ocppMessage is the internal buffered form of an OCPP capture.
type ocppMessage struct {
	EventType    OCPPEventType
	Identity     ChargerIdentity
	ConnectionID string
	// direction is nil for connect/disconnect events.
	Direction *OCPPDirection
	Payload   []byte
}

// anyMessage is an ocpiMessage or ocppMessage; it lets the buffer hold
// either without a protocol tag.
type anyMessage interface {
	isMessage()
}

func (ocpiMessage) isMessage() {}
func (ocppMessage) isMessage() {}
