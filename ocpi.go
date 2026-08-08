package evpanda

// OCPIClient captures and ships OCPI roaming traffic. Build it with
// [StartOCPI]; it is safe for concurrent use.
type OCPIClient struct {
	client
	redactor *ocpiRedactor
}

// StartOCPI validates cfg, builds the client, and launches its background
// worker.
//
// It always returns a non-nil, usable *OCPIClient. On an invalid config
// the returned client is an inert no-op and the error describes the
// problem, so a bad config can never crash the host's boot — callers may
// surface the error or ignore it and keep a silent client.
func StartOCPI(cfg OCPIConfig) (*OCPIClient, error) {
	resolved, err := resolveOCPIConfig(cfg)
	if err != nil {
		return &OCPIClient{}, err
	}
	eng, err := newEngine(resolved)
	if err != nil {
		return &OCPIClient{}, err
	}
	c := &OCPIClient{redactor: newOCPIRedactor(resolved.allowedHeaders)}
	c.eng = eng
	return c, nil
}

// CaptureInbound buffers an inbound OCPI message (partner → host) for
// delivery. Non-blocking and never panics; a message with an invalid
// identity or an oversize body is silently dropped.
func (c *OCPIClient) CaptureInbound(msg OCPIMessageInput) {
	c.guardCapture("CaptureInbound", func() { c.capture(msg, OCPIInbound) })
}

// CaptureOutbound buffers an outbound OCPI message (host → partner) for
// delivery. Non-blocking and never panics; a message with an invalid
// identity or an oversize body is silently dropped.
func (c *OCPIClient) CaptureOutbound(msg OCPIMessageInput) {
	c.guardCapture("CaptureOutbound", func() { c.capture(msg, OCPIOutbound) })
}

// capture stamps the direction and runs the chokepoint:
// validate → cap → redact → enqueue.
func (c *OCPIClient) capture(msg OCPIMessageInput, direction OCPIDirection) {
	eng := c.current()
	if eng == nil {
		return
	}
	if !validateRoamingIdentity(msg.Identity) {
		return
	}
	if len(msg.HTTP.RequestBody) > eng.maxCaptureBytes ||
		len(msg.HTTP.ResponseBody) > eng.maxCaptureBytes {
		return
	}
	eng.enqueue(c.redactor.redact(ocpiMessage{
		Direction: direction,
		Identity:  msg.Identity,
		HTTP:      msg.HTTP,
	}))
}
