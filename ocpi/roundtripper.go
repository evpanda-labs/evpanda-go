package ocpi

import (
	"net/http"

	evpanda "github.com/evpanda-labs/evpanda-go"
)

// Adapter — OCPI outbound (host → partner), http.RoundTripper.
//
// Wraps any RoundTripper and returns a new one, so it drops onto an
// http.Client's Transport and captures every call made through it —
// including calls made by a generated OCPI client, as long as it accepts
// an *http.Client.
//
// The only change it makes to a request is stripping the SDK's own
// X-EVPanda-* identity headers before dispatch, so a partner never
// receives them. The response is never altered: bodies are recorded as
// the caller reads them, not by buffering them up front.

// RoundTripper wraps base so that OCPI calls made through it are
// captured. A nil base uses [http.DefaultTransport].
//
// Identity comes from [DefaultResolver] unless [WithResolver]
// overrides it — the request context first, then the X-EVPanda-* headers.
// The context is usually the better fit here, since you have already
// looked the partner up to get their token:
//
//	client := &http.Client{Transport: panda.RoundTripper(nil)}
//
//	ctx := evpanda.ContextWithIdentity(ctx, evpanda.RoamingIdentity{
//		PlatformID:   partner.ID,
//		PlatformName: partner.Name,
//	})
//	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, partner.URL+"/sessions", body)
//	req.Header.Set("Authorization", "Token "+partner.TokenB)
//	resp, err := client.Do(req)
//
// One partner per client? Stamp the headers as defaults on your own
// wrapper instead, and skip the context entirely.
//
// Capture completes when you close the response body — which you must do
// anyway — so a request whose response is never closed is never shipped.
// A transport error propagates unchanged and captures nothing: there is
// no exchange to record.
func RoundTripper(c Capturer, base http.RoundTripper, opts ...Option) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	// An inert client (bad config) hands the base transport straight
	// back: no wrapper, no per-request work.
	if c == nil {
		return base
	}
	if _, active := capturing(c); !active {
		return base
	}
	return &ocpiRoundTripper{client: c, base: base, opts: newOptions(opts)}
}

type ocpiRoundTripper struct {
	client Capturer
	base   http.RoundTripper
	opts   options
}

// RoundTrip implements [http.RoundTripper]. It never mutates the request
// it was given — the contract requires that — and never returns an error
// that the base transport did not produce.
func (t *ocpiRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Re-checked per call: Close drops the worker, and a closed client
	// reverts to an untouched transport rather than capturing into a
	// no-op. The identity headers are still stripped, so a client that
	// closes mid-flight doesn't suddenly start leaking them to partners.
	maxBytes, active := capturing(t.client)

	outbound := req.Clone(req.Context())
	for _, h := range identityHeaders {
		outbound.Header.Del(h)
	}

	if !active {
		return t.base.RoundTrip(outbound)
	}

	// Resolve against the original request, so the resolver still sees
	// the identity headers the partner never will.
	identity, ok := safeResolve(t.opts.resolve, req)
	if !ok {
		return t.base.RoundTrip(outbound)
	}

	reqBody := newCappedBody(maxBytes)
	if outbound.Body != nil {
		outbound.Body = &teeReadCloser{rc: outbound.Body, body: reqBody}
	}

	// Snapshot the request before dispatch: the transport is free to add
	// its own headers, and those are not part of what we set out to send.
	method, url := outbound.Method, outbound.URL.String()
	reqHeaders := headerMap(outbound.Header)

	resp, err := t.base.RoundTrip(outbound)
	if err != nil {
		return nil, err // no exchange happened; nothing to capture
	}

	exchange := outboundExchange{
		identity:    identity,
		method:      method,
		url:         url,
		statusCode:  resp.StatusCode,
		reqHeaders:  reqHeaders,
		reqBody:     reqBody,
		respHeaders: headerMap(resp.Header),
		respBody:    newCappedBody(maxBytes),
	}

	// A response with no body (a HEAD, or 204) has nothing left to wait
	// for, so ship it now.
	if resp.Body == nil || resp.Body == http.NoBody {
		t.ship(exchange)
		return resp, nil
	}

	resp.Body = &teeReadCloser{
		rc:     resp.Body,
		body:   exchange.respBody,
		onDone: func() { t.ship(exchange) },
	}
	return resp, nil
}

// outboundExchange is the assembled-but-not-yet-shipped capture. The
// bodies fill in as the transport writes the request and the caller reads
// the response.
type outboundExchange struct {
	identity    evpanda.RoamingIdentity
	method      string
	url         string
	statusCode  int
	reqHeaders  map[string]string
	reqBody     *cappedBody
	respHeaders map[string]string
	respBody    *cappedBody
}

// ship turns the recorded exchange into a capture, dropping it if either
// body outgrew the cap.
func (t *ocpiRoundTripper) ship(ex outboundExchange) {
	guard(func() {
		reqBody, reqOver := ex.reqBody.result()
		respBody, respOver := ex.respBody.result()
		if reqOver || respOver {
			return
		}
		data := evpanda.HTTPExchange{
			Method:          ex.method,
			URL:             ex.url,
			StatusCode:      ex.statusCode,
			RequestHeaders:  ex.reqHeaders,
			ResponseHeaders: ex.respHeaders,
		}
		data.SetRequestBody(reqBody)
		data.SetResponseBody(respBody)
		t.client.CaptureOutboundMessage(evpanda.OCPIMessageInput{Identity: ex.identity, Data: data})
	})
}
