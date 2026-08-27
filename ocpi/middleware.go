package ocpi

import (
	"bufio"
	"errors"
	"net"
	"net/http"

	evpanda "github.com/evpanda-labs/evpanda-go"
)

// Adapter — OCPI inbound (partner → host), net/http.
//
// Standard func(http.Handler) http.Handler middleware, so it drops into
// net/http, chi, gorilla/mux, echo's WrapMiddleware, gin's WrapH — or
// anything else that speaks the stdlib shape. It resolves identity,
// records the request body as the handler reads it, tees the response,
// and ships one message when the handler returns.
//
// A request with no resolvable identity passes through untouched: no
// wrapping, no recording, no error. The middleware never panics into the
// host, and it never alters the response the client receives.

// Middleware returns net/http middleware that captures inbound OCPI
// exchanges (a partner calling your server).
//
// Identity comes from [DefaultResolver] unless [WithResolver]
// overrides it — the request context first, then the X-EVPanda-* headers.
// Mount it after whatever authenticates the partner, so the identity is
// already on the context:
//
//	mux := http.NewServeMux()
//	mux.Handle("POST /ocpi/2.2/cdrs", cdrHandler)
//
//	srv := &http.Server{Handler: auth(panda.Middleware()(mux))}
//
// The request body is recorded as your handler reads it, so a handler
// that ignores the body records none — nothing is buffered on your
// behalf, and a streaming handler is not forced into memory. Bodies are
// capped at MaxCaptureBytes; an oversize body on either side drops the
// whole message rather than storing a truncated one.
func Middleware(c Capturer, opts ...Option) func(http.Handler) http.Handler {
	o := newOptions(opts)

	return func(next http.Handler) http.Handler {
		// An inert client (bad config) costs nothing: hand back the
		// handler unwrapped, so there is not even a call frame per
		// request.
		if c == nil {
			return next
		}
		if _, active := limitsOf(c); !active {
			return next
		}

		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Re-checked per request: Close drops the worker, and a
			// closed client must stop doing capture work rather than
			// record into a no-op.
			maxBytes, active := limitsOf(c)
			if !active {
				next.ServeHTTP(w, r)
				return
			}

			identity, ok := safeResolve(o.resolve, r)
			if !ok {
				next.ServeHTTP(w, r)
				return
			}

			reqBody := newCappedBody(maxBytes)
			if r.Body != nil {
				r = r.Clone(r.Context())
				r.Body = &teeReadCloser{rc: r.Body, body: reqBody}
			}

			// Snapshot the request line before the handler runs: a
			// handler is free to rewrite r.URL as it routes.
			method, url := r.Method, requestURI(r)
			reqHeaders := headerMap(r.Header)

			rec := &responseRecorder{ResponseWriter: w, body: newCappedBody(maxBytes)}

			// Deferred so an exchange the handler panicked out of is
			// still recorded; the panic itself continues to unwind.
			defer func() {
				guard(func() {
					shipInbound(c, identity, inboundExchange{
						method:      method,
						url:         url,
						reqHeaders:  reqHeaders,
						reqBody:     reqBody,
						respHeaders: headerMap(rec.Header()),
						rec:         rec,
					})
				})
			}()

			next.ServeHTTP(rec, r)
			rec.completed = true
		})
	}
}

// inboundExchange is the assembled-but-not-yet-validated capture.
type inboundExchange struct {
	method      string
	url         string
	reqHeaders  map[string]string
	reqBody     *cappedBody
	respHeaders map[string]string
	rec         *responseRecorder
}

// shipInbound turns the recorded exchange into a capture, dropping it if
// either body outgrew the cap or the connection was hijacked (in which
// case there is no HTTP response to describe).
func shipInbound(c Capturer, identity evpanda.RoamingIdentity, ex inboundExchange) {
	if ex.rec.hijacked {
		return
	}
	reqBody, reqOver := ex.reqBody.result()
	respBody, respOver := ex.rec.body.result()
	if reqOver || respOver {
		return
	}
	data := evpanda.HTTPExchange{
		Method:          ex.method,
		URL:             ex.url,
		StatusCode:      ex.rec.statusCode(),
		RequestHeaders:  ex.reqHeaders,
		ResponseHeaders: ex.respHeaders,
	}
	data.SetRequestBody(reqBody)
	data.SetResponseBody(respBody)
	c.CaptureInboundMessage(evpanda.OCPIMessageInput{Identity: identity, Data: data})
}

// requestURI is the path as it arrived on the wire, falling back to the
// parsed URL for requests built in-process (as httptest and client-side
// middleware do).
func requestURI(r *http.Request) string {
	if r.RequestURI != "" {
		return r.RequestURI
	}
	if r.URL != nil {
		return r.URL.String()
	}
	return ""
}

// ── Response recording ───────────────────────────────────────────────────

// responseRecorder tees the status and body on their way to the client.
// It forwards everything unchanged — the host's response is never altered
// by capture.
type responseRecorder struct {
	http.ResponseWriter
	body *cappedBody

	status      int
	wroteHeader bool
	// completed is set once the handler has returned normally, which is
	// how a panicked handler is told apart from a silent 200.
	completed bool
	hijacked  bool
}

func (w *responseRecorder) WriteHeader(status int) {
	if !w.wroteHeader {
		w.status = status
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseRecorder) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		// net/http infers 200 from the first write; record the same.
		w.status = http.StatusOK
		w.wroteHeader = true
	}
	n, err := w.ResponseWriter.Write(p)
	if n > 0 {
		w.body.push(p[:n])
	}
	return n, err
}

// statusCode is what the client actually saw: the recorded status, or the
// 200 net/http sends for a handler that returned without writing. A
// handler that panicked before writing gets zero, which ships as null
// rather than an invented success.
func (w *responseRecorder) statusCode() int {
	if w.wroteHeader {
		return w.status
	}
	if w.completed {
		return http.StatusOK
	}
	return 0
}

// Unwrap exposes the wrapped writer to http.ResponseController, which is
// how Flush, SetWriteDeadline and friends reach the real writer on
// Go 1.20+.
func (w *responseRecorder) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// Flush forwards to the wrapped writer when it supports flushing, for
// handlers that still type-assert http.Flusher directly.
func (w *responseRecorder) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack forwards to the wrapped writer, and marks the exchange
// uncapturable: once the connection is taken over (a WebSocket upgrade,
// say) there is no HTTP response left to record.
func (w *responseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("evpanda: the wrapped ResponseWriter does not support hijacking")
	}
	conn, rw, err := h.Hijack()
	if err == nil {
		w.hijacked = true
	}
	return conn, rw, err
}
