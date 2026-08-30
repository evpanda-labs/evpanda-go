// Adapter tests: drive the net/http middleware and the RoundTripper
// against an httptest upstream, and assert what reaches the ingestion
// mock — identity resolution (context first, headers second), body and
// header capture, outbound header stripping, and that neither adapter
// alters the traffic it wraps.

package ocpi_test

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	evpanda "github.com/evpanda-labs/evpanda-go"
	"github.com/evpanda-labs/evpanda-go/ocpi"
)

var testIdentity = evpanda.Platform{
	ID:   "acme",
	Name: "Acme Mobility",
}

// startOCPI builds a live client pointed at the ingestion mock.
func startOCPI(t *testing.T, mock *mockUpstream) *evpanda.OCPIClient {
	t.Helper()
	panda, err := evpanda.StartOCPI(ocpiConfig(mock.server.URL))
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	t.Cleanup(func() { _ = panda.Close() })
	return panda
}

// decodeBody returns a captured base64 body as a string, or "" for null.
func decodeBody(t *testing.T, rec map[string]any, key string) string {
	t.Helper()
	raw, ok := rec[key]
	if !ok {
		t.Fatalf("record has no %q key", key)
	}
	if raw == nil {
		return ""
	}
	decoded, err := base64.StdEncoding.DecodeString(raw.(string))
	if err != nil {
		t.Fatalf("%s is not base64: %v", key, err)
	}
	return string(decoded)
}

// echoHandler reads the request body and answers with a fixed JSON body,
// which is what an OCPI endpoint does.
var echoHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Correlation-Id", "corr-1")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(`{"status_code":1000,"echo":` + string(body) + `}`))
})

// ── Middleware (inbound) ─────────────────────────────────────────────────

// The context is the first-priority identity source: an auth layer stamps
// it, and the adapter picks it up without anything touching the wire.
func TestMiddlewareResolvesIdentityFromContext(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)

	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := ocpi.ContextWithIdentity(r.Context(), evpanda.Platform{
				ID:         "acme",
				Name:       "Acme Mobility",
				TenantID:   "t1",
				TenantName: "Tenant One",
			})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	srv := httptest.NewServer(auth(ocpi.Middleware(panda)(echoHandler)))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/ocpi/2.2/cdrs", "application/json",
		strings.NewReader(`{"id":"cdr-1"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	// The middleware must not disturb what the caller receives.
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if !strings.Contains(string(body), `"echo":{"id":"cdr-1"}`) {
		t.Fatalf("handler response was altered: %s", body)
	}

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	rec := mock.recordsFor("/v1/ocpi")[0]

	if rec["direction"] != "IN" {
		t.Fatalf("direction = %v, want IN", rec["direction"])
	}
	if rec["platform_id"] != "acme" || rec["tenant_id"] != "t1" {
		t.Fatalf("identity not taken from the context: %v / %v", rec["platform_id"], rec["tenant_id"])
	}
	if rec["http_method"] != "POST" || rec["url"] != "/ocpi/2.2/cdrs" {
		t.Fatalf("request line = %v %v", rec["http_method"], rec["url"])
	}
	if rec["response_status_code"].(float64) != 201 {
		t.Fatalf("response_status_code = %v, want 201", rec["response_status_code"])
	}
	if got := decodeBody(t, rec, "request_body"); got != `{"id":"cdr-1"}` {
		t.Fatalf("request_body = %q", got)
	}
	if got := decodeBody(t, rec, "response_body"); !strings.Contains(got, `"status_code":1000`) {
		t.Fatalf("response_body = %q", got)
	}
	// Response headers are captured and still pass the allowlist.
	respHeaders := rec["response_headers"].(map[string]any)
	if respHeaders["x-correlation-id"] != "corr-1" {
		t.Fatalf("response headers = %v", respHeaders)
	}
}

// With nothing on the context, the adapter falls back to the X-EVPanda-*
// headers — for hosts whose identity is stamped by an upstream proxy.
func TestMiddlewareFallsBackToHeaders(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)

	srv := httptest.NewServer(ocpi.Middleware(panda)(echoHandler))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/ocpi/2.2/tokens",
		strings.NewReader(`{"id":"t1"}`))
	req.Header.Set(ocpi.HeaderPlatformID, "acme")
	req.Header.Set(ocpi.HeaderPlatformName, "Acme Mobility")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	if got := mock.recordsFor("/v1/ocpi")[0]["platform_id"]; got != "acme" {
		t.Fatalf("platform_id = %v, want acme (from headers)", got)
	}
}

// A context identity wins over headers, so a stale or spoofed header
// cannot override what your auth layer resolved.
func TestMiddlewareContextBeatsHeaders(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)

	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := ocpi.ContextWithIdentity(r.Context(), testIdentity)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	srv := httptest.NewServer(auth(ocpi.Middleware(panda)(echoHandler)))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ocpi/2.2/locations", nil)
	req.Header.Set(ocpi.HeaderPlatformID, "impostor")
	req.Header.Set(ocpi.HeaderPlatformName, "Impostor")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	if got := mock.recordsFor("/v1/ocpi")[0]["platform_id"]; got != "acme" {
		t.Fatalf("platform_id = %v — the context must win over headers", got)
	}
}

// No identity anywhere ⇒ the request is served exactly as it would have
// been, and nothing is captured.
func TestMiddlewarePassesThroughUnidentifiedRequests(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)

	srv := httptest.NewServer(ocpi.Middleware(panda)(echoHandler))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/health", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want the handler's 201", resp.StatusCode)
	}

	_ = panda.Flush()
	time.Sleep(200 * time.Millisecond)
	if n := len(mock.recordsFor("/v1/ocpi")); n != 0 {
		t.Fatalf("captured %d messages for an unidentified request, want 0", n)
	}
}

// A custom resolver replaces the shipped one entirely, for identity that
// lives somewhere neither the context nor the headers reach.
func TestMiddlewareCustomResolver(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)

	byPath := func(r *http.Request) (evpanda.Platform, bool) {
		id, ok := strings.CutPrefix(r.URL.Path, "/partners/")
		if !ok {
			return evpanda.Platform{}, false
		}
		name, _, _ := strings.Cut(id, "/")
		return evpanda.Platform{ID: name, Name: name}, true
	}

	srv := httptest.NewServer(ocpi.Middleware(panda, ocpi.WithResolver(byPath))(echoHandler))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/partners/nova/cdrs", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	if got := mock.recordsFor("/v1/ocpi")[0]["platform_id"]; got != "nova" {
		t.Fatalf("platform_id = %v, want nova", got)
	}
}

// A resolver that panics must not take the request down with it.
func TestMiddlewareSurvivesAPanickingResolver(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)

	boom := func(*http.Request) (evpanda.Platform, bool) { panic("resolver blew up") }
	srv := httptest.NewServer(ocpi.Middleware(panda, ocpi.WithResolver(boom))(echoHandler))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/ocpi/2.2/cdrs", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d — a broken resolver must not affect the response", resp.StatusCode)
	}
}

// An oversize body drops the whole exchange, never a truncated record.
func TestMiddlewareDropsOversizeBodies(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	// Big enough for the small exchange (10-byte request, 37-byte
	// response) and far below the oversize one.
	cfg := ocpiConfig(mock.server.URL)
	cfg.MaxCaptureBytes = 64
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer func() { _ = panda.Close() }()

	srv := httptest.NewServer(ocpi.Middleware(panda)(echoHandler))
	defer srv.Close()

	post := func(body string) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/ocpi/2.2/cdrs", strings.NewReader(body))
		req.Header.Set(ocpi.HeaderPlatformID, "acme")
		req.Header.Set(ocpi.HeaderPlatformName, "Acme Mobility")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
	}

	post(`{"id":"` + strings.Repeat("x", 100) + `"}`) // over the cap ⇒ dropped
	post(`{"id":"1"}`)                                // small ⇒ kept

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	time.Sleep(200 * time.Millisecond)
	recs := mock.recordsFor("/v1/ocpi")
	if len(recs) != 1 {
		t.Fatalf("captured %d messages, want only the small one", len(recs))
	}
	if got := decodeBody(t, recs[0], "request_body"); got != `{"id":"1"}` {
		t.Fatalf("survivor request_body = %q", got)
	}
}

// An inert client hands the handler back unwrapped, and a closed one
// stops capturing without breaking the server it is mounted on.
func TestMiddlewareOnInertAndClosedClients(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	inert, err := evpanda.StartOCPI(evpanda.OCPIConfig{
		BaseConfig: evpanda.BaseConfig{Endpoint: "not-a-url", APIKey: "k"},
	})
	if err == nil {
		t.Fatal("expected an error for a bad endpoint")
	}
	mux := http.NewServeMux() // a pointer, so identity is comparable
	if got := ocpi.Middleware(inert)(mux); got != http.Handler(mux) {
		t.Fatal("an inert client must return the handler unwrapped")
	}

	panda := startOCPI(t, mock)
	srv := httptest.NewServer(ocpi.Middleware(panda)(echoHandler))
	defer srv.Close()

	if err := panda.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/ocpi/2.2/cdrs", strings.NewReader(`{}`))
	req.Header.Set(ocpi.HeaderPlatformID, "acme")
	req.Header.Set(ocpi.HeaderPlatformName, "Acme Mobility")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST after Close: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d — a closed client must not affect the response", resp.StatusCode)
	}
}

// ── RoundTripper (outbound) ──────────────────────────────────────────────

// partnerServer stands in for a roaming partner's OCPI server and records
// the headers it was actually sent.
type partnerServer struct {
	server  *httptest.Server
	gotHdrs http.Header
}

func startPartner(t *testing.T) *partnerServer {
	t.Helper()
	p := &partnerServer{}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.gotHdrs = r.Header.Clone()
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status_code":1000,"sent":` + string(body) + `}`))
	}))
	t.Cleanup(p.server.Close)
	return p
}

func TestRoundTripperResolvesIdentityFromContext(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)
	partner := startPartner(t)

	client := &http.Client{Transport: ocpi.RoundTripper(panda, nil)}

	ctx := ocpi.ContextWithIdentity(context.Background(), evpanda.Platform{
		ID:         "acme",
		Name:       "Acme Mobility",
		TenantID:   "t1",
		TenantName: "Tenant One",
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		partner.server.URL+"/ocpi/2.2/sessions", strings.NewReader(`{"id":"s1"}`))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The caller's response must arrive intact.
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"sent":{"id":"s1"}`) {
		t.Fatalf("response was altered: %d %s", resp.StatusCode, body)
	}

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	rec := mock.recordsFor("/v1/ocpi")[0]

	if rec["direction"] != "OUT" {
		t.Fatalf("direction = %v, want OUT", rec["direction"])
	}
	if rec["platform_id"] != "acme" || rec["tenant_name"] != "Tenant One" {
		t.Fatalf("identity not taken from the context: %v", rec)
	}
	if rec["response_status_code"].(float64) != 200 {
		t.Fatalf("response_status_code = %v", rec["response_status_code"])
	}
	if got := decodeBody(t, rec, "request_body"); got != `{"id":"s1"}` {
		t.Fatalf("request_body = %q", got)
	}
	if got := decodeBody(t, rec, "response_body"); !strings.Contains(got, `"status_code":1000`) {
		t.Fatalf("response_body = %q", got)
	}
}

// Identity headers are read by the resolver and then stripped, so the
// partner never learns the host's tenancy.
func TestRoundTripperStripsIdentityHeaders(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)
	partner := startPartner(t)

	client := &http.Client{Transport: ocpi.RoundTripper(panda, nil)}

	req, _ := http.NewRequest(http.MethodGet, partner.server.URL+"/ocpi/2.2/locations", nil)
	req.Header.Set(ocpi.HeaderPlatformID, "acme")
	req.Header.Set(ocpi.HeaderPlatformName, "Acme Mobility")
	req.Header.Set(ocpi.HeaderTenantID, "t1")
	req.Header.Set(ocpi.HeaderTenantName, "Tenant One")
	req.Header.Set("Authorization", "Token B")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	for _, h := range []string{
		ocpi.HeaderPlatformID, ocpi.HeaderPlatformName,
		ocpi.HeaderTenantID, ocpi.HeaderTenantName,
	} {
		if got := partner.gotHdrs.Get(h); got != "" {
			t.Fatalf("partner received %s: %q", h, got)
		}
	}
	if got := partner.gotHdrs.Get("Authorization"); got != "Token B" {
		t.Fatalf("the adapter dropped a header it does not own: Authorization = %q", got)
	}
	// The caller's own request object must come back unmodified.
	if got := req.Header.Get(ocpi.HeaderPlatformID); got != "acme" {
		t.Fatalf("RoundTrip mutated the caller's request: %s = %q", ocpi.HeaderPlatformID, got)
	}

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	rec := mock.recordsFor("/v1/ocpi")[0]
	if rec["platform_id"] != "acme" || rec["tenant_id"] != "t1" {
		t.Fatalf("identity not read from headers before stripping: %v", rec)
	}
	// The allowlist still applies to what was captured.
	if hdrs, ok := rec["request_headers"].(map[string]any); ok {
		if _, leaked := hdrs["authorization"]; leaked {
			t.Fatal("Authorization survived the OCPI allowlist")
		}
	}
}

// A call with no identity goes out untouched and is not captured.
func TestRoundTripperPassesThroughUnidentifiedCalls(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)
	partner := startPartner(t)

	client := &http.Client{Transport: ocpi.RoundTripper(panda, nil)}
	resp, err := client.Get(partner.server.URL + "/health")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	_ = panda.Flush()
	time.Sleep(200 * time.Millisecond)
	if n := len(mock.recordsFor("/v1/ocpi")); n != 0 {
		t.Fatalf("captured %d messages for an unidentified call, want 0", n)
	}
}

// A transport error propagates unchanged, and nothing is captured —
// there was no exchange to record.
func TestRoundTripperPropagatesTransportErrors(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)

	client := &http.Client{Transport: ocpi.RoundTripper(panda, nil)}
	ctx := ocpi.ContextWithIdentity(context.Background(), testIdentity)
	// Port 1 on loopback refuses connections.
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:1/ocpi", nil)

	if _, err := client.Do(req); err == nil {
		t.Fatal("expected the connection error to propagate")
	}

	_ = panda.Flush()
	time.Sleep(200 * time.Millisecond)
	if n := len(mock.recordsFor("/v1/ocpi")); n != 0 {
		t.Fatalf("captured %d messages for a failed call, want 0", n)
	}
}

// Capture completes when the caller closes the body, even if they never
// read it.
func TestRoundTripperCapturesOnCloseWithoutReading(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)
	partner := startPartner(t)

	client := &http.Client{Transport: ocpi.RoundTripper(panda, nil)}
	ctx := ocpi.ContextWithIdentity(context.Background(), testIdentity)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, partner.server.URL+"/ocpi/2.2/tariffs", nil)

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	_ = resp.Body.Close() // closed without reading

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	if got := mock.recordsFor("/v1/ocpi")[0]["url"]; !strings.HasSuffix(got.(string), "/ocpi/2.2/tariffs") {
		t.Fatalf("url = %v", got)
	}
}

// An inert client returns the base transport itself — no wrapper at all.
func TestRoundTripperOnInertClient(t *testing.T) {
	inert, err := evpanda.StartOCPI(evpanda.OCPIConfig{
		BaseConfig: evpanda.BaseConfig{Endpoint: "not-a-url", APIKey: "k"},
	})
	if err == nil {
		t.Fatal("expected an error for a bad endpoint")
	}
	base := http.DefaultTransport
	if got := ocpi.RoundTripper(inert, base); got != base {
		t.Fatal("an inert client must return the base transport unwrapped")
	}
	if got := ocpi.RoundTripper(inert, nil); got != http.DefaultTransport {
		t.Fatal("a nil base must resolve to http.DefaultTransport")
	}
}

// ── Context helpers ──────────────────────────────────────────────────────

func TestRoamingIdentityContextRoundTrip(t *testing.T) {
	if _, ok := ocpi.IdentityFromContext(context.Background()); ok {
		t.Fatal("a bare context must not carry an identity")
	}

	ctx := ocpi.ContextWithIdentity(context.Background(), testIdentity)
	got, ok := ocpi.IdentityFromContext(ctx)
	if !ok || got != testIdentity {
		t.Fatalf("round trip = %v, %v; want %v, true", got, ok, testIdentity)
	}

	// The most recent value wins, so a nested layer can refine what an
	// outer one resolved.
	inner := evpanda.Platform{ID: "nova", Name: "Nova"}
	got, _ = ocpi.IdentityFromContext(ocpi.ContextWithIdentity(ctx, inner))
	if got != inner {
		t.Fatalf("nested value = %v, want %v", got, inner)
	}
}

// The recorder must stay transparent to everything a handler can reach
// through the ResponseWriter, or wrapping it would break streaming,
// deadlines, and upgrades.
func TestMiddlewarePreservesResponseWriterCapabilities(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)

	streamed := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// http.ResponseController is how Go 1.20+ reaches the real writer.
		rc := http.NewResponseController(w)
		if err := rc.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Errorf("SetWriteDeadline through the recorder: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("chunk-1\n"))
		if err := rc.Flush(); err != nil {
			t.Errorf("Flush through the recorder: %v", err)
		}
		// A handler that type-asserts http.Flusher directly must work too.
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		} else {
			t.Error("recorder must still satisfy http.Flusher")
		}
		close(streamed)
		_, _ = w.Write([]byte("chunk-2\n"))
	})

	srv := httptest.NewServer(ocpi.Middleware(panda)(handler))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ocpi/2.2/sessions", nil)
	req.Header.Set(ocpi.HeaderPlatformID, "acme")
	req.Header.Set(ocpi.HeaderPlatformName, "Acme Mobility")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	<-streamed

	if string(body) != "chunk-1\nchunk-2\n" {
		t.Fatalf("streamed body = %q, want both chunks", body)
	}
	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	if got := decodeBody(t, mock.recordsFor("/v1/ocpi")[0], "response_body"); got != "chunk-1\nchunk-2\n" {
		t.Fatalf("captured body = %q, want the whole stream", got)
	}
}

// A hijacked connection has no HTTP response left to describe, so the
// exchange is skipped rather than recorded with an invented status.
func TestMiddlewareSkipsHijackedConnections(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)

	hijacked := make(chan error, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, buf, err := http.NewResponseController(w).Hijack()
		if err != nil {
			hijacked <- err
			return
		}
		_, _ = buf.WriteString("HTTP/1.1 101 Switching Protocols\r\n\r\n")
		_ = buf.Flush()
		_ = conn.Close()
		hijacked <- nil
	})

	srv := httptest.NewServer(ocpi.Middleware(panda)(handler))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ocpi/2.2/ws", nil)
	req.Header.Set(ocpi.HeaderPlatformID, "acme")
	req.Header.Set(ocpi.HeaderPlatformName, "Acme Mobility")
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
	}
	if err := <-hijacked; err != nil {
		t.Fatalf("Hijack through the recorder failed: %v", err)
	}

	_ = panda.Flush()
	time.Sleep(200 * time.Millisecond)
	if n := len(mock.recordsFor("/v1/ocpi")); n != 0 {
		t.Fatalf("captured %d messages for a hijacked connection, want 0", n)
	}
}

// A handler that writes nothing still produced a 200 on the wire, so
// that is what gets recorded. A handler that panicked produced no status
// at all, so the record says null rather than inventing success.
func TestMiddlewareStatusCodeEdgeCases(t *testing.T) {
	run := func(t *testing.T, h http.Handler) map[string]any {
		t.Helper()
		mock := startMockUpstream()
		defer mock.close()
		panda := startOCPI(t, mock)

		srv := httptest.NewServer(ocpi.Middleware(panda)(h))
		defer srv.Close()

		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ocpi/2.2/locations", nil)
		req.Header.Set(ocpi.HeaderPlatformID, "acme")
		req.Header.Set(ocpi.HeaderPlatformName, "Acme Mobility")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
		}
		waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
		return mock.recordsFor("/v1/ocpi")[0]
	}

	t.Run("silent handler records the implicit 200", func(t *testing.T) {
		rec := run(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		if got := rec["response_status_code"]; got == nil || got.(float64) != 200 {
			t.Fatalf("response_status_code = %v, want 200", got)
		}
	})

	t.Run("panicking handler records no status", func(t *testing.T) {
		rec := run(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			panic("handler exploded")
		}))
		v, present := rec["response_status_code"]
		if !present || v != nil {
			t.Fatalf("response_status_code = %v, want explicit null", v)
		}
	})
}

// The capture runs in a defer, so it must not swallow a handler's panic
// on its way out — the host's own recovery middleware still has to see it.
func TestMiddlewareLetsHandlerPanicsThrough(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	panda := startOCPI(t, mock)

	seen := make(chan any, 1)
	recovery := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() { seen <- recover() }()
			next.ServeHTTP(w, r)
		})
	}
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("handler exploded")
	})

	srv := httptest.NewServer(recovery(ocpi.Middleware(panda)(handler)))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/ocpi/2.2/cdrs", nil)
	req.Header.Set(ocpi.HeaderPlatformID, "acme")
	req.Header.Set(ocpi.HeaderPlatformName, "Acme Mobility")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_ = resp.Body.Close()
	}

	if got := <-seen; got != "handler exploded" {
		t.Fatalf("the host's recovery saw %v — the adapter must not swallow panics", got)
	}
	// And the exchange is still captured on the way out.
	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
}
