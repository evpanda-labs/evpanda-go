// End-to-end tests: drive the public API against an httptest upstream and
// assert capture, batching, redaction, routing, wire shape, compression,
// drop-oldest, graceful close, and that nothing panics.
//
// The direction wire values asserted here ("IN"/"OUT", "TO_CP"/"FROM_CP")
// are the exact strings the Atlas ingestion server validates
// (apps/atlas/internal/db/models_ocpi.go, internal/ingest/request.go).
// If these tests fail after an ingestion-contract change, fix the SDK to
// match the spec (apispec/ingestion-api.yaml) — not the tests.

package evpanda_test

import (
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	evpanda "github.com/evpanda-labs/evpanda-go"
)

type received struct {
	path    string
	headers http.Header
	records []map[string]any
}

type mockUpstream struct {
	mu       sync.Mutex
	server   *httptest.Server
	received []received
	status   int // mutable: change to make the upstream reject
}

func startMockUpstream() *mockUpstream {
	m := &mockUpstream{status: http.StatusOK}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reader io.Reader = r.Body
		switch r.Header.Get("content-encoding") {
		case "gzip":
			if gz, err := gzip.NewReader(r.Body); err == nil {
				defer gz.Close()
				reader = gz
			}
		case "zstd":
			if zr, err := zstd.NewReader(r.Body); err == nil {
				defer zr.Close()
				reader = zr
			}
		}
		raw, _ := io.ReadAll(reader)
		var body struct {
			Messages []map[string]any `json:"messages"`
		}
		_ = json.Unmarshal(raw, &body)

		m.mu.Lock()
		m.received = append(m.received, received{
			path:    r.URL.Path,
			headers: r.Header.Clone(),
			records: body.Messages,
		})
		status := m.status
		m.mu.Unlock()

		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"captured":0,"failed":0}`))
	}))
	return m
}

func (m *mockUpstream) setStatus(s int) {
	m.mu.Lock()
	m.status = s
	m.mu.Unlock()
}

func (m *mockUpstream) recordsFor(path string) []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []map[string]any
	for _, r := range m.received {
		if r.path == path {
			out = append(out, r.records...)
		}
	}
	return out
}

func (m *mockUpstream) postsFor(path string) []received {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []received
	for _, r := range m.received {
		if r.path == path {
			out = append(out, r)
		}
	}
	return out
}

func (m *mockUpstream) close() { m.server.Close() }

func waitFor(t *testing.T, predicate func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("waitFor: timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func ocpiConfig(endpoint string) evpanda.OCPIConfig {
	return evpanda.OCPIConfig{
		BaseConfig: evpanda.BaseConfig{
			Endpoint:      endpoint,
			APIKey:        "test-key",
			FlushInterval: 100 * time.Millisecond,
		},
	}
}

// makeOCPI is a valid OCPI message tagged with an index; it carries an
// off-allowlist header and a tracing header the allowlist keeps.
func makeOCPI(i int) evpanda.OCPIMessageInput {
	return evpanda.OCPIMessageInput{
		Identity: evpanda.RoamingIdentity{
			PlatformID:   "acme",
			PlatformName: "Acme Mobility",
			TenantID:     "t1",
			TenantName:   "Tenant One",
		},
		HTTP: evpanda.CapturedHTTP{
			Method:     "POST",
			URL:        "/ocpi/2.2/cdrs/" + strconv.Itoa(i),
			StatusCode: 200,
			RequestHeaders: map[string]string{
				"Authorization":    "Bearer SECRET",
				"X-Correlation-Id": strconv.Itoa(i),
			},
			ResponseHeaders: map[string]string{"content-type": "application/json"},
			RequestBody:     []byte("body-" + strconv.Itoa(i)),
		},
	}
}

func TestOCPIWireShape(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	panda, err := evpanda.StartOCPI(ocpiConfig(mock.server.URL))
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer panda.Close()

	for i := 0; i < 2; i++ {
		panda.CaptureInbound(makeOCPI(i))
	}
	panda.CaptureOutbound(makeOCPI(2))

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 3 }, 3*time.Second)

	recs := mock.recordsFor("/v1/ocpi")
	sort.Slice(recs, func(a, b int) bool {
		return recs[a]["url"].(string) < recs[b]["url"].(string)
	})

	// Routing + auth.
	for _, p := range mock.postsFor("/v1/ocpi") {
		if got := p.headers.Get("x-api-key"); got != "test-key" {
			t.Fatalf("x-api-key = %q, want test-key", got)
		}
	}

	tsRe := regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)
	for i, rec := range recs {
		// Direction wire values: exactly "IN"/"OUT".
		wantDir := "IN"
		if i == 2 {
			wantDir = "OUT"
		}
		if rec["direction"] != wantDir {
			t.Fatalf("direction = %v, want %q", rec["direction"], wantDir)
		}
		// Flat ingestion shape, ISO-millis-Z timestamp.
		if !tsRe.MatchString(rec["captured_at"].(string)) {
			t.Fatalf("captured_at %q not ISO-millis-Z", rec["captured_at"])
		}
		if rec["platform_id"] != "acme" || rec["http_method"] != "POST" {
			t.Fatalf("platform_id/http_method = %v / %v", rec["platform_id"], rec["http_method"])
		}
		if rec["response_status_code"].(float64) != 200 {
			t.Fatalf("response_status_code = %v", rec["response_status_code"])
		}
		// No protocol/truncated keys; explicit tenant values present.
		if _, ok := rec["protocol"]; ok {
			t.Fatal("record must not carry a protocol key")
		}
		if rec["tenant_id"] != "t1" || rec["tenant_name"] != "Tenant One" {
			t.Fatalf("tenant fields = %v / %v", rec["tenant_id"], rec["tenant_name"])
		}
		// Allowlist: Authorization dropped, x-correlation-id kept.
		reqHeaders := rec["request_headers"].(map[string]any)
		for k := range reqHeaders {
			if k == "Authorization" || k == "authorization" {
				t.Fatal("Authorization header survived the allowlist")
			}
		}
		if reqHeaders["X-Correlation-Id"] == nil {
			t.Fatalf("x-correlation-id was dropped: %v", reqHeaders)
		}
		// Body round-trips as base64.
		decoded, err := base64.StdEncoding.DecodeString(rec["request_body"].(string))
		if err != nil || len(decoded) == 0 {
			t.Fatalf("request_body round-trip failed: %v", err)
		}
		// Absent response body is explicit null, not omitted.
		v, present := rec["response_body"]
		if !present || v != nil {
			t.Fatalf("response_body = %v (present=%v), want explicit null", v, present)
		}
	}
}

func TestOCPIExtendedAllowlist(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.OCPIAllowedHeaders = []string{"X-Custom-Trace"}
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer panda.Close()

	msg := makeOCPI(0)
	msg.HTTP.RequestHeaders["X-Custom-Trace"] = "keep-me"
	panda.CaptureInbound(msg)

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)

	reqHeaders := mock.recordsFor("/v1/ocpi")[0]["request_headers"].(map[string]any)
	if reqHeaders["X-Custom-Trace"] != "keep-me" {
		t.Fatalf("extended allowlist ignored: %v", reqHeaders)
	}
	if _, ok := reqHeaders["Authorization"]; ok {
		t.Fatal("extending the allowlist must not admit Authorization")
	}
}

func TestOCPICredentialsTokenMasked(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	panda, err := evpanda.StartOCPI(ocpiConfig(mock.server.URL))
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer panda.Close()

	msg := makeOCPI(0)
	msg.HTTP.URL = "/ocpi/2.2/credentials"
	msg.HTTP.RequestBody = []byte(`{"token":"SECRET-A","url":"https://acme.example"}`)
	msg.HTTP.ResponseBody = []byte(`{"data":{"token":"SECRET-B"},"status_code":1000}`)
	panda.CaptureInbound(msg)

	// A non-credentials URL with a token must NOT be masked.
	other := makeOCPI(1)
	other.HTTP.RequestBody = []byte(`{"token":"NOT-A-CREDENTIAL"}`)
	panda.CaptureInbound(other)

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 2 }, 3*time.Second)

	recs := mock.recordsFor("/v1/ocpi")
	sort.Slice(recs, func(a, b int) bool {
		return recs[a]["url"].(string) < recs[b]["url"].(string)
	})
	// recs[0] = /ocpi/2.2/cdrs/1, recs[1] = /ocpi/2.2/credentials
	credReq, _ := base64.StdEncoding.DecodeString(recs[1]["request_body"].(string))
	var parsedReq map[string]any
	if err := json.Unmarshal(credReq, &parsedReq); err != nil {
		t.Fatalf("credentials request body not JSON after masking: %v", err)
	}
	if parsedReq["token"] != "[redacted]" {
		t.Fatalf("request token = %v, want [redacted]", parsedReq["token"])
	}
	if parsedReq["url"] != "https://acme.example" {
		t.Fatalf("masking corrupted sibling fields: %v", parsedReq)
	}
	credResp, _ := base64.StdEncoding.DecodeString(recs[1]["response_body"].(string))
	var parsedResp map[string]any
	_ = json.Unmarshal(credResp, &parsedResp)
	if parsedResp["data"].(map[string]any)["token"] != "[redacted]" {
		t.Fatalf("response data.token = %v, want [redacted]", parsedResp)
	}
	// Non-credentials body untouched.
	otherReq, _ := base64.StdEncoding.DecodeString(recs[0]["request_body"].(string))
	if string(otherReq) != `{"token":"NOT-A-CREDENTIAL"}` {
		t.Fatalf("non-credentials body was rewritten: %s", otherReq)
	}
}

func TestOCPIOversizeBodyDropped(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.MaxCaptureBytes = 16
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer panda.Close()

	big := makeOCPI(0)
	big.HTTP.RequestBody = make([]byte, 17) // over the cap → whole message dropped
	panda.CaptureInbound(big)
	panda.CaptureInbound(makeOCPI(1)) // "body-1" (6 bytes) → kept

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	if got := mock.recordsFor("/v1/ocpi")[0]["url"]; got != "/ocpi/2.2/cdrs/1" {
		t.Fatalf("survivor = %v, want the small message", got)
	}
}

func TestOCPPSessionAndWireShape(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	panda, err := evpanda.StartOCPP(evpanda.OCPPConfig{
		BaseConfig: evpanda.BaseConfig{
			Endpoint:      mock.server.URL,
			APIKey:        "test-key",
			FlushInterval: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("StartOCPP: %v", err)
	}
	defer panda.Close()

	sess := panda.Connection(evpanda.ChargerIdentity{ChargerID: "CP-001"})
	if sess.ConnectionID == "" {
		t.Fatal("session must mint a connection id")
	}
	sess.Message([]byte(`[2,"id","BootNotification",{}]`), evpanda.FromCP)
	sess.Message([]byte(`[3,"id",{}]`), evpanda.ToCP)
	sess.Disconnect()

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpp")) == 4 }, 3*time.Second)

	recs := mock.recordsFor("/v1/ocpp")
	// Order: CONNECT, MESSAGE, MESSAGE, DISCONNECT.
	wantEvents := []float64{1, 2, 2, 0}
	wantDirs := []any{nil, "FROM_CP", "TO_CP", nil}
	for i, rec := range recs {
		if rec["charger_id"] != "CP-001" {
			t.Fatalf("charger_id = %v", rec["charger_id"])
		}
		if rec["connection_id"] != sess.ConnectionID {
			t.Fatalf("connection_id = %v, want %v", rec["connection_id"], sess.ConnectionID)
		}
		// event_type is an int and never null — 0 (DISCONNECT) included.
		et, present := rec["event_type"]
		if !present || et == nil || et.(float64) != wantEvents[i] {
			t.Fatalf("event_type[%d] = %v, want %v", i, et, wantEvents[i])
		}
		// Direction: exact wire strings on MESSAGE, explicit null otherwise.
		dir, present := rec["direction"]
		if !present {
			t.Fatalf("direction[%d] missing — must be explicit null or a value", i)
		}
		if dir != wantDirs[i] {
			t.Fatalf("direction[%d] = %v, want %v", i, dir, wantDirs[i])
		}
		// Frame: base64 on MESSAGE, explicit null otherwise.
		frame, present := rec["raw_frame"]
		if !present {
			t.Fatalf("raw_frame[%d] missing — must be explicit null or a value", i)
		}
		if wantEvents[i] == 2 && frame == nil {
			t.Fatalf("raw_frame[%d] null on a MESSAGE event", i)
		}
		if wantEvents[i] != 2 && frame != nil {
			t.Fatalf("raw_frame[%d] = %v on a non-MESSAGE event", i, frame)
		}
		if rec["tenant_id"] != nil {
			t.Fatalf("tenant_id = %v, want explicit null", rec["tenant_id"])
		}
	}

	frame, _ := base64.StdEncoding.DecodeString(recs[1]["raw_frame"].(string))
	if string(frame) != `[2,"id","BootNotification",{}]` {
		t.Fatalf("raw_frame round-trip failed: %s", frame)
	}
}

func TestOCPPPrimitivesValidation(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	panda, err := evpanda.StartOCPP(evpanda.OCPPConfig{
		BaseConfig: evpanda.BaseConfig{
			Endpoint:      mock.server.URL,
			APIKey:        "test-key",
			FlushInterval: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("StartOCPP: %v", err)
	}
	defer panda.Close()

	valid := evpanda.ChargerIdentity{ChargerID: "CP-002"}

	// Dropped: missing data / missing direction / invalid identity.
	panda.CaptureMessage(evpanda.OCPPMessageInput{Identity: valid, ConnectionID: "c1", Direction: evpanda.FromCP})
	panda.CaptureMessage(evpanda.OCPPMessageInput{Identity: valid, ConnectionID: "c1", Data: []byte("x")})
	panda.CaptureMessage(evpanda.OCPPMessageInput{ConnectionID: "c1", Data: []byte("x"), Direction: evpanda.FromCP})
	// Tenant all-or-nothing: only one of the pair → dropped.
	panda.CaptureConnect(evpanda.OCPPMessageInput{
		Identity:     evpanda.ChargerIdentity{ChargerID: "CP-003", TenantID: "t1"},
		ConnectionID: "c2",
	})
	// Kept.
	panda.CaptureMessage(evpanda.OCPPMessageInput{
		Identity: valid, ConnectionID: "c1", Data: []byte("ok"), Direction: evpanda.FromCP,
	})

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpp")) == 1 }, 3*time.Second)
	time.Sleep(300 * time.Millisecond) // one more flush cycle: nothing else arrives
	if n := len(mock.recordsFor("/v1/ocpp")); n != 1 {
		t.Fatalf("want exactly 1 surviving record, got %d", n)
	}
}

func TestGzipAndChunking(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.Compression = "gzip" // exercise the opt-in gzip path explicitly
	cfg.BufferCapacity = 100_000
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer panda.Close()

	const n = 2500
	for i := 0; i < n; i++ {
		panda.CaptureInbound(makeOCPI(i))
	}

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == n }, 8*time.Second)

	// Chunked at ≤1000 per POST → ceil(2500/1000) = 3 requests.
	posts := mock.postsFor("/v1/ocpi")
	if len(posts) != 3 {
		t.Fatalf("want 3 posts, got %d", len(posts))
	}
	for _, p := range posts {
		if len(p.records) > 1000 {
			t.Fatalf("post had %d records (>1000)", len(p.records))
		}
		if p.headers.Get("content-encoding") != "gzip" {
			t.Fatalf("post not gzip-encoded")
		}
	}

	// FIFO order preserved across the chunked POSTs.
	for i, rec := range mock.recordsFor("/v1/ocpi") {
		if got := rec["url"]; got != "/ocpi/2.2/cdrs/"+strconv.Itoa(i) {
			t.Fatalf("order broken at %d: %v", i, got)
		}
	}
}

func TestDropOldest(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.BufferCapacity = 5
	cfg.FlushInterval = 60 * time.Second // no auto flush during the test
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer panda.Close()

	for i := 0; i < 12; i++ { // 0..11
		panda.CaptureInbound(makeOCPI(i))
	}
	if err := panda.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 5 }, 3*time.Second)

	var urls []string
	for _, rec := range mock.recordsFor("/v1/ocpi") {
		urls = append(urls, rec["url"].(string))
	}
	sort.Strings(urls)
	want := []string{
		"/ocpi/2.2/cdrs/10",
		"/ocpi/2.2/cdrs/11",
		"/ocpi/2.2/cdrs/7",
		"/ocpi/2.2/cdrs/8",
		"/ocpi/2.2/cdrs/9",
	}
	for i := range want {
		if urls[i] != want[i] {
			t.Fatalf("survivors = %v, want %v", urls, want)
		}
	}
}

func TestFlushOnClose(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.FlushInterval = 60 * time.Second // never auto-flushes within the test
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}

	for i := 0; i < 4; i++ {
		panda.CaptureInbound(makeOCPI(i))
	}
	if len(mock.recordsFor("/v1/ocpi")) != 0 {
		t.Fatalf("nothing should be sent yet, got %d", len(mock.recordsFor("/v1/ocpi")))
	}

	if err := panda.Close(); err != nil { // graceful drain, clean → nil
		t.Fatalf("Close on a clean drain returned %v, want nil", err)
	}

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 4 }, 3*time.Second)

	// Post-close captures are safe no-ops.
	panda.CaptureInbound(makeOCPI(99))
	if err := panda.Close(); err != nil { // idempotent
		t.Fatalf("second Close returned %v, want nil", err)
	}
}

func TestNeverPanicsWhenUpstreamFails(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	mock.setStatus(http.StatusBadRequest) // permanent reject → dropped
	cfg := ocpiConfig(mock.server.URL)
	cfg.FlushInterval = 60 * time.Second
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer panda.Close()

	// Capture during a failing upstream — must not panic.
	for i := 0; i < 3; i++ {
		panda.CaptureInbound(makeOCPI(i))
	}
	// Malformed customer input — must not panic either.
	panda.CaptureInbound(evpanda.OCPIMessageInput{}) // invalid identity
	panda.CaptureInbound(makeOCPI(99))               // still usable

	if err := panda.Flush(); err != nil { // resolves even though the upstream 400s
		t.Fatalf("Flush: %v", err)
	}

	if len(mock.postsFor("/v1/ocpi")) == 0 {
		t.Fatal("expected at least one delivery attempt")
	}
	// Still usable afterwards.
	panda.CaptureInbound(makeOCPI(100))
	_ = panda.Flush()
}

// A bad config must return an inert (no-op) client plus the error — never
// a nil pointer, never a panic.
func TestBadConfigIsInert(t *testing.T) {
	ocpi, err := evpanda.StartOCPI(evpanda.OCPIConfig{
		BaseConfig: evpanda.BaseConfig{Endpoint: "not-a-url", APIKey: "k"},
	})
	if err == nil {
		t.Fatal("StartOCPI on a bad config must return an error")
	}
	if ocpi == nil {
		t.Fatal("StartOCPI must never return nil")
	}
	ocpi.CaptureInbound(makeOCPI(1))
	if err := ocpi.Flush(); err != nil {
		t.Fatalf("inert Flush returned %v, want nil", err)
	}
	if err := ocpi.Close(); err != nil {
		t.Fatalf("inert Close returned %v, want nil", err)
	}

	ocpp, err := evpanda.StartOCPP(evpanda.OCPPConfig{
		BaseConfig: evpanda.BaseConfig{Endpoint: "not-a-url", APIKey: "k"},
	})
	if err == nil {
		t.Fatal("StartOCPP on a bad config must return an error")
	}
	sess := ocpp.Connection(evpanda.ChargerIdentity{ChargerID: "CP-001"}) // safe on inert
	sess.Message([]byte("x"), evpanda.FromCP)
	sess.Disconnect()
	if err := ocpp.Close(); err != nil {
		t.Fatalf("inert Close returned %v, want nil", err)
	}
}

// APIKey falls back to EVPANDA_API_KEY; one of the two must be set.
func TestAPIKeyFromEnv(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	base := evpanda.OCPIConfig{
		BaseConfig: evpanda.BaseConfig{Endpoint: mock.server.URL},
	}

	// Neither APIKey nor the env var → error, inert client.
	t.Setenv("EVPANDA_API_KEY", "")
	if _, err := evpanda.StartOCPI(base); err == nil {
		t.Fatal("StartOCPI must error when no API key is set anywhere")
	}

	// Env var set, APIKey empty → resolves from the environment.
	t.Setenv("EVPANDA_API_KEY", "env-key")
	panda, err := evpanda.StartOCPI(base)
	if err != nil {
		t.Fatalf("StartOCPI with EVPANDA_API_KEY set returned %v", err)
	}
	defer panda.Close()

	panda.CaptureInbound(makeOCPI(1))
	_ = panda.Flush()
	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	if got := mock.postsFor("/v1/ocpi")[0].headers.Get("x-api-key"); got != "env-key" {
		t.Fatalf("x-api-key = %q, want env-key", got)
	}
}

// DrainTimeout: 0 ⇒ default (ok); a positive value below the 5s minimum
// is rejected (inert client + error).
func TestDrainTimeoutMinimum(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	base := evpanda.OCPIConfig{
		BaseConfig: evpanda.BaseConfig{Endpoint: mock.server.URL, APIKey: "k"},
	}

	base.DrainTimeout = 0 // ⇒ default 10s
	if _, err := evpanda.StartOCPI(base); err != nil {
		t.Fatalf("DrainTimeout 0 should use the default, got error: %v", err)
	}

	base.DrainTimeout = 2 * time.Second // below the 5s minimum
	if _, err := evpanda.StartOCPI(base); err == nil {
		t.Fatal("DrainTimeout below 5s must be rejected")
	}

	base.DrainTimeout = 5 * time.Second // exactly the minimum → ok
	panda, err := evpanda.StartOCPI(base)
	if err != nil {
		t.Fatalf("DrainTimeout 5s should be accepted, got: %v", err)
	}
	_ = panda.Close()
}
