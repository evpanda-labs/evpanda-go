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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
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
	failNext int // when > 0, answer 503 and decrement
}

func startMockUpstream() *mockUpstream {
	m := &mockUpstream{status: http.StatusOK}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reader io.Reader = r.Body
		if r.Header.Get("content-encoding") == "zstd" {
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
		if m.failNext > 0 {
			m.failNext--
			status = http.StatusServiceUnavailable
		}
		m.mu.Unlock()

		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"captured":0,"failed":0}`))
	}))
	return m
}

// failFirst makes the next n requests answer 503, which is retryable —
// so the transport backs off and tries again rather than dropping.
func (m *mockUpstream) failFirst(n int) {
	m.mu.Lock()
	m.failNext = n
	m.mu.Unlock()
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
		Identity: evpanda.Platform{
			ID:         "acme",
			Name:       "Acme Mobility",
			TenantID:   "t1",
			TenantName: "Tenant One",
		},
		Data: evpanda.HTTPExchange{
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
		panda.CaptureInboundMessage(makeOCPI(i))
	}
	panda.CaptureOutboundMessage(makeOCPI(2))

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
	msg.Data.RequestHeaders["X-Custom-Trace"] = "keep-me"
	panda.CaptureInboundMessage(msg)

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
	msg.Data.URL = "/ocpi/2.2/credentials"
	msg.Data.RequestBody = []byte(`{"token":"SECRET-A","url":"https://acme.example"}`)
	msg.Data.ResponseBody = []byte(`{"data":{"token":"SECRET-B"},"status_code":1000}`)
	panda.CaptureInboundMessage(msg)

	// A non-credentials URL with a token must NOT be masked.
	other := makeOCPI(1)
	other.Data.RequestBody = []byte(`{"token":"NOT-A-CREDENTIAL"}`)
	panda.CaptureInboundMessage(other)

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
	big.Data.RequestBody = make([]byte, 17) // over the cap → whole message dropped
	panda.CaptureInboundMessage(big)
	panda.CaptureInboundMessage(makeOCPI(1)) // "body-1" (6 bytes) → kept

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

	sess := panda.Connection(evpanda.Charger{ID: "CP-001"})
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

	valid := evpanda.Charger{ID: "CP-002"}

	// Dropped: missing data / missing direction / invalid identity.
	panda.CaptureMessage(evpanda.OCPPMessageInput{Identity: valid, ConnectionID: "c1", Direction: evpanda.FromCP})
	panda.CaptureMessage(evpanda.OCPPMessageInput{Identity: valid, ConnectionID: "c1", Data: []byte("x")})
	panda.CaptureMessage(evpanda.OCPPMessageInput{ConnectionID: "c1", Data: []byte("x"), Direction: evpanda.FromCP})
	// Tenant all-or-nothing: only one of the pair → dropped.
	panda.CaptureConnect(evpanda.OCPPMessageInput{
		Identity:     evpanda.Charger{ID: "CP-003", TenantID: "t1"},
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

// A batch larger than the API's 1000-record cap is split across requests,
// in order, with each chunk compressed.
func TestBatchChunking(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.MaxBufferBytes = 8 << 20
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer panda.Close()

	const n = 2500
	for i := 0; i < n; i++ {
		panda.CaptureInboundMessage(makeOCPI(i))
	}

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == n }, 8*time.Second)

	// Chunked at ≤1000 records per POST, so 2500 messages take at least
	// 3 requests. The exact count is deliberately not pinned: the size
	// trigger flushes as soon as a full batch is waiting, so a fast
	// producer can hand the worker several partial drains.
	posts := mock.postsFor("/v1/ocpi")
	if len(posts) < 3 {
		t.Fatalf("want at least 3 posts, got %d", len(posts))
	}
	compressed := 0
	for _, p := range posts {
		if len(p.records) > 1000 {
			t.Fatalf("post had %d records (>1000)", len(p.records))
		}
		switch enc := p.headers.Get("content-encoding"); enc {
		case "zstd":
			compressed++
		case "": // identity — only legitimate for a sub-1KiB payload
			if len(p.records) > 5 {
				t.Fatalf("post of %d records went out uncompressed", len(p.records))
			}
		default:
			t.Fatalf("content-encoding = %q, want zstd or identity", enc)
		}
	}
	if compressed == 0 {
		t.Fatal("no post was compressed")
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

	// The smallest budget config accepts, against enough ~530-byte
	// messages to overflow it several times over. The assertion is the
	// drop-oldest property, not an exact count — pinning the count would
	// make the test a mirror of the size accounting.
	cfg := ocpiConfig(mock.server.URL)
	cfg.MaxBufferBytes = 64 << 10
	cfg.FlushInterval = 60 * time.Second // no auto flush during the test
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer panda.Close()

	const n = 400 // stays under the 1000-message size trigger
	for i := 0; i < n; i++ {
		panda.CaptureInboundMessage(makeOCPI(i))
	}
	if err := panda.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) > 0 }, 3*time.Second)
	time.Sleep(200 * time.Millisecond) // let any further delivery land

	var got []int
	for _, rec := range mock.recordsFor("/v1/ocpi") {
		idx, err := strconv.Atoi(strings.TrimPrefix(rec["url"].(string), "/ocpi/2.2/cdrs/"))
		if err != nil {
			t.Fatalf("unexpected url %v", rec["url"])
		}
		got = append(got, idx)
	}
	sort.Ints(got)

	if len(got) >= n {
		t.Fatalf("nothing was evicted: %v — the budget should not have held all %d", got, n)
	}
	if len(got) < 2 {
		t.Fatalf("survivors = %v — the budget should hold more than one message", got)
	}
	// Drop-oldest: the survivors are the newest contiguous run, ending at
	// the last message captured.
	for i, idx := range got {
		if want := n - len(got) + i; idx != want {
			t.Fatalf("survivors = %v, want the contiguous newest run ending at %d", got, n-1)
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
		panda.CaptureInboundMessage(makeOCPI(i))
	}
	if len(mock.recordsFor("/v1/ocpi")) != 0 {
		t.Fatalf("nothing should be sent yet, got %d", len(mock.recordsFor("/v1/ocpi")))
	}

	if err := panda.Close(); err != nil { // graceful drain, clean → nil
		t.Fatalf("Close on a clean drain returned %v, want nil", err)
	}

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 4 }, 3*time.Second)

	// Post-close captures are safe no-ops.
	panda.CaptureInboundMessage(makeOCPI(99))
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
		panda.CaptureInboundMessage(makeOCPI(i))
	}
	// Malformed customer input — must not panic either.
	panda.CaptureInboundMessage(evpanda.OCPIMessageInput{}) // invalid identity
	panda.CaptureInboundMessage(makeOCPI(99))               // still usable

	if err := panda.Flush(); err != nil { // resolves even though the upstream 400s
		t.Fatalf("Flush: %v", err)
	}

	if len(mock.postsFor("/v1/ocpi")) == 0 {
		t.Fatal("expected at least one delivery attempt")
	}
	// Still usable afterwards.
	panda.CaptureInboundMessage(makeOCPI(100))
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
	ocpi.CaptureInboundMessage(makeOCPI(1))
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
	sess := ocpp.Connection(evpanda.Charger{ID: "CP-001"}) // safe on inert
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

	panda.CaptureInboundMessage(makeOCPI(1))
	_ = panda.Flush()
	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	if got := mock.postsFor("/v1/ocpi")[0].headers.Get("x-api-key"); got != "env-key" {
		t.Fatalf("x-api-key = %q, want env-key", got)
	}
}

// Only Endpoint and APIKey are hard-required. A tunable field set out of
// range falls back to its default and keeps the client live — a typo must
// never silence capture — and the fallback is reported by default, with
// nothing switched on.
func TestOutOfRangeTunablesFallBackAndWarn(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	t.Setenv("EVPANDA_LOG", "")

	var logs bytes.Buffer
	cfg := evpanda.OCPIConfig{
		BaseConfig: evpanda.BaseConfig{
			Endpoint:        mock.server.URL,
			APIKey:          "test-key",
			FlushInterval:   100 * time.Millisecond,
			DrainTimeout:    2 * time.Second, // below the 5s minimum ⇒ default
			MaxBufferBytes:  -1,              // ⇒ default
			MaxCaptureBytes: -1,              // ⇒ default
			Logger:          slog.New(slog.NewTextHandler(&logs, nil)),
			// LogMode deliberately unset — this is the out-of-the-box path.
		},
	}

	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("out-of-range tunables must not fail Start: %v", err)
	}
	defer func() { _ = panda.Close() }()

	// Still live: capture and delivery work on the defaults.
	panda.CaptureInboundMessage(makeOCPI(1))
	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)

	for _, field := range []string{"DrainTimeout", "MaxBufferBytes", "MaxCaptureBytes"} {
		if !strings.Contains(logs.String(), field) {
			t.Fatalf("no warning logged for %s: %s", field, logs.String())
		}
	}
}

// LogModeSilent means silent: nothing at all, even with junk config and an
// upstream that rejects every batch.
func TestSilentMode(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	mock.setStatus(http.StatusBadRequest)

	var logs bytes.Buffer
	cfg := ocpiConfig(mock.server.URL)
	cfg.DrainTimeout = time.Second // out of range ⇒ default, silently
	cfg.LogMode = evpanda.LogModeSilent
	cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	panda.CaptureInboundMessage(makeOCPI(1))
	_ = panda.Close()

	if logs.Len() != 0 {
		t.Fatalf("LogModeSilent still logged: %s", logs.String())
	}
	// Counters keep working regardless — silencing logs is not silencing
	// the SDK's own accounting.
	if got := panda.Stats().Captured; got != 1 {
		t.Fatalf("Captured = %d, want 1 even in silent mode", got)
	}
}

// EVPANDA_LOG is the operational escape hatch: silence the SDK with a
// restart, no code change.
func TestLogModeFromEnvironment(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	t.Setenv("EVPANDA_LOG", "silent")

	var logs bytes.Buffer
	cfg := ocpiConfig(mock.server.URL)
	cfg.DrainTimeout = time.Second // would warn in the default mode
	cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	_ = panda.Close()

	if logs.Len() != 0 {
		t.Fatalf("EVPANDA_LOG=silent did not silence the SDK: %s", logs.String())
	}
}

// Every drop path lands in its own counter, so "why am I seeing no data?"
// has a single-lookup answer.
func TestStatsCountEachDropReason(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.MaxCaptureBytes = 32
	cfg.FlushInterval = time.Hour // hold everything in the buffer
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}

	panda.CaptureInboundMessage(makeOCPI(1)) // good
	panda.CaptureInboundMessage(makeOCPI(2)) // good

	panda.CaptureInboundMessage(evpanda.OCPIMessageInput{}) // no identity
	half := makeOCPI(3)
	half.Identity.TenantID = "t1" // tenant pair broken ⇒ invalid
	half.Identity.TenantName = ""
	panda.CaptureInboundMessage(half)

	big := makeOCPI(4)
	big.Data.RequestBody = make([]byte, 64) // over the 32-byte cap
	panda.CaptureInboundMessage(big)

	got := panda.Stats()
	if got.Captured != 2 {
		t.Fatalf("Captured = %d, want 2", got.Captured)
	}
	if got.DroppedInvalid != 2 {
		t.Fatalf("DroppedInvalid = %d, want 2", got.DroppedInvalid)
	}
	if got.DroppedOversize != 1 {
		t.Fatalf("DroppedOversize = %d, want 1", got.DroppedOversize)
	}
	if got.TotalDropped() != 3 {
		t.Fatalf("TotalDropped = %d, want 3", got.TotalDropped())
	}
	if got.BufferedMessages != 2 || got.BufferBytes == 0 {
		t.Fatalf("buffer gauges = %d messages / %d bytes, want 2 and non-zero",
			got.BufferedMessages, got.BufferBytes)
	}

	// The totals survive Close, so the final tally is still readable.
	_ = panda.Close()
	after := panda.Stats()
	if after.Captured != 2 || after.TotalDropped() != 3 {
		t.Fatalf("counters after Close = %+v, want the lifetime totals", after)
	}
	if after.BufferedMessages != 0 {
		t.Fatalf("BufferedMessages after Close = %d, want 0", after.BufferedMessages)
	}
}

// An undeliverable batch is counted per message, not per batch.
func TestStatsCountUndeliverable(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	mock.setStatus(http.StatusBadRequest) // permanent ⇒ dropped, no retries

	cfg := ocpiConfig(mock.server.URL)
	cfg.FlushInterval = time.Hour
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer func() { _ = panda.Close() }()

	for i := range 3 {
		panda.CaptureInboundMessage(makeOCPI(i))
	}
	_ = panda.Flush()

	if got := panda.Stats().DroppedUndeliverable; got != 3 {
		t.Fatalf("DroppedUndeliverable = %d, want 3", got)
	}
}

// Closing logs a summary when something was lost, and stays quiet when
// the run was clean.
func TestShutdownSummary(t *testing.T) {
	run := func(t *testing.T, dropSomething bool) string {
		t.Helper()
		mock := startMockUpstream()
		defer mock.close()
		t.Setenv("EVPANDA_LOG", "")

		var logs bytes.Buffer
		cfg := ocpiConfig(mock.server.URL)
		cfg.Logger = slog.New(slog.NewTextHandler(&logs, nil))
		panda, err := evpanda.StartOCPI(cfg)
		if err != nil {
			t.Fatalf("StartOCPI: %v", err)
		}
		panda.CaptureInboundMessage(makeOCPI(1))
		if dropSomething {
			panda.CaptureInboundMessage(evpanda.OCPIMessageInput{}) // invalid
		}
		_ = panda.Close()
		return logs.String()
	}

	if got := run(t, true); !strings.Contains(got, "client closed") ||
		!strings.Contains(got, "invalid_identity=1") {
		t.Fatalf("a lossy run must log its tally on close: %q", got)
	}
	if got := run(t, false); got != "" {
		t.Fatalf("a clean run must close quietly in the default mode: %q", got)
	}
}

// A full batch flushes on the size trigger, without waiting out the
// interval — the producer signals the worker instead of it polling.
func TestBatchSizeTriggersImmediateFlush(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.FlushInterval = time.Hour // only the size trigger can fire
	cfg.MaxBufferBytes = 8 << 20
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer func() { _ = panda.Close() }()

	for i := range 1000 { // exactly one batch
		panda.CaptureInboundMessage(makeOCPI(i))
	}
	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1000 }, 3*time.Second)
}

// Shutdown drains against a caller-supplied context; an already-expired
// one reports ErrDrainIncomplete rather than blocking or dropping
// silently.
func TestShutdownHonoursContext(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.FlushInterval = time.Hour
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	panda.CaptureInboundMessage(makeOCPI(1))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := panda.Shutdown(ctx); !errors.Is(err, evpanda.ErrDrainIncomplete) {
		t.Fatalf("Shutdown with an expired context = %v, want ErrDrainIncomplete", err)
	}
	// Idempotent, and the second call reports the first call's result.
	if err := panda.Close(); err != nil {
		t.Fatalf("Close after Shutdown returned %v, want nil", err)
	}

	// A live client with room to drain finishes clean.
	panda2, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	panda2.CaptureInboundMessage(makeOCPI(2))
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := panda2.Shutdown(ctx2); err != nil {
		t.Fatalf("Shutdown on a clean drain returned %v, want nil", err)
	}
	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
}

// Close must leave nothing running: not the worker goroutine, and not the
// zstd encoder's own pool. A client per connection-heavy host would leak
// steadily otherwise.
func TestNoGoroutineLeakAfterClose(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	settle := func() int {
		// Let already-finished goroutines be reaped before counting.
		for range 20 {
			runtime.GC()
			time.Sleep(20 * time.Millisecond)
		}
		return runtime.NumGoroutine()
	}

	// Warm up the HTTP client and the mock server once, so their pooled
	// goroutines are part of the baseline rather than the measurement.
	warm, err := evpanda.StartOCPI(ocpiConfig(mock.server.URL))
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	warm.CaptureInboundMessage(makeOCPI(0))
	_ = warm.Close()
	baseline := settle()

	const clients = 10
	for i := range clients {
		// Every client spawns a zstd encoder, which owns goroutines of
		// its own — the thing most likely to leak on close.
		cfg := ocpiConfig(mock.server.URL)
		panda, err := evpanda.StartOCPI(cfg)
		if err != nil {
			t.Fatalf("StartOCPI: %v", err)
		}
		panda.CaptureInboundMessage(makeOCPI(i))
		if err := panda.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// A per-client leak would show up as a multiple of `clients`; allow a
	// small margin for the runtime's own transient goroutines.
	if got := settle(); got > baseline+clients {
		t.Fatalf("goroutines after %d closed clients = %d, baseline %d", clients, got, baseline)
	}
}

// Concurrent producers, flushers and a closer must not race or panic.
// Meaningful under -race, which is how CI runs the suite.
func TestConcurrentCaptureFlushClose(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.FlushInterval = 20 * time.Millisecond
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 200 {
				panda.CaptureInboundMessage(makeOCPI(g*200 + i))
			}
		}()
	}
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				_ = panda.Flush()
			}
		}()
	}
	wg.Wait()

	if err := panda.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Every capture either shipped or was dropped by the ring; nothing
	// may be left behind after a clean drain.
	if err := panda.Flush(); err != nil {
		t.Fatalf("Flush after Close: %v", err)
	}
}

// A retryable failure is retried, not dropped: the transport backs off and
// the batch still lands. This is the only test that exercises the backoff
// path, so it is also what keeps nextDelay honest.
func TestRetriesTransientFailure(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()
	mock.failFirst(1) // one 503, then success

	cfg := ocpiConfig(mock.server.URL)
	cfg.FlushInterval = time.Hour // only the explicit flush delivers
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer func() { _ = panda.Close() }()

	panda.CaptureInboundMessage(makeOCPI(1))
	_ = panda.Flush() // blocks through the backoff and the retry

	if n := len(mock.postsFor("/v1/ocpi")); n != 2 {
		t.Fatalf("delivery attempts = %d, want 2 (one 503, one success)", n)
	}
	if got := panda.Stats().DroppedUndeliverable; got != 0 {
		t.Fatalf("DroppedUndeliverable = %d — a retried batch must not count as lost", got)
	}
	if n := len(mock.recordsFor("/v1/ocpi")); n != 2 {
		t.Fatalf("records seen = %d, want the same message twice (503 then 200)", n)
	}
}

// zstd is the only codec, and payloads below the compression floor go out
// uncompressed rather than paying the CPU for nothing.
func TestCompressionIsZstdAboveTheFloor(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	cfg := ocpiConfig(mock.server.URL)
	cfg.MaxBufferBytes = 8 << 20
	panda, err := evpanda.StartOCPI(cfg)
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer func() { _ = panda.Close() }()

	// One small message stays under the 1 KiB floor.
	panda.CaptureInboundMessage(makeOCPI(0))
	_ = panda.Flush()
	if got := mock.postsFor("/v1/ocpi")[0].headers.Get("content-encoding"); got != "" {
		t.Fatalf("a sub-1KiB payload went out as %q, want identity", got)
	}

	// Enough messages to clear the floor.
	for i := range 200 {
		panda.CaptureInboundMessage(makeOCPI(i))
	}
	_ = panda.Flush()
	posts := mock.postsFor("/v1/ocpi")
	last := posts[len(posts)-1]
	if got := last.headers.Get("content-encoding"); got != "zstd" {
		t.Fatalf("content-encoding = %q, want zstd", got)
	}
	// The mock decoded it, so the round trip actually works.
	if len(last.records) != 200 {
		t.Fatalf("zstd payload decoded to %d records, want 200", len(last.records))
	}
}

// The SDK copies bodies at capture, so a host may reuse its read buffer
// the moment the call returns. Asserted through the wire, since that is
// where corruption would actually show up.
func TestCapturedBodiesDoNotAliasTheCaller(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	panda, err := evpanda.StartOCPI(ocpiConfig(mock.server.URL))
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer func() { _ = panda.Close() }()

	// A pooled reader would look exactly like this: one buffer, refilled.
	buf := []byte(`{"id":"cdr-1"}`)
	msg := makeOCPI(0)
	msg.Data.SetRequestBody(buf)
	msg.Data.ResponseBody = buf // assigned, not Set — must be safe too
	panda.CaptureInboundMessage(msg)
	copy(buf, []byte(`{"id":"XXXXX"}`)) // host reuses it immediately

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	rec := mock.recordsFor("/v1/ocpi")[0]
	for _, key := range []string{"request_body", "response_body"} {
		got, err := base64.StdEncoding.DecodeString(rec[key].(string))
		if err != nil {
			t.Fatalf("%s not base64: %v", key, err)
		}
		if string(got) != `{"id":"cdr-1"}` {
			t.Fatalf("%s followed the caller's buffer: %s", key, got)
		}
	}
}

// The same guarantee for OCPP frames, which have no setter of their own.
func TestCapturedFramesDoNotAliasTheCaller(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	panda, err := evpanda.StartOCPP(evpanda.OCPPConfig{
		BaseConfig: evpanda.BaseConfig{
			Endpoint: mock.server.URL, APIKey: "test-key",
			FlushInterval: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("StartOCPP: %v", err)
	}
	defer func() { _ = panda.Close() }()

	frame := []byte(`[2,"id","BootNotification",{}]`)
	sess := panda.Connection(evpanda.Charger{ID: "CP-001"})
	sess.Message(frame, evpanda.FromCP)
	copy(frame, bytes.Repeat([]byte("X"), len(frame)))

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpp")) == 2 }, 3*time.Second)
	for _, rec := range mock.recordsFor("/v1/ocpp") {
		if rec["raw_frame"] == nil {
			continue // the CONNECT event
		}
		got, _ := base64.StdEncoding.DecodeString(rec["raw_frame"].(string))
		if string(got) != `[2,"id","BootNotification",{}]` {
			t.Fatalf("frame followed the caller's buffer: %s", got)
		}
	}
}

// Empty bodies are absent on the wire, not empty strings — the ingestion
// contract distinguishes them.
func TestEmptyBodiesSerializeAsNull(t *testing.T) {
	mock := startMockUpstream()
	defer mock.close()

	panda, err := evpanda.StartOCPI(ocpiConfig(mock.server.URL))
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer func() { _ = panda.Close() }()

	msg := makeOCPI(0)
	msg.Data.SetRequestBody([]byte{}) // explicitly empty
	msg.Data.SetResponseBody(nil)
	msg.Data.StatusCode = 0 // absent status
	panda.CaptureInboundMessage(msg)

	waitFor(t, func() bool { return len(mock.recordsFor("/v1/ocpi")) == 1 }, 3*time.Second)
	rec := mock.recordsFor("/v1/ocpi")[0]
	for _, key := range []string{"request_body", "response_body", "response_status_code"} {
		v, present := rec[key]
		if !present || v != nil {
			t.Fatalf("%s = %v (present=%v), want explicit null", key, v, present)
		}
	}
}
