package evpanda

// Hand-rolled transport over net/http. Body: JSON; zstd by default, gzip
// when configured, identity for tiny payloads. It owns the bounded retry:
// 200 or 400/401/413 is terminal, 5xx and network errors back off; the
// caller never retries. It never panics.
//
// The POST /v1/{protocol} call lives in apiClient.post — no generated
// client, which would pull heavy transitive dependencies into customer
// production for two endpoints.

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Retry backoff bounds. Fixed by design — not configurable.
const (
	backoffBase        = 500 * time.Millisecond
	backoffMax         = 30 * time.Second
	backoffMaxAttempts = 5
)

// nextDelay returns the capped-exponential, fully-jittered backoff before
// a retry attempt.
//
// The shift is guarded: backoffBase<<attempt overflows int64 somewhere
// past attempt 34 and goes negative, which rand.Int64N rejects with a
// panic. The send loop never gets that far today, but this runs on the
// worker goroutine, where a panic would be a very expensive way to
// discover that someone raised backoffMaxAttempts.
func nextDelay(attempt int) time.Duration {
	capped := backoffMax
	if attempt >= 0 && attempt < 32 {
		if d := backoffBase << attempt; d > 0 && d < backoffMax {
			capped = d
		}
	}
	return time.Duration(rand.Int64N(int64(capped)))
}

// requestTimeout caps a single POST attempt, so a hung connection still
// feeds the backoff instead of stalling the worker.
const requestTimeout = 30 * time.Second

const (
	headerContentType     = "Content-Type"
	headerContentEncoding = "Content-Encoding"
	headerAPIKey          = "X-API-Key"
	contentTypeJSON       = "application/json"
)

type contentEncoding string

const (
	encodingIdentity contentEncoding = "identity"
	encodingGzip     contentEncoding = "gzip"
	encodingZstd     contentEncoding = "zstd"
)

// compressMinBytes is the size below which compression isn't worth the
// CPU; the payload is sent as-is.
const compressMinBytes = 1024

// ── Ingestion wire records ───────────────────────────────────────────────
//
// The exact request payload shapes the ingestion service accepts — keep
// these in lock-step with apispec/ingestion-api.yaml and the Node/Python
// SDKs. Optional fields are pointers or json.RawMessage without omitempty,
// so an absent value serializes as JSON null and never as a Go zero value.

type ocpiIngest struct {
	CapturedAt         string          `json:"captured_at"`
	PlatformID         string          `json:"platform_id"`
	PlatformName       string          `json:"platform_name"`
	TenantID           *string         `json:"tenant_id"`
	TenantName         *string         `json:"tenant_name"`
	Direction          string          `json:"direction"`
	HTTPMethod         string          `json:"http_method"`
	URL                string          `json:"url"`
	ResponseStatusCode *int            `json:"response_status_code"`
	RequestHeaders     json.RawMessage `json:"request_headers"`
	RequestBody        *string         `json:"request_body"`
	ResponseHeaders    json.RawMessage `json:"response_headers"`
	ResponseBody       *string         `json:"response_body"`
}

type ocppIngest struct {
	ChargerID    string  `json:"charger_id"`
	ConnectionID string  `json:"connection_id"`
	TenantID     *string `json:"tenant_id"`
	TenantName   *string `json:"tenant_name"`
	CapturedAt   string  `json:"captured_at"`
	EventType    int     `json:"event_type"`
	Direction    *string `json:"direction"`
	RawFrame     *string `json:"raw_frame"`
}

// record maps an OCPI capture onto its flat ingestion record. It is the
// message implementation for ocpiMessage.
func (m ocpiMessage) record(capturedAt string) any {
	return ocpiIngest{
		CapturedAt:         capturedAt,
		PlatformID:         m.Identity.PlatformID,
		PlatformName:       m.Identity.PlatformName,
		TenantID:           optStr(m.Identity.TenantID),
		TenantName:         optStr(m.Identity.TenantName),
		Direction:          string(m.Direction),
		HTTPMethod:         m.Data.Method,
		URL:                m.Data.URL,
		ResponseStatusCode: optInt(m.Data.StatusCode),
		RequestHeaders:     headersJSON(m.Data.RequestHeaders),
		RequestBody:        bodyB64(m.Data.RequestBody),
		ResponseHeaders:    headersJSON(m.Data.ResponseHeaders),
		ResponseBody:       bodyB64(m.Data.ResponseBody),
	}
}

// record maps an OCPP capture onto its flat ingestion record. It is the
// message implementation for ocppMessage.
func (m ocppMessage) record(capturedAt string) any {
	return ocppIngest{
		ChargerID:    m.Identity.ChargerID,
		ConnectionID: m.ConnectionID,
		TenantID:     optStr(m.Identity.TenantID),
		TenantName:   optStr(m.Identity.TenantName),
		CapturedAt:   capturedAt,
		EventType:    int(m.EventType),
		Direction:    optStr(string(m.Direction)),
		RawFrame:     bodyB64(m.Payload),
	}
}

// headersJSON marshals a header map to a JSON object, or nil to send null
// when empty.
func headersJSON(h map[string]string) json.RawMessage {
	if len(h) == 0 {
		return nil
	}
	b, err := json.Marshal(h)
	if err != nil {
		return nil
	}
	return b
}

// bodyB64 base64-encodes a body or frame, or returns nil to send null
// when empty. Every byte payload the SDK ships goes through this — OCPI
// HTTP bodies and OCPP wire frames alike. The ingest server decodes
// before persistence, so consumers see plain UTF-8; encoding keeps the
// wire contract uniform across protocols and binary-safe.
func bodyB64(b []byte) *string {
	if len(b) == 0 {
		return nil
	}
	s := base64.StdEncoding.EncodeToString(b)
	return &s
}

// optStr returns nil for the empty string, so the field serializes as
// null rather than "".
func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// optInt returns nil for 0 (treated as absent), so the field serializes
// as null rather than 0.
func optInt(v int) *int {
	if v == 0 {
		return nil
	}
	return &v
}

// ingestBody is the request envelope: {"messages": [ <record>, ... ]}.
type ingestBody struct {
	Messages []any `json:"messages"`
}

// serialize maps the batch onto the ingestion wire records and encodes it
// inside the request envelope.
func serialize(batch []bufferedMessage) ([]byte, error) {
	records := make([]any, 0, len(batch))
	for _, e := range batch {
		records = append(records, e.message.record(e.capturedAt))
	}
	return json.Marshal(ingestBody{Messages: records})
}

// ── HTTP ─────────────────────────────────────────────────────────────────

type apiClient struct {
	endpoint string
	apiKey   string
	http     *http.Client
}

// post issues one POST /v1/{protocol}, drains the response, and returns
// the status code.
func (c *apiClient) post(ctx context.Context, protocol Protocol, body []byte, encoding contentEncoding) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.endpoint+"/v1/"+string(protocol), bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set(headerContentType, contentTypeJSON)
	req.Header.Set(headerAPIKey, c.apiKey)
	if encoding != encodingIdentity {
		req.Header.Set(headerContentEncoding, string(encoding))
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body) // drain so the connection can be reused
	return resp.StatusCode, nil
}

type transport struct {
	client *apiClient
	// zstdEnc is non-nil for zstd (the default); nil means gzip. It is
	// safe for concurrent EncodeAll and must be closed on shutdown, since
	// it holds worker goroutines of its own.
	zstdEnc *zstd.Encoder
	// logger records dropped batches; nil means silent.
	logger  *slog.Logger
	logMode LogMode
	stats   *stats
}

func newTransport(c resolvedConfig, st *stats) *transport {
	t := &transport{
		client: &apiClient{
			endpoint: c.endpoint,
			apiKey:   c.apiKey,
			http:     &http.Client{}, // per-attempt deadline comes from the context
		},
		logger:  c.logger,
		logMode: c.logMode,
		stats:   st,
	}
	if c.compression == CompressionZstd {
		// Fall back to gzip if the encoder won't build.
		if enc, err := zstd.NewWriter(nil); err == nil {
			t.zstdEnc = enc
		}
	}
	return t
}

// close releases the transport's resources. The worker calls it once, at
// the end of shutdown, when no send can still be running.
func (t *transport) close() {
	if t.zstdEnc != nil {
		_ = t.zstdEnc.Close()
		t.zstdEnc = nil
	}
	t.client.http.CloseIdleConnections()
}

// compress encodes raw with the configured codec, degrading to identity
// on any failure or for payloads below compressMinBytes.
func (t *transport) compress(raw []byte) ([]byte, contentEncoding) {
	if len(raw) < compressMinBytes {
		return raw, encodingIdentity
	}
	if t.zstdEnc != nil {
		return t.zstdEnc.EncodeAll(raw, nil), encodingZstd
	}
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(raw); err != nil {
		return raw, encodingIdentity
	}
	if err := w.Close(); err != nil {
		return raw, encodingIdentity
	}
	return b.Bytes(), encodingGzip
}

// send serializes, compresses, and POSTs the batch with bounded retry:
// 200 or 400/401/413 is terminal; 5xx and network errors back off and
// retry. A batch that can't be delivered is dropped — loss is acceptable
// by design, and the alternative is unbounded memory in the host.
func (t *transport) send(ctx context.Context, protocol Protocol, batch []bufferedMessage) {
	if len(batch) == 0 {
		return
	}
	raw, err := serialize(batch)
	if err != nil {
		t.logDrop(protocol, len(batch), "batch could not be serialized")
		return
	}
	body, encoding := t.compress(raw)

	lastStatus := 0
	for attempt := range backoffMaxAttempts {
		if attempt > 0 {
			select {
			case <-time.After(nextDelay(attempt)):
			case <-ctx.Done():
				t.logDrop(protocol, len(batch), "context cancelled before retry")
				return
			}
		}

		status, err := t.client.post(ctx, protocol, body, encoding)
		if err != nil {
			lastStatus = 0
			continue // network error or timeout: retryable
		}
		lastStatus = status

		// 200 accepted; 400/401/413 permanent (drop, never retry — only
		// these three per the ingestion contract); anything else retries.
		switch status {
		case http.StatusOK:
			return
		case http.StatusBadRequest, http.StatusUnauthorized,
			http.StatusRequestEntityTooLarge:
			t.logDrop(protocol, len(batch), fmt.Sprintf("permanent rejection: HTTP %d", status))
			return
		}
	}
	if lastStatus != 0 {
		t.logDrop(protocol, len(batch), fmt.Sprintf("retries exhausted (last HTTP %d)", lastStatus))
	} else {
		t.logDrop(protocol, len(batch), "retries exhausted (network error / timeout)")
	}
}

// logDrop counts a dropped batch, and logs it per-occurrence only in
// LogModeDebug. In the default mode the worker's once-a-minute health
// line reports the same loss with bounded volume — an outage would
// otherwise emit a line every flush interval, for as long as it lasts,
// across every client at once.
func (t *transport) logDrop(protocol Protocol, n int, reason string) {
	t.stats.countDrop(dropUndeliverable, n)
	if t.logger == nil || t.logMode != LogModeDebug {
		return
	}
	t.logger.Warn("evpanda: dropped batch (delivery failed)",
		"protocol", string(protocol),
		"messages", n,
		"reason", reason,
	)
}
