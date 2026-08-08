package evpanda

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

// Retry backoff bounds (fixed, not configurable).
const (
	backoffBase        = 500 * time.Millisecond
	backoffMax         = 30 * time.Second
	backoffMaxAttempts = 5
)

// nextDelay returns the capped-exponential, fully-jittered backoff before
// a retry attempt.
func nextDelay(attempt int) time.Duration {
	capped := min(backoffBase<<attempt, backoffMax)
	return time.Duration(rand.Int64N(int64(capped)))
}

// requestTimeout caps a single POST attempt.
const requestTimeout = 30 * time.Second

const (
	headerContentType     = "Content-Type"
	headerContentEncoding = "Content-Encoding"
	headerAPIKey          = "X-API-Key"
	contentTypeJSON       = "application/json"
)

type apiClient struct {
	endpoint string
	apiKey   string
	http     *http.Client
}

// post issues one POST /v1/{protocol}, drains the response, and returns
// the status code.
func (c *apiClient) post(ctx context.Context, protocol protocol, body []byte, encoding contentEncoding) (int, error) {
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

type contentEncoding string

const (
	encodingIdentity contentEncoding = "identity"
	encodingGzip     contentEncoding = "gzip"
	encodingZstd     contentEncoding = "zstd"
)

// compressMinBytes is the size below which a payload is sent uncompressed.
const compressMinBytes = 1024

// ocpiIngest and ocppIngest are the exact request payload shapes the
// ingestion service accepts. Keep them in lock-step with that service.
// Optional fields are pointers / json.RawMessage without omitempty so an
// absent value serializes as JSON null — never a Go zero value ("" / 0).
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

// headersJSON marshals a header map to a JSON object, or nil to omit it
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

// bodyB64 base64-encodes a body/frame to a *string, or nil to omit it
// when empty.
func bodyB64(b []byte) *string {
	if len(b) == 0 {
		return nil
	}
	s := base64.StdEncoding.EncodeToString(b)
	return &s
}

func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// optInt returns nil for 0 (treated as "absent") so the field serializes
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

// serialize maps the batch onto the ingestion wire structs and JSON-encodes
// it inside the request envelope.
func serialize(batch []bufferedMessage) ([]byte, error) {
	records := make([]any, 0, len(batch))
	for _, e := range batch {
		switch m := e.message.(type) {
		case ocpiMessage:
			records = append(records, ocpiIngest{
				CapturedAt:         e.capturedAt,
				PlatformID:         m.Identity.PlatformID,
				PlatformName:       m.Identity.PlatformName,
				TenantID:           optStr(m.Identity.TenantID),
				TenantName:         optStr(m.Identity.TenantName),
				Direction:          string(m.Direction),
				HTTPMethod:         m.HTTP.Method,
				URL:                m.HTTP.URL,
				ResponseStatusCode: optInt(m.HTTP.StatusCode),
				RequestHeaders:     headersJSON(m.HTTP.RequestHeaders),
				RequestBody:        bodyB64(m.HTTP.RequestBody),
				ResponseHeaders:    headersJSON(m.HTTP.ResponseHeaders),
				ResponseBody:       bodyB64(m.HTTP.ResponseBody),
			})
		case ocppMessage:
			var direction *string
			if m.Direction != nil {
				s := string(*m.Direction)
				direction = &s
			}
			records = append(records, ocppIngest{
				ChargerID:    m.Identity.ChargerID,
				ConnectionID: m.ConnectionID,
				TenantID:     optStr(m.Identity.TenantID),
				TenantName:   optStr(m.Identity.TenantName),
				CapturedAt:   e.capturedAt,
				EventType:    int(m.EventType),
				Direction:    direction,
				RawFrame:     bodyB64(m.Payload),
			})
		}
	}
	return json.Marshal(ingestBody{Messages: records})
}

type transport struct {
	client *apiClient
	// zstdEnc is non-nil for zstd (the default); nil means gzip. It is
	// safe for concurrent EncodeAll.
	zstdEnc *zstd.Encoder
	// logger records dropped batches; nil means silent.
	logger *slog.Logger
}

func newTransport(c resolvedConfig) *transport {
	t := &transport{
		client: &apiClient{
			endpoint: c.endpoint,
			apiKey:   c.apiKey,
			http:     &http.Client{}, // per-attempt deadline via context
		},
		logger: c.logger,
	}
	if c.compression == "zstd" {
		// Fall back to gzip if the encoder won't build.
		if enc, err := zstd.NewWriter(nil); err == nil {
			t.zstdEnc = enc
		}
	}
	return t
}

// compress encodes raw with the configured codec, degrading to identity
// on any failure or for sub-compressMinBytes payloads.
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
// retry. A batch that can't be delivered is dropped. Never panics.
func (t *transport) send(ctx context.Context, protocol protocol, batch []bufferedMessage) {
	if len(batch) == 0 {
		return
	}
	raw, err := serialize(batch)
	if err != nil {
		return // unserializable batch is dropped
	}
	body, encoding := t.compress(raw)

	lastStatus := 0
	for attempt := 0; attempt < backoffMaxAttempts; attempt++ {
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
			continue // network error / timeout: retry
		}
		lastStatus = status
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

// logDrop records a dropped batch when the debug logger is configured.
func (t *transport) logDrop(protocol protocol, n int, reason string) {
	if t.logger == nil {
		return
	}
	t.logger.Warn("evpanda: dropped batch (delivery failed)",
		"protocol", string(protocol),
		"messages", n,
		"reason", reason,
	)
}
