package evpanda

import (
	"encoding/json"
	"regexp"
	"strings"
)

// defaultOCPIHeaderAllowlist is the set of stock OCPI headers safe to
// capture — none of these can carry a secret. Everything unlisted
// (Authorization, Cookie, X-API-Key, ...) is dropped.
// OCPIConfig.OCPIAllowedHeaders extends this list, never shrinks it.
var defaultOCPIHeaderAllowlist = []string{
	// OCPI routing
	"ocpi-from-country-code",
	"ocpi-from-party-id",
	"ocpi-to-country-code",
	"ocpi-to-party-id",
	// Content negotiation + standard HTTP
	"content-type",
	"accept",
	"user-agent",
	// Tracing
	"x-correlation-id",
	"x-request-id",
	// Pagination
	"x-total-count",
	"x-limit",
	"link",
}

// tokenPlaceholder is written in place of redacted token values.
const tokenPlaceholder = "[redacted]"

// credentialsURL matches a URL ending with /credentials, /credentials/,
// or /credentials?…. Sub-paths like /credentials/foo don't match — no
// such OCPI route.
var credentialsURL = regexp.MustCompile(`(?i)/credentials/?(\?|$)`)

// ocpiRedactor applies the header allowlist and the credentials-token
// mask. Built once per client; the allowlist set is amortized across
// every message.
type ocpiRedactor struct {
	allow map[string]struct{}
}

func newOCPIRedactor(extraAllowedHeaders []string) *ocpiRedactor {
	allow := make(map[string]struct{}, len(defaultOCPIHeaderAllowlist)+len(extraAllowedHeaders))
	for _, h := range defaultOCPIHeaderAllowlist {
		allow[h] = struct{}{}
	}
	for _, h := range extraAllowedHeaders {
		allow[strings.ToLower(h)] = struct{}{}
	}
	return &ocpiRedactor{allow: allow}
}

// redact returns msg with non-allowlisted headers removed and the
// credentials token masked. It does not mutate the input maps.
func (r *ocpiRedactor) redact(msg ocpiMessage) ocpiMessage {
	msg.HTTP.RequestHeaders = r.filterHeaders(msg.HTTP.RequestHeaders)
	msg.HTTP.ResponseHeaders = r.filterHeaders(msg.HTTP.ResponseHeaders)
	msg.HTTP.RequestBody = maskCredentialsToken(msg.HTTP.RequestBody, msg.HTTP.URL)
	msg.HTTP.ResponseBody = maskCredentialsToken(msg.HTTP.ResponseBody, msg.HTTP.URL)
	return msg
}

// filterHeaders keeps only allowlisted headers, case-insensitively.
// The result is non-nil whenever the input is.
func (r *ocpiRedactor) filterHeaders(h map[string]string) map[string]string {
	if h == nil {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		if _, ok := r.allow[strings.ToLower(k)]; ok {
			out[k] = v
		}
	}
	return out
}

// maskCredentialsToken masks the `token` field in an OCPI credentials
// body. It returns the original bytes on any miss (non-credentials URL,
// non-JSON, no token at either known path, re-encode error) — redaction
// never silently drops data it couldn't safely rewrite.
func maskCredentialsToken(body []byte, url string) []byte {
	if len(body) == 0 || !credentialsURL.MatchString(url) {
		return body
	}

	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(body, &parsed); err != nil {
		return body
	}

	// Token lives at the root (request) or under `data` (response
	// envelope).
	if maskTokenIn(parsed) {
		return marshalOr(parsed, body)
	}
	var data map[string]json.RawMessage
	if raw, ok := parsed["data"]; ok && json.Unmarshal(raw, &data) == nil && maskTokenIn(data) {
		if b, err := json.Marshal(data); err == nil {
			parsed["data"] = b
			return marshalOr(parsed, body)
		}
	}
	return body
}

// maskTokenIn replaces a non-empty string `token` in obj with the
// placeholder, reporting whether it did.
func maskTokenIn(obj map[string]json.RawMessage) bool {
	raw, ok := obj["token"]
	if !ok {
		return false
	}
	var token string
	if err := json.Unmarshal(raw, &token); err != nil || token == "" {
		return false
	}
	b, err := json.Marshal(tokenPlaceholder)
	if err != nil {
		return false
	}
	obj["token"] = b
	return true
}

// marshalOr re-encodes obj, falling back to orig on error.
func marshalOr(obj map[string]json.RawMessage, orig []byte) []byte {
	b, err := json.Marshal(obj)
	if err != nil {
		return orig
	}
	return b
}

// redactOCPP is the OCPP redaction seam. Today the identity transform —
// frames are captured verbatim. The seam exists so masking (e.g. idTag
// in Authorize) can be added later without touching the capture path.
func redactOCPP(msg ocppMessage) ocppMessage {
	return msg
}
