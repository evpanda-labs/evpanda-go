package evpanda

// Redaction, applied at the capture chokepoint — the last point at which
// a secret can be removed, since the next step is memory that outlives
// the call.
//
// OCPI has two rules:
//
//  1. Header allowlist — only listed headers are kept; Authorization,
//     Cookie, X-API-Key and anything else unlisted fall off the end.
//     OCPIConfig.OCPIAllowedHeaders extends the list, never shrinks it.
//  2. Credentials-endpoint token mask — on a /credentials URL the token
//     field (at the root for requests, under data for the response
//     envelope) is replaced with [redacted].

import (
	"encoding/json"
	"regexp"
	"strings"
)

// defaultOCPIHeaderAllowlist is the set of stock OCPI headers safe to
// capture — none of these can carry a secret.
var defaultOCPIHeaderAllowlist = []string{
	// OCPI routing
	"ocpi-from-country-code",
	"ocpi-from-party-id",
	"ocpi-to-country-code",
	"ocpi-to-party-id",
	// Content negotiation and standard HTTP
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
// or /credentials?…. Sub-paths like /credentials/foo don't match — there
// is no such OCPI route.
var credentialsURL = regexp.MustCompile(`(?i)/credentials/?(\?|$)`)

// defaultOCPIRedactor builds the redactor closure from the resolved config.
// It is called once per client, so the allowlist set is amortized across
// every message.
func defaultOCPIRedactor(extraAllowedHeaders []string) ocpiRedactor {
	allow := make(map[string]struct{}, len(defaultOCPIHeaderAllowlist)+len(extraAllowedHeaders))
	for _, h := range defaultOCPIHeaderAllowlist {
		allow[h] = struct{}{}
	}
	for _, h := range extraAllowedHeaders {
		allow[strings.ToLower(h)] = struct{}{}
	}
	return func(msg ocpiMessage) ocpiMessage {
		msg.Data.RequestHeaders = filterHeaders(msg.Data.RequestHeaders, allow)
		msg.Data.ResponseHeaders = filterHeaders(msg.Data.ResponseHeaders, allow)
		msg.Data.SetRequestBody(maskCredentialsToken(msg.Data.RequestBody, msg.Data.URL))
		msg.Data.SetResponseBody(maskCredentialsToken(msg.Data.ResponseBody, msg.Data.URL))
		return msg
	}
}

// filterHeaders keeps only allowlisted headers, matching the key
// case-insensitively. It builds a new map rather than mutating the
// caller's, and returns nil only when the input is nil.
func filterHeaders(h map[string]string, allow map[string]struct{}) map[string]string {
	if h == nil {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		if _, ok := allow[strings.ToLower(k)]; ok {
			out[k] = v
		}
	}
	return out
}

// maskCredentialsToken masks the token field in an OCPI credentials body.
// It returns the original bytes on any miss (non-credentials URL,
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

	// The token lives at the root (request) or under data (response
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

// maskTokenIn replaces a non-empty string token in obj with the
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

// ── OCPP ─────────────────────────────────────────────────────────────────
//
// There is no OCPP redactor. Frames are captured verbatim, so
// OCPPClient.redact stays nil and the chokepoint skips the step
// entirely — rather than paying an indirect call per frame to run an
// identity transform.
//
// The seam is the nil-able field itself: masking (idTag in Authorize,
// for example) arrives as a defaultOCPPRedactor here plus one line in
// StartOCPP, with nothing in the worker or the client to change.
