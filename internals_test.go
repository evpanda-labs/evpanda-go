// Tests for the parts the end-to-end suite structurally cannot reach:
// the chokepoint's drop taxonomy, config resolution, the panic guard, and
// the health reporter's one-minute ticker. Everything observable from the
// public API is tested in e2e_test.go instead, against a real upstream.

package evpanda

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// ══ Chokepoint ══════════════════════════════════════════════════════════
//
// The single place a message is validated, capped and redacted. Each drop
// must land in the counter that names its cause, since that mapping is
// what makes Stats() diagnostic.

func validPlatform() Platform {
	return Platform{ID: "acme", Name: "Acme"}
}

func passthroughOCPI(m ocpiMessage) ocpiMessage { return m }
func passthroughOCPP(m ocppMessage) ocppMessage { return m }

func TestPrepareOCPIDrops(t *testing.T) {
	tests := []struct {
		name string
		msg  ocpiMessage
		want dropReason
	}{
		{"valid", ocpiMessage{Direction: ocpiInbound, Identity: validPlatform()}, dropNone},
		{"no identity", ocpiMessage{Direction: ocpiInbound}, dropInvalidIdentity},
		{"missing platform name", ocpiMessage{
			Identity: Platform{ID: "acme"},
		}, dropInvalidIdentity},
		{"half a tenant pair", ocpiMessage{
			Identity: Platform{ID: "acme", Name: "Acme", TenantID: "t1"},
		}, dropInvalidIdentity},
		{"whole tenant pair", ocpiMessage{
			Identity: Platform{
				ID: "acme", Name: "Acme",
				TenantID: "t1", TenantName: "Tenant One",
			},
		}, dropNone},
		{"oversize request body", ocpiMessage{
			Identity: validPlatform(),
			Data:     HTTPExchange{RequestBody: make([]byte, 11)},
		}, dropOversize},
		{"oversize response body", ocpiMessage{
			Identity: validPlatform(),
			Data:     HTTPExchange{ResponseBody: make([]byte, 11)},
		}, dropOversize},
		{"body exactly at the cap", ocpiMessage{
			Identity: validPlatform(),
			Data:     HTTPExchange{RequestBody: make([]byte, 10)},
		}, dropNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, reason := prepareOCPI(tc.msg, passthroughOCPI, 10)
			if reason != tc.want {
				t.Fatalf("prepareOCPI reason = %v, want %v", reason, tc.want)
			}
			if reason == dropNone && env.capturedAt == "" {
				t.Fatal("a kept message must be stamped with its capture time")
			}
		})
	}
}

func TestPrepareOCPPDrops(t *testing.T) {
	valid := Charger{ID: "CP-001"}
	tests := []struct {
		name string
		msg  ocppMessage
		want dropReason
	}{
		{"connect", ocppMessage{EventType: ocppEventTypeConnect, Identity: valid}, dropNone},
		{"disconnect", ocppMessage{EventType: ocppEventTypeDisconnect, Identity: valid}, dropNone},
		{"message", ocppMessage{
			EventType: ocppEventTypeMessage, Identity: valid,
			Direction: FromCP, Payload: []byte("x"),
		}, dropNone},
		{"no identity", ocppMessage{EventType: ocppEventTypeConnect}, dropInvalidIdentity},
		{"half a tenant pair", ocppMessage{
			EventType: ocppEventTypeConnect,
			Identity:  Charger{ID: "CP-001", TenantID: "t1"},
		}, dropInvalidIdentity},
		// event_type 2 requires both direction and raw_frame on the wire.
		{"message without a direction", ocppMessage{
			EventType: ocppEventTypeMessage, Identity: valid, Payload: []byte("x"),
		}, dropOversize},
		{"message without a frame", ocppMessage{
			EventType: ocppEventTypeMessage, Identity: valid, Direction: FromCP,
		}, dropOversize},
		{"oversize frame", ocppMessage{
			EventType: ocppEventTypeMessage, Identity: valid,
			Direction: FromCP, Payload: make([]byte, 11),
		}, dropOversize},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, reason := prepareOCPP(tc.msg, passthroughOCPP, 10)
			if reason != tc.want {
				t.Fatalf("prepareOCPP reason = %v, want %v", reason, tc.want)
			}
			if reason == dropNone && env.capturedAt == "" {
				t.Fatal("a kept message must be stamped with its capture time")
			}
		})
	}
}

// Redaction runs before the message reaches the queue, so nothing
// unredacted is ever buffered.
func TestPrepareRunsTheRedactor(t *testing.T) {
	redact := func(m ocpiMessage) ocpiMessage {
		m.Data.URL = "redacted"
		return m
	}
	env, reason := prepareOCPI(ocpiMessage{
		Identity: validPlatform(),
		Data:     HTTPExchange{URL: "/ocpi/2.2/cdrs"},
	}, redact, 10)
	if reason != dropNone {
		t.Fatal("message should have been kept")
	}
	if got := env.message.(ocpiMessage).Data.URL; got != "redacted" {
		t.Fatalf("buffered URL = %q, want the redacted value", got)
	}
}

// A nil redactor means "nothing to redact" — the normal case for OCPP.
func TestPrepareSkipsNilRedactor(t *testing.T) {
	frame := []byte(`[2,"id","Heartbeat",{}]`)
	env, reason := prepareOCPP(ocppMessage{
		EventType: ocppEventTypeMessage,
		Identity:  Charger{ID: "CP-001"},
		Direction: FromCP,
		Payload:   frame,
	}, nil, 1024)
	if reason != dropNone {
		t.Fatalf("a nil redactor must not drop the message, got %v", reason)
	}
	if got := string(env.message.(ocppMessage).Payload); got != string(frame) {
		t.Fatalf("frame = %q, want it captured verbatim", got)
	}
}

// The flip side of the nil contract: because nil means "no redaction", a
// live OCPI client must always carry a redactor, or its allowlist and
// credentials mask would silently stop applying.
func TestRedactorPresenceByProtocol(t *testing.T) {
	cfg := BaseConfig{Endpoint: "https://ingest.example", APIKey: "k", LogMode: LogModeSilent}

	ocpiClient, err := StartOCPI(OCPIConfig{BaseConfig: cfg})
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer func() { _ = ocpiClient.Close() }()
	if ocpiClient.redact == nil {
		t.Fatal("a live OCPI client must have a redactor — nil would skip the security floor")
	}

	ocppClient, err := StartOCPP(OCPPConfig{BaseConfig: cfg})
	if err != nil {
		t.Fatalf("StartOCPP: %v", err)
	}
	defer func() { _ = ocppClient.Close() }()
	if ocppClient.redact != nil {
		t.Fatal("OCPP has nothing to redact; the field should stay nil")
	}
}

// The ownership transfer that lets a host reuse its read buffer the
// instant a capture call returns. Observable end-to-end too, but pinned
// here at the chokepoint where it actually happens.
func TestChokepointTakesOwnership(t *testing.T) {
	src := []byte(`{"id":"cdr-1"}`)
	env, reason := prepareOCPI(ocpiMessage{
		Identity: validPlatform(),
		Data: HTTPExchange{
			Method: "POST", URL: "/ocpi/2.2/cdrs",
			RequestBody: src, ResponseBody: src, // assigned, not Set
		},
	}, passthroughOCPI, 1024)
	if reason != dropNone {
		t.Fatalf("message should have been kept, got %v", reason)
	}
	copy(src, []byte(`{"id":"XXXXX"}`))

	buffered := env.message.(ocpiMessage)
	if got := string(buffered.Data.RequestBody); got != `{"id":"cdr-1"}` {
		t.Fatalf("buffered request body followed the caller's buffer: %s", got)
	}
	if got := string(buffered.Data.ResponseBody); got != `{"id":"cdr-1"}` {
		t.Fatalf("buffered response body followed the caller's buffer: %s", got)
	}

	frame := []byte(`[2,"id","Heartbeat",{}]`)
	env, _ = prepareOCPP(ocppMessage{
		EventType: ocppEventTypeMessage,
		Identity:  Charger{ID: "CP-001"},
		Direction: FromCP, Payload: frame,
	}, nil, 1024)
	copy(frame, bytes.Repeat([]byte("X"), len(frame)))
	if got := string(env.message.(ocppMessage).Payload); got != `[2,"id","Heartbeat",{}]` {
		t.Fatalf("buffered frame followed the caller's buffer: %s", got)
	}
}

// Ownership is taken *before* redaction, so a redactor may rewrite a body
// in place without reaching into the host's memory.
func TestRedactorMayMutateInPlace(t *testing.T) {
	src := []byte(`{"secret":"aaaa"}`)
	inPlace := func(m ocpiMessage) ocpiMessage {
		for i := range m.Data.RequestBody {
			m.Data.RequestBody[i] = 'z'
		}
		return m
	}
	env, reason := prepareOCPI(ocpiMessage{
		Identity: validPlatform(),
		Data:     HTTPExchange{Method: "POST", URL: "/x", RequestBody: src},
	}, inPlace, 1024)
	if reason != dropNone {
		t.Fatalf("message should have been kept, got %v", reason)
	}
	if string(src) != `{"secret":"aaaa"}` {
		t.Fatalf("an in-place redactor reached the caller's buffer: %s", src)
	}
	if got := env.message.(ocpiMessage).Data.RequestBody; string(got) != "zzzzzzzzzzzzzzzzz" {
		t.Fatalf("the redactor's rewrite was lost: %s", got)
	}
}

// The size trigger fires exactly once per full batch and never blocks the
// producer, even with nothing draining the channel.
func TestEnqueueSignalsOnFullBatch(t *testing.T) {
	budget := 4 * batchCap * probeSize
	buf, err := newRingBuffer(budget, nil)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	w := newWorker(buf, newTransport(resolvedConfig{endpoint: "http://127.0.0.1:1"}, nil), resolvedConfig{
		maxBufferBytes: budget,
		flushInterval:  time.Hour,
	}, nil)

	for range batchCap - 1 {
		w.enqueue(env(0))
	}
	select {
	case <-w.wake:
		t.Fatal("a partial batch must not raise the size trigger")
	default:
	}

	w.enqueue(env(0)) // now exactly batchCap
	select {
	case <-w.wake:
	default:
		t.Fatal("a full batch must raise the size trigger")
	}

	for range batchCap { // a burst collapses into the single pending slot
		w.enqueue(env(0))
	}
	if got := len(w.wake); got != 1 {
		t.Fatalf("pending triggers = %d, want 1 — bursts must collapse", got)
	}
}

// ══ Panic guard ═════════════════════════════════════════════════════════
//
// Invariant 2: nothing panics into the host. There is no known panic path
// from the public API, so this drives the guard directly.

func TestGuardCaptureSwallowsAndCounts(t *testing.T) {
	c, err := StartOCPI(OCPIConfig{BaseConfig: BaseConfig{
		Endpoint: "https://ingest.example", APIKey: "k", LogMode: LogModeSilent,
	}})
	if err != nil {
		t.Fatalf("StartOCPI: %v", err)
	}
	defer func() { _ = c.Close() }()

	c.guardCapture("boom", func() { panic("capture exploded") })

	if got := c.Stats().DroppedPanic; got != 1 {
		t.Fatalf("DroppedPanic = %d, want 1 — a recovered panic must be counted", got)
	}
	// Still usable afterwards.
	c.guardCapture("fine", func() {})
	if got := c.Stats().DroppedPanic; got != 1 {
		t.Fatalf("DroppedPanic = %d, want it unchanged by a clean call", got)
	}
}

// An inert client has no counters and no worker; the guard must still
// swallow rather than nil-deref.
func TestGuardCaptureOnInertClient(t *testing.T) {
	inert, err := StartOCPI(OCPIConfig{BaseConfig: BaseConfig{Endpoint: "not-a-url"}})
	if err == nil {
		t.Fatal("expected a config error")
	}
	inert.guardCapture("boom", func() { panic("exploded") })
	if got := inert.Stats(); got != (Stats{}) {
		t.Fatalf("inert Stats = %+v, want the zero value", got)
	}
}

func TestGuardConvertsPanicToError(t *testing.T) {
	err := guard("Op", func() error { panic("exploded") })
	if err == nil || !strings.Contains(err.Error(), "recovered panic in Op") {
		t.Fatalf("guard error = %v, want a wrapped panic", err)
	}
	if err := guard("Op", func() error { return nil }); err != nil {
		t.Fatalf("guard returned %v for a clean call", err)
	}
}

// ══ Retry backoff ═══════════════════════════════════════════════════════

// Capped exponential with full jitter: every delay is below the cap for
// its attempt, and never negative.
func TestNextDelayBounds(t *testing.T) {
	for attempt := range backoffMaxAttempts {
		want := min(backoffBase<<attempt, backoffMax)
		for range 200 {
			d := nextDelay(attempt)
			if d < 0 || d >= want {
				t.Fatalf("nextDelay(%d) = %v, want [0, %v)", attempt, d, want)
			}
		}
	}
	// The cap holds however many attempts are asked for.
	if d := nextDelay(60); d < 0 || d >= backoffMax {
		t.Fatalf("nextDelay(60) = %v, want [0, %v)", d, backoffMax)
	}
}

// ══ Health reporter ═════════════════════════════════════════════════════
//
// Driven directly because the real trigger is a one-minute ticker.

func reportWorker(t *testing.T, mode LogMode) (*worker, *stats, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	st := &stats{}
	buf, err := newRingBuffer(64<<10, st)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	cfg := resolvedConfig{
		endpoint:      "http://127.0.0.1:1",
		flushInterval: time.Hour,
		logMode:       mode,
		logger:        slog.New(slog.NewTextHandler(logs, nil)),
	}
	if mode == LogModeSilent {
		cfg.logger = nil
	}
	return newWorker(buf, newTransport(cfg, st), cfg, st), st, logs
}

// A healthy client says nothing, however much traffic it handled.
func TestReportHealthSilentWhenNothingDropped(t *testing.T) {
	w, st, logs := reportWorker(t, LogModeErrors)
	for range 5000 {
		st.countCaptured()
	}
	w.reportHealth()
	if logs.Len() != 0 {
		t.Fatalf("a healthy client must not log: %q", logs.String())
	}
}

// One line per window, naming what was lost and why, omitting what did not
// move, and reporting the delta rather than the running total.
func TestReportHealthLogsDeltaThenGoesQuiet(t *testing.T) {
	w, st, logs := reportWorker(t, LogModeErrors)
	for range 3 {
		st.countDrop(dropInvalidIdentity, 1)
	}
	st.countDrop(dropEvicted, 1)

	w.reportHealth()
	line := logs.String()
	for _, want := range []string{"captures dropped", "invalid_identity=3", "evicted=1", "window=1m0s"} {
		if !strings.Contains(line, want) {
			t.Fatalf("health line missing %q: %q", want, line)
		}
	}
	if strings.Contains(line, "oversize") || strings.Contains(line, "undeliverable") {
		t.Fatalf("health line includes zero counters: %q", line)
	}

	logs.Reset()
	st.countDrop(dropOversize, 2)
	w.reportHealth()
	if !strings.Contains(logs.String(), "oversize=2") {
		t.Fatalf("second window must report its own delta: %q", logs.String())
	}
	if strings.Contains(logs.String(), "invalid_identity") {
		t.Fatalf("a counter that stopped moving must drop out: %q", logs.String())
	}

	logs.Reset()
	w.reportHealth()
	if logs.Len() != 0 {
		t.Fatalf("a recovered client must go quiet again: %q", logs.String())
	}
}

// The property that makes default-on logging safe: a fault repeating on
// every request still costs exactly one line per window.
func TestReportHealthIsRateBounded(t *testing.T) {
	w, st, logs := reportWorker(t, LogModeErrors)
	for range 250_000 { // e.g. every request failing identity resolution
		st.countDrop(dropInvalidIdentity, 1)
	}
	w.reportHealth()

	if n := strings.Count(logs.String(), "\n"); n != 1 {
		t.Fatalf("250k drops produced %d lines, want exactly 1", n)
	}
	if !strings.Contains(logs.String(), "invalid_identity=250000") {
		t.Fatalf("the single line must carry the full count: %q", logs.String())
	}
}

func TestReportHealthSilentMode(t *testing.T) {
	w, st, logs := reportWorker(t, LogModeSilent)
	st.countDrop(dropInvalidIdentity, 1)
	w.reportHealth()
	if logs.Len() != 0 {
		t.Fatalf("LogModeSilent must not log: %q", logs.String())
	}
}

// Debug logs the closing summary even for a clean run; the default mode
// only speaks up when something was lost.
func TestReportShutdownByMode(t *testing.T) {
	w, _, logs := reportWorker(t, LogModeDebug)
	w.reportShutdown(t.Context(), nil)
	if !strings.Contains(logs.String(), "client closed") {
		t.Fatalf("debug mode must log a clean close: %q", logs.String())
	}

	w, _, logs = reportWorker(t, LogModeErrors)
	w.reportShutdown(t.Context(), nil)
	if logs.Len() != 0 {
		t.Fatalf("default mode must close quietly when clean: %q", logs.String())
	}

	w, _, logs = reportWorker(t, LogModeErrors)
	w.reportShutdown(t.Context(), ErrDrainIncomplete)
	if !strings.Contains(logs.String(), "drain=") {
		t.Fatalf("an incomplete drain must be reported: %q", logs.String())
	}
}

// Every reason lands in its own counter, and dropNone lands nowhere. This
// is the one mapping between the drop taxonomy and Stats.
func TestCountDropMapping(t *testing.T) {
	tests := []struct {
		reason dropReason
		field  func(Stats) uint64
	}{
		{dropInvalidIdentity, func(s Stats) uint64 { return s.DroppedInvalid }},
		{dropOversize, func(s Stats) uint64 { return s.DroppedOversize }},
		{dropEvicted, func(s Stats) uint64 { return s.DroppedEvicted }},
		{dropUndeliverable, func(s Stats) uint64 { return s.DroppedUndeliverable }},
		{dropPanic, func(s Stats) uint64 { return s.DroppedPanic }},
	}
	for _, tc := range tests {
		st := &stats{}
		st.countDrop(tc.reason, 3)
		got := st.snapshot()
		if tc.field(got) != 3 || got.TotalDropped() != 3 {
			t.Fatalf("reason %d did not land in exactly its own counter: %+v", tc.reason, got)
		}
	}

	st := &stats{}
	st.countDrop(dropNone, 5)     // the chokepoint's "kept" result
	st.countDrop(dropEvicted, 0)  // a zero count is a no-op
	st.countDrop(dropEvicted, -1) // so is a negative one
	if got := st.snapshot(); got.TotalDropped() != 0 {
		t.Fatalf("dropNone / non-positive counts must not register: %+v", got)
	}

	// A nil counter set is inert — that is what lets an inert client
	// answer Stats() and lets unit tests skip the bookkeeping.
	var nilStats *stats
	nilStats.countCaptured()
	nilStats.countDrop(dropEvicted, 1)
	if got := nilStats.snapshot(); got != (Stats{}) {
		t.Fatalf("nil stats snapshot = %+v, want the zero value", got)
	}
}

// ══ Config resolution ═══════════════════════════════════════════════════
//
// Only Endpoint and APIKey can fail; every other field falls back to its
// default and warns, so a typo can never silence capture.

func TestResolveEndpoint(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"trims trailing slashes", "https://ingest.example///", "https://ingest.example", false},
		{"trims surrounding space", "  https://ingest.example  ", "https://ingest.example", false},
		{"keeps a base path", "https://host/api/", "https://host/api", false},
		{"plain http is allowed", "http://localhost:8080", "http://localhost:8080", false},
		{"empty defaults to production", "", defaultEndpoint, false},
		{"blank defaults to production", "   ", defaultEndpoint, false},
		{"no scheme", "ingest.example", "", true},
		{"no host", "https://", "", true},
		{"wrong scheme", "ftp://ingest.example", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveEndpoint(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolveEndpoint(%q) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Fatalf("resolveEndpoint(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestResolveAPIKeyPrefersConfigOverEnv(t *testing.T) {
	t.Setenv(apiKeyEnvVar, "env-key")
	got, err := resolveAPIKey("  cfg-key  ")
	if err != nil || got != "cfg-key" {
		t.Fatalf("resolveAPIKey = %q, %v; want cfg-key, nil", got, err)
	}
	if got, err = resolveAPIKey(""); err != nil || got != "env-key" {
		t.Fatalf("resolveAPIKey = %q, %v; want the env value", got, err)
	}
	t.Setenv(apiKeyEnvVar, "")
	if _, err = resolveAPIKey(""); err == nil {
		t.Fatal("no key anywhere must be an error")
	}
}

// A tunable is replaced only when zero or below its minimum, and the
// replacement is announced.
func TestResolveBound(t *testing.T) {
	var warned int
	warn := func(string, ...any) { warned++ }

	if got := resolveBound(0, 10, "Field", 1, warn); got != 10 || warned != 0 {
		t.Fatalf("zero should take the default silently, got %d after %d warnings", got, warned)
	}
	if got := resolveBound(-5, 10, "Field", 1, warn); got != 10 || warned != 1 {
		t.Fatalf("below-minimum should take the default and warn, got %d after %d warnings", got, warned)
	}
	if got := resolveBound(7, 10, "Field", 1, warn); got != 7 || warned != 1 {
		t.Fatalf("in-range value should survive silently, got %d", got)
	}
	if got := resolveBound(3*time.Second, 10*time.Second, "Field", time.Second, warn); got != 3*time.Second {
		t.Fatalf("duration in range should survive, got %v", got)
	}
}

func TestResolveAllowedHeadersNormalizes(t *testing.T) {
	got := resolveAllowedHeaders([]string{"  X-Trace ", "x-trace", "", "   ", "X-Other"})
	want := []string{"x-trace", "x-other"}
	if len(got) != len(want) {
		t.Fatalf("resolveAllowedHeaders = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resolveAllowedHeaders = %v, want %v (insertion order preserved)", got, want)
		}
	}
}

func TestResolveBaseConfigDefaults(t *testing.T) {
	t.Setenv(logModeEnvVar, "") // don't inherit the developer's environment
	r, err := resolveBaseConfig(BaseConfig{
		Endpoint: "https://ingest.example",
		APIKey:   "k",
	}, protocolOCPI)
	if err != nil {
		t.Fatalf("resolveBaseConfig: %v", err)
	}
	if r.maxBufferBytes != defaultMaxBufferBytes ||
		r.maxCaptureBytes != defaultMaxCaptureBytes ||
		r.flushInterval != defaultFlushInterval ||
		r.drainTimeout != defaultDrainTimeout {
		t.Fatalf("defaults not applied: %+v", r)
	}
	// Reporting is on by default, so an unconfigured client still has a
	// logger — what keeps it quiet is having nothing to report.
	if r.logMode != LogModeErrors || r.logger == nil {
		t.Fatalf("default logMode = %q, logger nil = %v; want errors with a logger",
			r.logMode, r.logger == nil)
	}
	if r.protocol != protocolOCPI {
		t.Fatalf("protocol = %q, want %q", r.protocol, protocolOCPI)
	}
}

// A budget below the per-message cap is legal in isolation but nonsense
// together, so it warns.
func TestResolveWarnsWhenBufferSmallerThanCapture(t *testing.T) {
	t.Setenv(logModeEnvVar, "")
	var logs bytes.Buffer
	_, err := resolveBaseConfig(BaseConfig{
		Endpoint:        "https://ingest.example",
		APIKey:          "k",
		MaxBufferBytes:  64 << 10,
		MaxCaptureBytes: 1 << 20,
		Logger:          slog.New(slog.NewTextHandler(&logs, nil)),
	}, protocolOCPI)
	if err != nil {
		t.Fatalf("resolveBaseConfig: %v", err)
	}
	if !strings.Contains(logs.String(), "MaxBufferBytes") {
		t.Fatalf("mismatched caps must warn: %q", logs.String())
	}
}

func TestEffectiveLogger(t *testing.T) {
	for _, mode := range []LogMode{LogModeErrors, LogModeDebug} {
		if got := effectiveLogger(BaseConfig{}, mode); got == nil {
			t.Fatalf("%s with no Logger must resolve to slog.Default()", mode)
		}
	}
	if got := effectiveLogger(BaseConfig{}, LogModeSilent); got != nil {
		t.Fatal("LogModeSilent must resolve to a nil (silent) logger")
	}
	custom := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	if got := effectiveLogger(BaseConfig{Logger: custom}, LogModeErrors); got != custom {
		t.Fatal("a supplied Logger must be used as-is")
	}
}

// Config wins over the environment; the environment fills in when the
// field is unset; anything unrecognised falls back to errors rather than
// silencing the SDK.
func TestResolveLogMode(t *testing.T) {
	tests := []struct {
		name     string
		field    LogMode
		env      string
		want     LogMode
		wantWarn bool
	}{
		{"unset, no env", "", "", LogModeErrors, false},
		{"unset, env silent", "", "silent", LogModeSilent, false},
		{"unset, env debug", "", "debug", LogModeDebug, false},
		{"unset, env is padded and cased", "", "  DEBUG ", LogModeDebug, false},
		{"unset, env junk", "", "loud", LogModeErrors, true},
		{"field wins over env", LogModeDebug, "silent", LogModeDebug, false},
		{"field silent wins over env", LogModeSilent, "debug", LogModeSilent, false},
		{"field junk", "loud", "silent", LogModeErrors, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(logModeEnvVar, tc.env)
			got, warning := resolveLogMode(tc.field)
			if got != tc.want {
				t.Fatalf("resolveLogMode(%q) with %s=%q = %q, want %q",
					tc.field, logModeEnvVar, tc.env, got, tc.want)
			}
			if (warning != "") != tc.wantWarn {
				t.Fatalf("warning = %q, wantWarn %v", warning, tc.wantWarn)
			}
		})
	}
}

// Config failures are matchable, so a caller can tell a deployment
// problem (no API key reached the process) from a code problem (a
// malformed endpoint) instead of only logging a string.
func TestConfigErrorsAreMatchable(t *testing.T) {
	t.Setenv(apiKeyEnvVar, "")

	tests := []struct {
		name string
		cfg  BaseConfig
		want error
	}{
		{"malformed endpoint", BaseConfig{Endpoint: "not-a-url", APIKey: "k"}, ErrEndpoint},
		{"wrong scheme", BaseConfig{Endpoint: "ftp://h", APIKey: "k"}, ErrEndpoint},
		{"no api key anywhere", BaseConfig{Endpoint: "https://h"}, ErrAPIKey},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := StartOCPI(OCPIConfig{BaseConfig: tc.cfg})
			if err == nil {
				t.Fatal("expected a config error")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("errors.Is(_, %v) = false for %v", tc.want, err)
			}
			// Every field error also matches the umbrella sentinel.
			if !errors.Is(err, ErrConfig) {
				t.Fatalf("every config failure must wrap ErrConfig: %v", err)
			}
			// The message keeps the detail, not just the sentinel text.
			if len(err.Error()) <= len(ErrConfig.Error()) {
				t.Fatalf("error lost its detail: %q", err)
			}
		})
	}

	// A good config produces no error at all.
	c, err := StartOCPI(OCPIConfig{BaseConfig: BaseConfig{
		Endpoint: "https://ingest.example", APIKey: "k", LogMode: LogModeSilent,
	}})
	if err != nil {
		t.Fatalf("valid config returned %v", err)
	}
	_ = c.Close()
}

// An unset Endpoint means production, not a misconfiguration — a host
// that only sets an API key must reach the real ingestion API.
func TestEndpointDefaultsToProduction(t *testing.T) {
	t.Setenv(logModeEnvVar, "")
	r, err := resolveBaseConfig(BaseConfig{APIKey: "k"}, protocolOCPI)
	if err != nil {
		t.Fatalf("an unset Endpoint must not fail: %v", err)
	}
	if r.endpoint != defaultEndpoint {
		t.Fatalf("endpoint = %q, want %q", r.endpoint, defaultEndpoint)
	}
	if !strings.HasPrefix(defaultEndpoint, "https://") {
		t.Fatalf("the default must be https, got %q", defaultEndpoint)
	}

	// An explicit value still wins, and a malformed one still fails.
	r, err = resolveBaseConfig(BaseConfig{Endpoint: "https://staging.example/", APIKey: "k"}, protocolOCPI)
	if err != nil || r.endpoint != "https://staging.example" {
		t.Fatalf("explicit endpoint = %q, %v", r.endpoint, err)
	}
	if _, err = resolveBaseConfig(BaseConfig{Endpoint: "nope", APIKey: "k"}, protocolOCPI); !errors.Is(err, ErrEndpoint) {
		t.Fatalf("a malformed endpoint must still fail, got %v", err)
	}
}
