package evpanda

// Customer-facing configuration. The protocol is the client — there is no
// network-type field. Common fields live on [BaseConfig]; per-protocol
// configs add only what that protocol's client cares about.
//
// Endpoint and APIKey are hard-required: a bad value fails Start* (which
// hands back an inert client plus the error). Every other field is
// tunable — a bad value falls back to its default and says so in the
// host's logs, so a typo can never silence the SDK entirely.

import (
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"
)

// LogMode selects how much the SDK says for itself. The zero value means
// "unset": the EVPANDA_LOG environment variable decides, and failing that
// [LogModeErrors].
//
// The default is deliberately not silence. An SDK that captures nothing
// because its identity resolution is misconfigured looks exactly like an
// SDK on an idle system, and a customer should not have to redeploy with
// a debug flag to tell those apart. What it will not do is log per event:
// problems are summarized once a minute, so a fault that occurs on every
// request still costs one line, and a healthy client says nothing at all.
type LogMode string

const (
	// LogModeSilent disables SDK logging completely. The counters from
	// [OCPIClient.Stats] keep working.
	LogModeSilent LogMode = "silent"
	// LogModeErrors is the default: config problems at startup, plus a
	// once-a-minute summary whenever captures are being dropped.
	LogModeErrors LogMode = "errors"
	// LogModeDebug adds per-batch delivery failures, recovered capture
	// faults, and a summary line on close even when nothing went wrong.
	LogModeDebug LogMode = "debug"
)

// logModeEnvVar lets an operator change the setting without a code
// change — including turning the SDK silent during an incident, which is
// the case that most needs a restart-only escape hatch.
const logModeEnvVar = "EVPANDA_LOG"

// BaseConfig holds the fields shared by [OCPIConfig] and [OCPPConfig].
// Every field except Endpoint and APIKey falls back to a default when
// left at its zero value.
type BaseConfig struct {
	// Endpoint is the ingestion API base, e.g. https://ingest.evpanda.io.
	// Required.
	Endpoint string
	// APIKey is sent as the X-API-Key header. If empty, it falls back to
	// the EVPANDA_API_KEY environment variable; one of the two must be set.
	APIKey string

	// MaxBufferBytes is the ceiling on everything held in memory awaiting
	// delivery. Past it the oldest captures are evicted, so this is the
	// SDK's memory footprint, not an estimate of it. Zero uses the
	// default (32 MiB); the buffer grows on demand and idles far below it.
	MaxBufferBytes int
	// MaxCaptureBytes is the per-body / per-frame capture cap, enforced
	// at capture: an oversize body or frame drops the whole message.
	// Zero uses the default (65536).
	MaxCaptureBytes int
	// FlushInterval is the maximum time between flushes. Zero uses the
	// default (5s).
	FlushInterval time.Duration
	// DrainTimeout is how long Close waits to drain buffered messages.
	// Zero uses the default (10s); an explicit value must be ≥ 5s.
	DrainTimeout time.Duration
	// LogMode selects how much the SDK logs. Empty consults the
	// EVPANDA_LOG env var, then falls back to LogModeErrors — problems
	// are reported by default, at a bounded rate.
	LogMode LogMode
	// Logger receives the SDK's own logs. Nil uses slog.Default().
	Logger *slog.Logger
}

// OCPIConfig configures [StartOCPI].
type OCPIConfig struct {
	BaseConfig

	// OCPIAllowedHeaders extends the default capture allowlist with
	// additional header names (matched case-insensitively). It can only
	// extend the list, never shrink it.
	OCPIAllowedHeaders []string
}

// OCPPConfig configures [StartOCPP]. It has no protocol-specific fields
// today.
type OCPPConfig struct {
	BaseConfig
}

// resolvedConfig is a config with defaults applied and validation passed.
// It is what the worker and transport read.
type resolvedConfig struct {
	endpoint        string
	apiKey          string
	protocol        protocol
	maxBufferBytes  int
	maxCaptureBytes int
	flushInterval   time.Duration
	drainTimeout    time.Duration
	// allowedHeaders is the lowercased extra allowlist (OCPI only).
	allowedHeaders []string
	// logMode is the resolved verbosity; never the empty zero value.
	logMode LogMode
	// logger is the effective logger: nil exactly when logMode is
	// LogModeSilent, so a nil check is the only silence test callers need.
	logger *slog.Logger
}

const (
	// defaultMaxBufferBytes covers roughly one full retry window of a
	// 10 000-charger CSMS (~400 msg/s at ~500 B) — enough to ride out a
	// blip, small enough to sit inside an ordinary container limit.
	defaultMaxBufferBytes  = 32 << 20 // 32 MiB
	defaultMaxCaptureBytes = 64 * 1024
	defaultFlushInterval   = 5 * time.Second
	defaultDrainTimeout    = 10 * time.Second

	// minMaxBufferBytes is one default-sized capture; below it the buffer
	// could not hold even a single message.
	minMaxBufferBytes = 64 << 10 // 64 KiB
	minFlushInterval  = time.Millisecond
	minDrainTimeout   = 5 * time.Second
)

// Configuration failures returned by [StartOCPI] and [StartOCPP]. Every
// one wraps ErrConfig, and the two field-specific sentinels wrap it in
// turn, so a caller can match at whichever level it cares about:
//
//	if errors.Is(err, evpanda.ErrAPIKey) {
//		// a deployment problem — the key never reached the process
//	}
//	if errors.Is(err, evpanda.ErrConfig) {
//		// any configuration fault
//	}
//
// Start* fails for no other reason, so these cover it.
var (
	// ErrConfig is wrapped by every configuration failure.
	ErrConfig = errors.New("evpanda: invalid config")
	// ErrEndpoint reports a missing or malformed Endpoint.
	ErrEndpoint = fmt.Errorf("%w: endpoint", ErrConfig)
	// ErrAPIKey reports that no API key was found in the config or the
	// EVPANDA_API_KEY environment variable.
	ErrAPIKey = fmt.Errorf("%w: api key", ErrConfig)
)

const errPrefix = "evpanda: config"

// apiKeyEnvVar is the fallback source for APIKey when BaseConfig.APIKey
// is empty.
const apiKeyEnvVar = "EVPANDA_API_KEY"

// warnFunc is the sink the tunable-field resolvers report to. It is a
// no-op when the resolved logger is nil (LogModeSilent), which keeps the
// silence rule in one place.
type warnFunc func(format string, args ...any)

// makeWarn builds the warn sink over an already-resolved logger. Taking
// the logger rather than the raw config means a nil logger is the single
// signal for "stay silent".
func makeWarn(logger *slog.Logger) warnFunc {
	if logger == nil {
		return func(string, ...any) {}
	}
	return func(format string, args ...any) {
		logger.Warn(errPrefix + ": " + fmt.Sprintf(format, args...))
	}
}

// resolveLogMode applies the config field, then the environment, then the
// default. An unrecognised value in either falls back to LogModeErrors —
// a typo must not silence the SDK, which is the whole point of the
// default.
//
// Config wins over the environment, matching how APIKey resolves. Since
// most hosts never set the field, EVPANDA_LOG still reaches almost every
// deployment, which is what makes it usable as an incident escape hatch.
func resolveLogMode(value LogMode) (LogMode, string) {
	switch value {
	case LogModeSilent, LogModeErrors, LogModeDebug:
		return value, ""
	case "":
		// fall through to the environment
	default:
		return LogModeErrors, fmt.Sprintf("`LogMode` must be %q, %q or %q; using %q",
			LogModeSilent, LogModeErrors, LogModeDebug, LogModeErrors)
	}

	switch env := LogMode(strings.ToLower(strings.TrimSpace(os.Getenv(logModeEnvVar)))); env {
	case LogModeSilent, LogModeErrors, LogModeDebug:
		return env, ""
	case "":
		return LogModeErrors, ""
	default:
		return LogModeErrors, fmt.Sprintf("%s must be %q, %q or %q; using %q",
			logModeEnvVar, LogModeSilent, LogModeErrors, LogModeDebug, LogModeErrors)
	}
}

// effectiveLogger returns the logger to use, or nil when the mode is
// silent. A nil logger is the single signal for "say nothing".
func effectiveLogger(c BaseConfig, mode LogMode) *slog.Logger {
	if mode == LogModeSilent {
		return nil
	}
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

// resolveAPIKey returns the configured APIKey, or the EVPANDA_API_KEY env
// var, or an error if neither is set.
func resolveAPIKey(value string) (string, error) {
	if v := strings.TrimSpace(value); v != "" {
		return v, nil
	}
	if v := strings.TrimSpace(os.Getenv(apiKeyEnvVar)); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("%w: set APIKey or the %s environment variable", ErrAPIKey, apiKeyEnvVar)
}

// resolveEndpoint requires a non-empty http(s) URL and trims trailing
// slashes; the transport appends /v1/{protocol}.
func resolveEndpoint(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", fmt.Errorf("%w: required and must be a non-empty string", ErrEndpoint)
	}
	u, err := url.Parse(s)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("%w: %q is not a valid URL", ErrEndpoint, s)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("%w: %q must use http or https", ErrEndpoint, s)
	}
	return strings.TrimRight(s, "/"), nil
}

// resolveBound returns fallback for a zero or below-minimum value (the
// latter with a warning), else value. Shared by the int and
// time.Duration options.
func resolveBound[T int | time.Duration](value, fallback T, field string, minVal T, warn warnFunc) T {
	if value == 0 {
		return fallback
	}
	if value < minVal {
		warn("`%s` must be >= %v; using default %v", field, minVal, fallback)
		return fallback
	}
	return value
}

// resolveAllowedHeaders trims, lowercases, and deduplicates (preserving
// insertion order) the extra allowlist entries, skipping empties.
func resolveAllowedHeaders(headers []string) []string {
	seen := make(map[string]struct{}, len(headers))
	out := make([]string, 0, len(headers))
	for _, h := range headers {
		v := strings.ToLower(strings.TrimSpace(h))
		if v == "" {
			continue
		}
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// resolveBaseConfig applies defaults and validates the shared fields.
// Only Endpoint and APIKey can fail.
func resolveBaseConfig(c BaseConfig, p protocol) (resolvedConfig, error) {
	logMode, modeWarning := resolveLogMode(c.LogMode)
	logger := effectiveLogger(c, logMode)
	warn := makeWarn(logger)
	if modeWarning != "" {
		warn("%s", modeWarning)
	}

	endpoint, err := resolveEndpoint(c.Endpoint)
	if err != nil {
		return resolvedConfig{}, err
	}
	apiKey, err := resolveAPIKey(c.APIKey)
	if err != nil {
		return resolvedConfig{}, err
	}

	r := resolvedConfig{
		endpoint:        endpoint,
		apiKey:          apiKey,
		protocol:        p,
		maxBufferBytes:  resolveBound(c.MaxBufferBytes, defaultMaxBufferBytes, "MaxBufferBytes", minMaxBufferBytes, warn),
		maxCaptureBytes: resolveBound(c.MaxCaptureBytes, defaultMaxCaptureBytes, "MaxCaptureBytes", 1, warn),
		flushInterval:   resolveBound(c.FlushInterval, defaultFlushInterval, "FlushInterval", minFlushInterval, warn),
		drainTimeout:    resolveBound(c.DrainTimeout, defaultDrainTimeout, "DrainTimeout", minDrainTimeout, warn),
		logMode:         logMode,
		logger:          logger,
	}

	// Both values are individually legal but nonsensical together: a
	// capture at the per-message cap would never fit in the buffer, so
	// every large message would be dropped after being redacted.
	if r.maxBufferBytes < r.maxCaptureBytes {
		warn("`MaxBufferBytes` (%d) is below `MaxCaptureBytes` (%d); a full-size capture can never be buffered",
			r.maxBufferBytes, r.maxCaptureBytes)
	}
	return r, nil
}

func resolveOCPIConfig(c OCPIConfig) (resolvedConfig, error) {
	r, err := resolveBaseConfig(c.BaseConfig, protocolOCPI)
	if err != nil {
		return r, err
	}
	r.allowedHeaders = resolveAllowedHeaders(c.OCPIAllowedHeaders)
	return r, nil
}

func resolveOCPPConfig(c OCPPConfig) (resolvedConfig, error) {
	return resolveBaseConfig(c.BaseConfig, protocolOCPP)
}
