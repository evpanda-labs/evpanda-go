# evpanda-go

[![CI](https://github.com/evpanda-labs/evpanda-go/actions/workflows/build.yml/badge.svg)](https://github.com/evpanda-labs/evpanda-go/actions/workflows/build.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/evpanda-labs/evpanda-go.svg)](https://pkg.go.dev/github.com/evpanda-labs/evpanda-go)

Passive OCPI / OCPP traffic capture for Go. Embed it in your OCPI server or
OCPP CSMS and it records protocol messages, buffers them in memory, and ships
them in batches to the EVPanda ingestion API.

- **Non-blocking.** Capture calls never wait on the network and never panic
  into your process.
- **Bounded.** Memory is capped by a byte budget you set; under pressure the
  SDK drops its own data rather than yours.
- **Safe by default.** Secrets are stripped before anything is buffered.
- **Small.** One dependency (`klauspost/compress`), one background goroutine
  per client.

## Requirements

Go 1.24 or later.

## Installation

```sh
go get github.com/evpanda-labs/evpanda-go
```

```go
import (
	evpanda "github.com/evpanda-labs/evpanda-go"
	"github.com/evpanda-labs/evpanda-go/ocpi" // optional HTTP adapters
)
```

## Quick start

Pick the client for the protocol your service speaks. `StartOCPI` and
`StartOCPP` always return a usable client — if the config is bad you get an
inert one plus an error, so a typo can't stop your service from booting.

Set `EVPANDA_API_KEY` in the environment, or pass `APIKey` in the config.

### OCPP

`Connection` returns a session handle that mints the connection ID and
carries the charger identity, so per-frame calls need neither.

```go
panda, err := evpanda.StartOCPP(evpanda.OCPPConfig{
	BaseConfig: evpanda.BaseConfig{Endpoint: "https://ingest.evpanda.io"},
})
if err != nil {
	log.Printf("evpanda: %v (running inert)", err)
}
defer func() { _ = panda.Close() }()

func handleCharger(w http.ResponseWriter, r *http.Request) {
	identity, ok := resolveChargerIdentity(r) // however your CSMS does it
	if !ok {
		http.Error(w, "unknown charge point", http.StatusUnauthorized)
		return
	}

	conn := upgrade(w, r)
	sess := panda.Connection(identity) // records the connect
	defer sess.Disconnect()            // records the close

	for {
		frame, err := conn.Read()
		if err != nil {
			return
		}
		sess.Message(frame, evpanda.FromCP)

		reply := handleFrame(frame) // your CSMS logic
		conn.Write(reply)
		sess.Message(reply, evpanda.ToCP)
	}
}
```

`Direction` is from the charge point's perspective: `FromCP` for frames it
sent you, `ToCP` for frames you send it. Use one session per socket — its
connection ID ties the connect, every frame and the disconnect into a single
session, and a reconnect gets a fresh one.

`CaptureConnect` / `CaptureMessage` / `CaptureDisconnect` are the flat
primitives underneath, for cases a session handle doesn't fit.

### OCPI

Two methods, one per direction. The method name sets the direction — there's
no field to get backwards.

| Method | You are the… | Typical case |
|---|---|---|
| `CaptureInboundMessage` | server | A partner pushes a CDR to your endpoint |
| `CaptureOutboundMessage` | client | You pull a partner's locations |

`Identity` is always the **partner on the other side** — never your own
platform.

```go
panda, err := evpanda.StartOCPI(evpanda.OCPIConfig{
	BaseConfig: evpanda.BaseConfig{Endpoint: "https://ingest.evpanda.io"},
})
if err != nil {
	log.Printf("evpanda: %v (running inert)", err)
}
defer func() { _ = panda.Close() }()

data := evpanda.HTTPExchange{
	Method:          "POST",
	URL:             "/ocpi/2.2/cdrs",
	StatusCode:      201,
	RequestHeaders:  map[string]string{"content-type": "application/json"},
	ResponseHeaders: map[string]string{"content-type": "application/json"},
}
data.SetRequestBody(reqBytes)   // copies — reuse your buffer right after
data.SetResponseBody(respBytes)

panda.CaptureInboundMessage(evpanda.OCPIMessageInput{
	Identity: evpanda.RoamingIdentity{PlatformID: "acme", PlatformName: "Acme Mobility"},
	Data:     data,
})
```

Use `SetRequestBody` / `SetResponseBody` rather than assigning the fields.
They copy, so you're free to reuse a pooled reader's buffer as soon as the
call returns.

`StatusCode` and both bodies are optional; header maps may be nil.

## HTTP adapters

The `ocpi` package wraps stdlib HTTP so you don't have to assemble exchanges
yourself.

```go
// Inbound — mount it after whatever authenticates the partner.
srv := &http.Server{Handler: auth(ocpi.Middleware(panda)(mux))}

// Outbound — a nil base means http.DefaultTransport.
client := &http.Client{Transport: ocpi.RoundTripper(panda, nil)}
```

`Middleware` is the standard `func(http.Handler) http.Handler`, so it works
with net/http, chi, gorilla/mux, `echo.WrapMiddleware` and `gin.WrapH`.

Both adapters take identity from the request **context** first, falling back
to `X-EVPanda-*` headers. Stamp the context wherever you already look the
partner up:

```go
func auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		partner, ok := lookupPartner(r.Header.Get("Authorization"))
		if !ok {
			http.Error(w, "unknown partner", http.StatusUnauthorized)
			return
		}
		ctx := ocpi.ContextWithIdentity(r.Context(), evpanda.RoamingIdentity{
			PlatformID:   partner.ID,
			PlatformName: partner.Name,
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
```

Outbound, stamp it on the request you're about to send:

```go
ctx = ocpi.ContextWithIdentity(ctx, evpanda.RoamingIdentity{
	PlatformID: partner.ID, PlatformName: partner.Name,
})
req, _ := http.NewRequestWithContext(ctx, http.MethodPost, partner.URL, body)
req.Header.Set("Authorization", "Token "+partner.TokenB)
resp, err := client.Do(req)
```

The `X-EVPanda-*` headers are stripped before dispatch, so partners never see
them. If identity lives somewhere else entirely — a client certificate, a
path prefix — pass your own resolver:

```go
byPath := func(r *http.Request) (evpanda.RoamingIdentity, bool) {
	name, ok := strings.CutPrefix(r.URL.Path, "/partners/")
	if !ok {
		return evpanda.RoamingIdentity{}, false // not captured
	}
	return evpanda.RoamingIdentity{PlatformID: name, PlatformName: name}, true
}

ocpi.Middleware(panda, ocpi.WithResolver(byPath))
ocpi.RoundTripper(panda, nil, ocpi.WithResolver(byPath))
```

A request with no resolvable identity is served exactly as it would have
been — it just isn't captured.

## Identity

Every message carries its own identity; messages the SDK can't attribute are
dropped rather than shipped as orphans.

| Protocol | Type | Required fields |
|---|---|---|
| OCPI | `RoamingIdentity` | `PlatformID`, `PlatformName` |
| OCPP | `ChargerIdentity` | `ChargerID` |

`TenantID` and `TenantName` are optional but **all-or-nothing** — set both or
neither. Call `id.Valid()` to check one yourself.

## Configuration

`Endpoint` and `APIKey` are required; `APIKey` falls back to
`$EVPANDA_API_KEY`. Everything else takes its default when left at the zero
value, and an out-of-range value falls back to that default with a warning
rather than failing.

Those two are the only things `Start*` can fail on, and the failure is
matchable — useful because a missing key is usually a deployment problem
while a bad endpoint is a code one:

```go
panda, err := evpanda.StartOCPI(cfg)
if errors.Is(err, evpanda.ErrAPIKey) {
	log.Fatal("EVPANDA_API_KEY is not set in this environment")
}
if err != nil {
	log.Printf("evpanda: %v (running inert)", err)
}
```

| Field | Default | Description |
|---|---|---|
| `Endpoint` | — | Ingestion API base URL (`http(s)://…`) |
| `APIKey` | `$EVPANDA_API_KEY` | Sent as `X-API-Key` |
| `MaxBufferBytes` | `32 MiB` | Memory ceiling for undelivered captures; oldest are evicted past it |
| `MaxCaptureBytes` | `64 KiB` | Per body / per frame cap; an oversize body drops the whole message |
| `FlushInterval` | `5s` | Maximum time between deliveries |
| `DrainTimeout` | `10s` | How long `Close` waits to drain (minimum `5s`) |
| `Compression` | `zstd` | `CompressionZstd` or `CompressionGzip` |
| `LogMode` | `errors` | `LogModeSilent`, `LogModeErrors`, `LogModeDebug` |
| `Logger` | `slog.Default()` | Where the SDK's own logs go |
| `OCPIAllowedHeaders` | — | *(OCPI only)* Extra headers to capture, on top of the defaults |

## Logging

The SDK reports problems to your logger by default, at a bounded rate: at
most one summary line per minute, and nothing at all while it's healthy.

```
level=WARN msg="evpanda: captures dropped" window=1m0s captured=12 invalid_identity=148302 buffered=0 buffer_bytes=0
```

Set `LogMode` to change that, or `EVPANDA_LOG=silent|errors|debug` to change
it without touching code:

| Mode | Output |
|---|---|
| `LogModeSilent` | Nothing. Counters still work. |
| `LogModeErrors` | Default. Config problems at startup, plus the per-minute summary. |
| `LogModeDebug` | Adds per-batch delivery failures and a summary on close. |

## Shutdown

```go
srv.Shutdown(ctx)                  // stop accepting first…
if err := panda.Shutdown(ctx); err != nil {
	log.Printf("evpanda: %v", err) // …then drain what was captured
}
```

`Close()` does the same using `DrainTimeout` instead of your context. Both
are idempotent, and captures after them are safe no-ops. They return
`evpanda.ErrDrainIncomplete` if the deadline passed with messages still
buffered.

`Flush()` forces an immediate delivery and waits for it. It blocks for as
long as the transport's retries take, so use it at shutdown or while
debugging — not on a request path.

## Documentation

- [API reference on pkg.go.dev](https://pkg.go.dev/github.com/evpanda-labs/evpanda-go)
- [Architecture and design notes](https://claude.ai/code/artifact/7ca90c40-e7f0-4dad-b833-74e82e86fa60)
  — how it works, and why
