# evpanda-go

[![CI](https://github.com/evpanda-labs/evpanda-go/actions/workflows/build.yml/badge.svg)](https://github.com/evpanda-labs/evpanda-go/actions/workflows/build.yml)

Go SDK for EVPanda. Embed it in your OCPI server or OCPP CSMS;
it records protocol messages, buffers them in-process, and ships them
in batches to the EVPanda ingestion API.

## Install

```sh
go get github.com/evpanda-labs/evpanda-go@latest
```

```go
import "github.com/evpanda-labs/evpanda-go" // package evpanda
```

## Quick start

**The protocol is the client.** `StartOCPI` returns an `*OCPIClient`,
`StartOCPP` an `*OCPPClient` — pick the one your service speaks. Both
constructors always return a usable, non-nil client: on a bad config the
client is an inert no-op and the error describes the problem, so your boot
never crashes.

### OCPI

```go
package main

import (
	"log"

	evpanda "github.com/evpanda-labs/evpanda-go"
)

func main() {
	// APIKey omitted ⇒ read from EVPANDA_API_KEY.
	panda, err := evpanda.StartOCPI(evpanda.OCPIConfig{
		BaseConfig: evpanda.BaseConfig{
			Endpoint: "https://ingest.evpanda.io",
		},
		OCPIAllowedHeaders: []string{"x-custom-trace"}, // extends the allowlist
	})
	if err != nil {
		log.Printf("evpanda: %v (running inert)", err)
	}
	defer func() { _ = panda.Close() }() // flushes what's buffered, within DrainTimeout

	panda.CaptureInbound(evpanda.OCPIMessageInput{ // partner → host
		Identity: evpanda.RoamingIdentity{
			PlatformID:   "acme",
			PlatformName: "Acme Mobility",
			TenantID:     "cpo-42", // tenant is all-or-nothing
			TenantName:   "CPO 42",
		},
		HTTP: evpanda.CapturedHTTP{
			Method:         "POST",
			URL:            "/ocpi/2.2/cdrs",
			StatusCode:     200,
			RequestHeaders: map[string]string{"content-type": "application/json"},
			RequestBody:    []byte(`{"id":"..."}`),
		},
	})
}
```

`CaptureOutbound` (host → partner) is the same call in the other
direction; the method stamps the direction for you.

### OCPP

The recommended path is a **session handle** — it mints the connection ID
and carries the identity, so per-frame calls carry neither:

```go
panda, err := evpanda.StartOCPP(evpanda.OCPPConfig{
	BaseConfig: evpanda.BaseConfig{Endpoint: "https://ingest.evpanda.io"},
})
if err != nil {
	log.Printf("evpanda: %v (running inert)", err)
}
defer func() { _ = panda.Close() }()

// On WebSocket connect — records the connect and returns the handle.
sess := panda.Connection(evpanda.ChargerIdentity{ChargerID: "CP-001"})
sess.Message([]byte(`[2,"id","BootNotification",{}]`), evpanda.FromCP)
sess.Disconnect() // on socket close
```

`CaptureConnect` / `CaptureMessage` / `CaptureDisconnect` are the flat
primitives underneath, taking an `OCPPMessageInput`, for one-off capture.

Capture is **non-blocking and never panics** — messages are buffered and
delivered by a background goroutine. One client serves one protocol.

## Identity

Every message carries its own identity; the SDK validates it and silently
drops what it can't attribute (it never panics back at you).

- **OCPI →** `RoamingIdentity`: `PlatformID` + `PlatformName` required.
- **OCPP →** `ChargerIdentity`: `ChargerID` required.
- `TenantID` + `TenantName` are optional but **all-or-nothing** — supply
  both or neither.

Identity is per message, not global config — one OCPI process can serve
many platforms and tenants; one OCPP process many chargers. OCPP identity
is known at connect time, so the session handle carries it for you.

## Configuration

`Endpoint` and `APIKey` are required (`APIKey` may come from the
`EVPANDA_API_KEY` env var). Every other field falls back to its default
when left at the zero value; an out-of-range value is rejected at `Start*`
(inert client + error).

| Field                | Default            | Description                                                              |
|----------------------|--------------------|--------------------------------------------------------------------------|
| `Endpoint`           | —                  | Ingestion API base URL (`http(s)://…`).                                  |
| `APIKey`             | `$EVPANDA_API_KEY` | Sent as `X-API-Key`. Falls back to the env var when empty.               |
| `BufferCapacity`     | `10000`            | Ring-buffer slots. Worst-case mem = `BufferCapacity × MaxCaptureBytes`.  |
| `MaxCaptureBytes`    | `65536`            | Per-body / per-frame cap. An oversize body drops the whole message.      |
| `FlushInterval`      | `5s`               | Max time between flushes (`time.Duration`).                              |
| `DrainTimeout`       | `10s`              | `Close` drain deadline. An explicit value must be ≥ `5s`.                |
| `Compression`        | `"zstd"`           | `"zstd"` or `"gzip"`.                                                    |
| `Debug`              | `false`            | Master log switch; silent by default.                                    |
| `Logger`             | `nil`              | `*slog.Logger` used when `Debug` is true; if nil, `slog.Default()`.      |
| `OCPIAllowedHeaders` | `nil`              | *(OCPIConfig only)* Extra headers to capture on top of the default allowlist. |

## Behavior

- **Batched delivery.** Messages flush when the buffer reaches 1000 or on
  `FlushInterval`, whichever comes first; each POST carries at most 1000
  records.
- **Backpressure = drop-oldest.** If the upstream is slow or down, the ring
  caps at `BufferCapacity` and discards the oldest. Your app never blocks.
- **Redaction at the chokepoint.** Every capture goes through one
  validate → cap → redact step before the queue. OCPI keeps a **header
  allowlist** (`OCPIAllowedHeaders` extends it, never shrinks it —
  `Authorization`, `Cookie`, `X-API-Key` and anything else unlisted fall
  off) and masks the `token` field on `/credentials` bodies; OCPP frames
  are captured verbatim today, behind a seam ready for masking.
- **Resilient transport.** Bounded retry (max 5 attempts) with capped
  exponential backoff + full jitter on other status / network errors;
  permanent rejections (400/401/413) are dropped without retry storms.
  Payloads under 1 KiB are sent uncompressed.
- **Graceful shutdown.** `Close()` flushes what's buffered within
  `DrainTimeout`, then stops. Idempotent; post-close captures are safe
  no-ops. Returns `evpanda.ErrDrainIncomplete` if the deadline elapsed
  with messages still buffered, else `nil`.
- **Error reporting.** `Flush()` and `Close()` return errors but never
  panic into the caller. Delivery failures are retried/dropped by design
  and not surfaced as return values; with `Debug: true` each dropped batch
  is logged.

## Development

```sh
just lint   # gofmt + golangci-lint
just vet
just test   # go test -race -count=1 ./...
just build
```
