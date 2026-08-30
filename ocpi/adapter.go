// Package ocpi provides drop-in capture adapters for OCPI traffic: a
// net/http middleware for the requests partners make to you, and an
// http.RoundTripper for the requests you make to them.
//
// Both assemble the HTTP exchange for you — collect headers and bodies,
// resolve the partner's identity, call the right capture method — so a
// host that speaks stdlib HTTP needs no capture code of its own:
//
//	panda, _ := evpanda.StartOCPI(cfg)
//
//	srv := &http.Server{Handler: auth(ocpi.Middleware(panda)(mux))}
//	client := &http.Client{Transport: ocpi.RoundTripper(panda, nil)}
//
// Identity comes from the request context first (see
// [ContextWithIdentity]) and falls back to the X-EVPanda-* headers;
// [WithResolver] replaces that logic entirely. A request with no
// resolvable identity is served exactly as it would have been and simply
// is not captured — the adapters never block, alter, or fail a request on
// the SDK's account.
//
// This package lives apart from the core so the two can move
// independently: adapters accumulate framework surface over time, the
// capture pipeline does not.
package ocpi

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"

	evpanda "github.com/evpanda-labs/evpanda-go"
)

// Capturer is what the adapters need from a live [evpanda.OCPIClient].
// Taking the interface rather than the concrete type keeps the seam
// explicit and lets a test drive an adapter without a running pipeline.
type Capturer interface {
	CaptureInboundMessage(evpanda.OCPIMessageInput)
	CaptureOutboundMessage(evpanda.OCPIMessageInput)
	Capturing() (maxCaptureBytes int, ok bool)
}

// Compile-time proof that the real client satisfies the seam, so widening
// Capturer fails here rather than at a call site.
var _ Capturer = (*evpanda.OCPIClient)(nil)

// Request headers the shipped resolver reads as a fallback. Matching is
// case-insensitive: net/http canonicalizes header keys, and the
// equivalent Node SDK lowercases them.
const (
	// HeaderPlatformID carries Platform.ID.
	HeaderPlatformID = "X-EVPanda-Platform-Id"
	// HeaderPlatformName carries Platform.Name.
	HeaderPlatformName = "X-EVPanda-Platform-Name"
	// HeaderTenantID carries Platform.TenantID.
	HeaderTenantID = "X-EVPanda-Tenant-Id"
	// HeaderTenantName carries Platform.TenantName.
	HeaderTenantName = "X-EVPanda-Tenant-Name"
)

// identityHeaders is the same set as a list. The round tripper strips
// these before dispatch so a partner never receives them — TenantID and
// TenantName in particular describe your own tenant, not theirs.
var identityHeaders = []string{
	HeaderPlatformID,
	HeaderPlatformName,
	HeaderTenantID,
	HeaderTenantName,
}

// ── Identity on the context ──────────────────────────────────────────────

// identityKey is the unexported context key type, so nothing
// outside this package can collide with or overwrite the value.
type identityKey struct{}

// ContextWithIdentity returns a copy of ctx carrying id, for the
// adapters to pick up later. This is the preferred way to attribute an
// OCPI exchange: resolve the partner wherever you already authenticate
// them, and stash it once.
//
// Inbound, from your auth middleware:
//
//	func auth(next http.Handler) http.Handler {
//		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//			partner, ok := lookupPartner(r.Header.Get("Authorization"))
//			if ok {
//				ctx := ContextWithIdentity(r.Context(), evpanda.Platform{
//					ID:   partner.ID,
//					Name: partner.Name,
//				})
//				r = r.WithContext(ctx)
//			}
//			next.ServeHTTP(w, r)
//		})
//	}
//
// Outbound, on the request you are about to send:
//
//	ctx := ContextWithIdentity(ctx, partnerIdentity)
//	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, partner.URL, nil)
func ContextWithIdentity(ctx context.Context, id evpanda.Platform) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFromContext returns the identity stored by
// [ContextWithIdentity] and reports whether one was present. The
// identity is returned as stored — it is not validated here, so a caller
// can inspect a partial value.
func IdentityFromContext(ctx context.Context) (evpanda.Platform, bool) {
	if ctx == nil {
		return evpanda.Platform{}, false
	}
	id, ok := ctx.Value(identityKey{}).(evpanda.Platform)
	return id, ok
}

// ── Resolver contract ────────────────────────────────────────────────────

// Resolver derives the roaming partner's identity for one request.
// Returning false — or an identity that fails validation — means the
// exchange is not captured; the request itself is never blocked or
// altered because of it.
//
// The whole request is passed so a resolver can read the context, the
// headers, the method or the URL. Adapters call it exactly once per
// request, before the handler or the transport runs.
type Resolver func(r *http.Request) (evpanda.Platform, bool)

// DefaultResolver is what both adapters use when no resolver is
// configured. It reads the identity from the request context first (see
// [ContextWithIdentity]) and falls back to the X-EVPanda-*
// headers.
//
// A request carrying neither is simply not captured — no error, no
// partial record. Tenant stays all-or-nothing: set both tenant values or
// neither, since a half-set pair fails validation and drops the message.
func DefaultResolver(r *http.Request) (evpanda.Platform, bool) {
	if r == nil {
		return evpanda.Platform{}, false
	}
	if id, ok := IdentityFromContext(r.Context()); ok {
		return id, true
	}
	id := evpanda.Platform{
		ID:         strings.TrimSpace(r.Header.Get(HeaderPlatformID)),
		Name:       strings.TrimSpace(r.Header.Get(HeaderPlatformName)),
		TenantID:   strings.TrimSpace(r.Header.Get(HeaderTenantID)),
		TenantName: strings.TrimSpace(r.Header.Get(HeaderTenantName)),
	}
	if id.ID == "" && id.Name == "" {
		return evpanda.Platform{}, false
	}
	return id, true
}

// safeResolve runs a resolver under panic protection and validates what
// it returns. A panicking resolver, a false result, or an invalid
// identity all mean the same thing: skip capture for this request.
func safeResolve(resolve Resolver, r *http.Request) (id evpanda.Platform, ok bool) {
	defer func() {
		if recover() != nil {
			id, ok = evpanda.Platform{}, false
		}
	}()
	id, ok = resolve(r)
	if !ok || !id.Valid() {
		return evpanda.Platform{}, false
	}
	return id, true
}

// ── Adapter options ──────────────────────────────────────────────────────

type options struct {
	resolve Resolver
}

// Option configures [Middleware] and [RoundTripper].
type Option func(*options)

// WithResolver replaces [DefaultResolver]. Use it when identity
// comes from somewhere the shipped resolver doesn't look — a client
// certificate, a path prefix, or your own session store.
func WithResolver(resolve Resolver) Option {
	return func(o *options) {
		if resolve != nil {
			o.resolve = resolve
		}
	}
}

func newOptions(opts []Option) options {
	o := options{resolve: DefaultResolver}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// ── Bounded body accumulation ────────────────────────────────────────────

// cappedBody accumulates at most max bytes. Crossing the cap sets
// overflowed and releases what was held: the SDK drops an oversize
// exchange rather than storing half a body, so there is nothing to keep.
//
// It is mutex-guarded because a request body is written by the
// transport's own goroutine while the capture is assembled on the
// caller's.
type cappedBody struct {
	mu         sync.Mutex
	buf        []byte
	max        int
	overflowed bool
}

func newCappedBody(limit int) *cappedBody {
	return &cappedBody{max: limit}
}

func (b *cappedBody) push(p []byte) {
	if len(p) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.overflowed {
		return
	}
	if len(b.buf)+len(p) > b.max {
		b.overflowed = true
		b.buf = nil
		return
	}
	b.buf = append(b.buf, p...)
}

// result returns the accumulated bytes and whether the cap was exceeded.
func (b *cappedBody) result() (body []byte, overflowed bool) {
	if b == nil {
		return nil, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf, b.overflowed
}

// ── Header conversion ────────────────────────────────────────────────────

// headerMap flattens an http.Header into the SDK's map, lowercasing keys
// and comma-joining repeated values — the same normalization the Node SDK
// applies, so both ship identical records. The OCPI redactor filters this
// against the allowlist afterwards.
func headerMap(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		switch len(v) {
		case 0:
			continue
		case 1:
			out[strings.ToLower(k)] = v[0]
		default:
			out[strings.ToLower(k)] = strings.Join(v, ", ")
		}
	}
	return out
}

// ── Body recording ───────────────────────────────────────────────────────

// teeReadCloser records what is read from rc, up to the body's cap, and
// optionally fires onDone once the stream is exhausted or closed —
// whichever happens first, and only once. A caller that reads to EOF and
// forgets to Close still gets its exchange captured.
type teeReadCloser struct {
	rc     io.ReadCloser
	body   *cappedBody
	onDone func()
	once   sync.Once
}

func (t *teeReadCloser) Read(p []byte) (int, error) {
	n, err := t.rc.Read(p)
	if n > 0 {
		t.body.push(p[:n])
	}
	if err != nil {
		t.finish()
	}
	return n, err
}

func (t *teeReadCloser) Close() error {
	err := t.rc.Close()
	t.finish()
	return err
}

func (t *teeReadCloser) finish() {
	if t.onDone == nil {
		return
	}
	t.once.Do(t.onDone)
}

// capturing asks the client what it can capture, tolerating a Capturer
// that misbehaves — a typed-nil client, or a third-party implementation
// with a fault of its own. Either way the adapter falls back to "not
// capturing", which is a pass-through rather than a broken host.
func capturing(c Capturer) (maxCaptureBytes int, ok bool) {
	if c == nil {
		return 0, false
	}
	defer func() {
		if recover() != nil {
			maxCaptureBytes, ok = 0, false
		}
	}()
	return c.Capturing()
}

// guard runs fn under a recover, so a fault while assembling a capture
// can never reach the host. The capture calls themselves are already
// guarded inside the SDK — that is where a panic gets counted — so this
// covers only the adapter's own bookkeeping.
func guard(fn func()) {
	defer func() { _ = recover() }()
	fn()
}
