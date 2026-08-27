module github.com/evpanda-labs/evpanda-go

// One runtime dependency: github.com/klauspost/compress (pure Go, no
// transitive deps) for zstd — the default compression. Everything else is
// stdlib.
//
// Kept current by hand for zstd security/perf fixes. Its own go.mod sets
// the consumer Go floor — v1.18.6 requires go 1.24 — so this module's `go`
// directive and the CI matrix follow it. A future bump may raise the floor
// again; update `go` below, the CI matrix, and the README together.
go 1.24

require github.com/klauspost/compress v1.19.2
