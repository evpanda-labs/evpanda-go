module github.com/ev-panda/evpanda-go

// One runtime dependency: github.com/klauspost/compress (pure Go, no
// transitive deps) for zstd — the default compression. Everything else is
// stdlib.
//
// Tracks the LATEST klauspost/compress (kept current for zstd security/
// perf fixes). That sets the consumer Go floor: v1.18.6 requires go 1.24,
// so this module's `go` directive and the CI matrix follow it. Dependabot
// keeps it current; a future bump may raise `go` again — update the CI
// matrix and README to match.
go 1.24

require github.com/klauspost/compress v1.19.0
