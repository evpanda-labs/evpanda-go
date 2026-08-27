// Test fixtures for the adapter suite: a stand-in ingestion API and the
// waiting helper. The core package has its own copies — duplicating a
// handful of lines is cheaper than exporting test scaffolding, and it
// keeps the two suites independent.

package ocpi_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	evpanda "github.com/evpanda-labs/evpanda-go"
)

type received struct {
	path    string
	headers http.Header
	records []map[string]any
}

// mockUpstream stands in for the ingestion API and records what it was
// sent. Compression is left off in these tests, so bodies arrive plain.
type mockUpstream struct {
	mu       sync.Mutex
	server   *httptest.Server
	received []received
}

func startMockUpstream() *mockUpstream {
	m := &mockUpstream{}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var body struct {
			Messages []map[string]any `json:"messages"`
		}
		_ = json.Unmarshal(raw, &body)

		m.mu.Lock()
		m.received = append(m.received, received{
			path:    r.URL.Path,
			headers: r.Header.Clone(),
			records: body.Messages,
		})
		m.mu.Unlock()

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"captured":0,"failed":0}`))
	}))
	return m
}

func (m *mockUpstream) recordsFor(path string) []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []map[string]any
	for _, r := range m.received {
		if r.path == path {
			out = append(out, r.records...)
		}
	}
	return out
}

func (m *mockUpstream) close() { m.server.Close() }

func waitFor(t *testing.T, predicate func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !predicate() {
		if time.Now().After(deadline) {
			t.Fatal("waitFor: timed out")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// ocpiConfig points a client at the mock with a fast flush, and no
// compression so the mock can read bodies without decoding.
func ocpiConfig(endpoint string) evpanda.OCPIConfig {
	return evpanda.OCPIConfig{
		BaseConfig: evpanda.BaseConfig{
			Endpoint:      endpoint,
			APIKey:        "test-key",
			FlushInterval: 100 * time.Millisecond,
			LogMode:       evpanda.LogModeSilent,
		},
	}
}
