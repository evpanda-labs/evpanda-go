package evpanda

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// env builds a probe envelope tagged with i, addressable through the OCPI
// message's URL. The tag is fixed-width so every probe accounts for the
// same number of bytes, which lets a test set an exact budget.
func env(i int) bufferedMessage {
	return bufferedMessage{
		capturedAt: nowISO(),
		message:    ocpiMessage{Data: HTTPExchange{URL: fmt.Sprintf("%08d", i)}},
	}
}

// probeSize is the accounted footprint of one env().
const probeSize = envelopeOverhead + 8

func urls(batch []bufferedMessage) []string {
	out := make([]string, len(batch))
	for i, e := range batch {
		out[i] = strings.TrimLeft(e.message.(ocpiMessage).Data.URL, "0")
		if out[i] == "" {
			out[i] = "0"
		}
	}
	return out
}

func wantURLs(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("drain = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("drain = %v, want %v", got, want)
		}
	}
}

func TestProbeSizeMatchesAccounting(t *testing.T) {
	if got := env(0).message.size(); got != probeSize {
		t.Fatalf("probe size = %d, want %d — the test budgets below assume it", got, probeSize)
	}
}

func TestRingBufferRejectsBadBudget(t *testing.T) {
	for _, budget := range []int{0, -1} {
		if _, err := newRingBuffer(budget, nil); err == nil {
			t.Fatalf("newRingBuffer(%d, nil) must fail", budget)
		}
	}
}

func TestRingBufferDrainsFIFO(t *testing.T) {
	r, err := newRingBuffer(4*probeSize, nil)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	for i := range 3 {
		if got := r.enqueue(env(i)); got != i+1 {
			t.Fatalf("enqueue returned %d, want %d", got, i+1)
		}
	}
	if got := r.byteLen(); got != 3*probeSize {
		t.Fatalf("byteLen = %d, want %d", got, 3*probeSize)
	}

	wantURLs(t, urls(r.drain()), "0", "1", "2")

	if r.length() != 0 || r.byteLen() != 0 {
		t.Fatalf("drain must reset the buffer: %d messages, %d bytes", r.length(), r.byteLen())
	}
	if r.drain() != nil {
		t.Fatal("draining an empty buffer must return nil")
	}
}

// Past the budget the oldest go, and the survivors still come out
// oldest-first.
func TestRingBufferDropsOldest(t *testing.T) {
	r, err := newRingBuffer(3*probeSize, nil)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	for i := range 7 { // 0..6 at a 3-message budget ⇒ 4,5,6 survive
		if got := r.enqueue(env(i)); got > 3 {
			t.Fatalf("enqueue returned %d, must never exceed the budget", got)
		}
	}
	wantURLs(t, urls(r.drain()), "4", "5", "6")
}

// The budget is in bytes, so a big message displaces several small ones
// rather than one slot displacing one slot.
func TestRingBufferEvictsByBytesNotCount(t *testing.T) {
	r, err := newRingBuffer(6*probeSize, nil)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	for i := range 6 {
		r.enqueue(env(i))
	}
	if r.length() != 6 {
		t.Fatalf("length = %d, want 6", r.length())
	}

	// One message worth about three probes evicts about three of them.
	big := bufferedMessage{
		capturedAt: nowISO(),
		message: ocpiMessage{Data: HTTPExchange{
			URL:         fmt.Sprintf("%08d", 99),
			RequestBody: make([]byte, 2*probeSize),
		}},
	}
	r.enqueue(big)

	if got := r.byteLen(); got > 6*probeSize {
		t.Fatalf("byteLen = %d, over the %d budget", got, 6*probeSize)
	}
	batch := r.drain()
	if len(batch) != 4 { // 3 evicted (#0,#1,#2), #3..#5 kept, + the big one
		t.Fatalf("kept %d messages, want 4: %v", len(batch), urls(batch))
	}
	if got := urls(batch); got[0] != "3" || got[len(got)-1] != "99" {
		t.Fatalf("survivors = %v, want oldest-evicted order ending in the big message", got)
	}
}

// A message that could never fit is dropped on its own, rather than
// emptying the buffer for something that still would not fit.
func TestRingBufferDropsUnfittableMessage(t *testing.T) {
	r, err := newRingBuffer(4*probeSize, nil)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	r.enqueue(env(1))
	r.enqueue(env(2))

	huge := bufferedMessage{
		capturedAt: nowISO(),
		message: ocpiMessage{Data: HTTPExchange{
			URL:         fmt.Sprintf("%08d", 3),
			RequestBody: make([]byte, 10*probeSize),
		}},
	}
	if got := r.enqueue(huge); got != 2 {
		t.Fatalf("enqueue returned %d, want the buffer left untouched at 2", got)
	}
	wantURLs(t, urls(r.drain()), "1", "2")
}

// The budget is a true ceiling under mixed sizes, not an average.
func TestRingBufferNeverExceedsBudget(t *testing.T) {
	const budget = 40 * probeSize
	r, err := newRingBuffer(budget, nil)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	for i := range 2000 {
		r.enqueue(bufferedMessage{
			capturedAt: nowISO(),
			message: ocpiMessage{Data: HTTPExchange{
				URL:         fmt.Sprintf("%08d", i),
				RequestBody: make([]byte, (i*37)%(3*probeSize)),
			}},
		})
		if got := r.byteLen(); got > budget {
			t.Fatalf("byteLen = %d after %d enqueues, over the %d budget", got, i+1, budget)
		}
	}
}

// Eviction-heavy traffic must not grow the backing array without bound —
// compaction reuses the space the evicted prefix left behind.
func TestRingBufferCompactsEvictedPrefix(t *testing.T) {
	r, err := newRingBuffer(4*probeSize, nil)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	for i := range 10_000 {
		r.enqueue(env(i))
	}
	if got := cap(r.buf); got > 64 {
		t.Fatalf("backing array grew to %d slots for a 4-message budget", got)
	}
}

// Reused and drained slots must not pin the message they held, or a busy
// buffer would keep every message it ever dropped alive.
func TestRingBufferReleasesReferences(t *testing.T) {
	r, err := newRingBuffer(2*probeSize, nil)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	r.enqueue(env(0))
	r.enqueue(env(1))
	r.enqueue(env(2)) // evicts #0
	r.drain()

	for _, slot := range r.buf[:cap(r.buf)] {
		if slot.message != nil {
			t.Fatal("every slot must be cleared by eviction or drain")
		}
	}
}

func TestRingBufferConcurrentProducers(t *testing.T) {
	r, err := newRingBuffer(1000*probeSize, nil)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	var wg sync.WaitGroup
	for g := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 100 {
				r.enqueue(env(g*100 + i))
			}
		}()
	}
	wg.Wait()
	if got := r.length(); got != 1000 {
		t.Fatalf("length = %d, want 1000", got)
	}
	if got := len(r.drain()); got != 1000 {
		t.Fatalf("drained %d, want 1000", got)
	}
}

// Producers and the worker meet only here, so enqueue and drain must
// interleave safely — and nothing may be lost or duplicated between them.
func TestRingBufferConcurrentEnqueueAndDrain(t *testing.T) {
	const producers, each = 8, 500
	r, err := newRingBuffer(producers*each*probeSize, nil) // roomy: no eviction
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}

	// One drainer, mirroring the worker: drain, yield, repeat.
	stop := make(chan struct{})
	drained := make(chan []string, 1)
	go func() {
		var seen []string
		for {
			select {
			case <-stop:
				drained <- append(seen, urls(r.drain())...) // final sweep
				return
			default:
				seen = append(seen, urls(r.drain())...)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	var produced sync.WaitGroup
	for g := range producers {
		produced.Add(1)
		go func() {
			defer produced.Done()
			for i := range each {
				r.enqueue(env(g*each + i))
			}
		}()
	}
	produced.Wait()
	close(stop)

	seen := map[string]int{}
	for _, u := range <-drained {
		seen[u]++
	}
	if len(seen) != producers*each {
		t.Fatalf("saw %d distinct messages, want %d", len(seen), producers*each)
	}
	for u, n := range seen {
		if n != 1 {
			t.Fatalf("message %s appeared %d times, want exactly once", u, n)
		}
	}
	if r.length() != 0 || r.byteLen() != 0 {
		t.Fatalf("buffer not empty after the final drain: %d / %d", r.length(), r.byteLen())
	}
}

// byteLen tracks the accounted size exactly across enqueue, eviction and
// drain — it is the number an operator provisions against.
func TestRingBufferByteAccounting(t *testing.T) {
	r, err := newRingBuffer(10*probeSize, nil)
	if err != nil {
		t.Fatalf("newRingBuffer: %v", err)
	}
	for i := range 4 {
		r.enqueue(env(i))
		if want := (i + 1) * probeSize; r.byteLen() != want {
			t.Fatalf("byteLen after %d enqueues = %d, want %d", i+1, r.byteLen(), want)
		}
	}
	// Past the budget the total holds steady rather than growing.
	for i := range 50 {
		r.enqueue(env(100 + i))
		if r.byteLen() > 10*probeSize {
			t.Fatalf("byteLen = %d, over the budget", r.byteLen())
		}
	}
	if r.byteLen() != 10*probeSize {
		t.Fatalf("a saturated buffer should sit at its budget, got %d", r.byteLen())
	}
	r.drain()
	if r.byteLen() != 0 {
		t.Fatalf("byteLen after drain = %d, want 0", r.byteLen())
	}
}

// OCPP frames account for their own footprint too, so a byte budget
// covers both protocols on the same terms.
func TestOCPPMessageAccounting(t *testing.T) {
	base := ocppMessage{
		EventType:    ocppEventTypeMessage,
		Identity:     Charger{ID: "CP-001"},
		ConnectionID: "5f2c1a9e",
		Direction:    FromCP,
	}
	empty := base.size()
	if empty <= envelopeOverhead {
		t.Fatalf("size %d must include the identity and connection id", empty)
	}
	base.Payload = make([]byte, 1000)
	if got := base.size(); got != empty+1000 {
		t.Fatalf("size with a 1000-byte frame = %d, want %d", got, empty+1000)
	}
}

// A connect event carries no frame, so it costs far less than a message —
// which is the whole reason the budget is bytes and not a slot count.
func TestConnectEventIsCheaperThanAMessage(t *testing.T) {
	id := Charger{ID: "CP-001"}
	connect := ocppMessage{EventType: ocppEventTypeConnect, Identity: id, ConnectionID: "c1"}
	message := ocppMessage{
		EventType: ocppEventTypeMessage, Identity: id, ConnectionID: "c1",
		Direction: FromCP, Payload: make([]byte, 4096),
	}
	if connect.size() >= message.size() {
		t.Fatalf("connect %d should cost less than a 4 KiB frame %d",
			connect.size(), message.size())
	}
}
