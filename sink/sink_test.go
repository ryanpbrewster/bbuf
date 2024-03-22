package sink_test

import (
	"bytes"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ryanpbrewster/bbuf/sink"
)

func Test_Smoke(t *testing.T) {
	inner := &testSink{}
	s := sink.New(inner)
	defer s.Close()

	payload := []byte("hello")
	s.Write(payload)
	s.Sync()

	if got, want := inner.payloads, [][]byte{payload}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func Test_ClosedMeansPassthrough(t *testing.T) {
	inner := &testSink{}
	s := sink.New(inner)
	s.Close()

	payload := []byte("hello")
	s.Write(payload)

	// No need to Sync(), because s is closed there is no buffering
	if got, want := inner.payloads, [][]byte{payload}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func Test_SyncDoesNotPanic_OnClosedSinks(t *testing.T) {
	inner := &testSink{}
	s := sink.New(inner)
	s.Close()
	// just test that this doesn't panic
	s.Sync()
}

func Test_Batching(t *testing.T) {
	inner := &testSink{
		start: make(chan struct{}),
		end:   make(chan struct{}),
	}
	s := sink.New(inner)
	defer s.Close()

	// The first write will trigger an immedate flush. Wait for that to begin.
	s.Write([]byte("aaa"))
	inner.start <- struct{}{}

	// Now perform two more writes. These will be flushed together in a batch.
	s.Write([]byte("bbb"))
	s.Write([]byte("ccc"))

	// Unblock the writer and wait for it to fully flush
	close(inner.start)
	close(inner.end)
	s.Sync()

	if got, want := inner.payloads, [][]byte{
		[]byte("aaa"),
		[]byte("bbbccc"),
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func Test_OverflowDiscard_Works(t *testing.T) {
	inner := &testSink{start: make(chan struct{})}
	s := sink.New(inner, sink.WithOverflow(sink.Discard), sink.WithCapacity(16))
	defer s.Close()
	defer close(inner.start)

	// Fill the buffer
	s.Write(bytes.Repeat([]byte("a"), 12))
	// Any subsequent writes will be discarded, without blocking
	for i := 0; i < 10; i++ {
		s.Write(bytes.Repeat([]byte("a"), 12))
	}
	if got, want := s.NumOverflows(), uint64(10); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

type testSink struct {
	lock     sync.Mutex
	payloads [][]byte

	// Just to simplify synchronization in tests,
	// these two channels exist to track whether the sink is mid-write
	start chan struct{}
	end   chan struct{}
}

func (t *testSink) Write(p []byte) (int, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.start != nil {
		<-t.start
	}

	clone := make([]byte, len(p))
	copy(clone, p)
	t.payloads = append(t.payloads, clone)

	if t.end != nil {
		<-t.end
	}
	return len(p), nil
}

type benchSink struct {
	writes     int
	totalBytes uint64
}

func (b *benchSink) Write(p []byte) (n int, err error) {
	time.Sleep(1 * time.Millisecond)
	b.writes++
	b.totalBytes += uint64(len(p))
	return len(p), nil
}

func Benchmark_Baseline(b *testing.B) {
	b.ReportAllocs()
	inner := &benchSink{}
	payload := bytes.Repeat([]byte("a"), 1024)
	for i := 0; i < b.N; i++ {
		inner.Write(payload)
	}
}

func Benchmark_OverflowFlush(b *testing.B) {
	benchmarkOverflow(b, sink.Flush)
}
func Benchmark_OverflowDiscard(b *testing.B) {
	benchmarkOverflow(b, sink.Discard)
}

// Benchmark_Baseline-10                        2054           1212308.00 ns/op            0 B/op          0 allocs/op
// Benchmark_OverflowFlush-10                 146078             17705.00 ns/op           33 B/op          0 allocs/op
// Benchmark_OverflowDiscard-10            163520164                14.59 ns/op            0 B/op          0 allocs/op

func benchmarkOverflow(b *testing.B, behavior sink.OverflowBehavior) {
	b.ReportAllocs()

	inner := &benchSink{}
	s := sink.New(inner, sink.WithOverflow(behavior))
	defer s.Close()

	payload := bytes.Repeat([]byte("a"), 1024)
	size := uint64(len(payload))

	for i := 0; i < b.N; i++ {
		s.Write(payload)
	}
	s.Sync()

	switch behavior {
	case sink.Flush:
		if got, want := inner.totalBytes, size*uint64(b.N); got != want {
			b.Fatalf("got %v, want %v", got, want)
		}
	case sink.Discard:
		if got, want := inner.totalBytes, size*(uint64(b.N)-s.NumOverflows()); got != want {
			b.Fatalf("got %v, want %v [b.N=%v, overflows=%v]", got, want, b.N, s.NumOverflows())
		}
	}
}
