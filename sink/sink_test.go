package sink_test

import (
	"reflect"
	"sync"
	"testing"

	"rpb.dev/bbufsink/sink"
)

func Test_Smoke(t *testing.T) {
	inner := &testSink{}
	s := sink.New(inner)

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
