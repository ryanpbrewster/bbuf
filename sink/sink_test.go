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

type testSink struct {
	lock     sync.Mutex
	payloads [][]byte
}

func (t *testSink) Write(p []byte) (int, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	clone := make([]byte, len(p))
	copy(clone, p)
	t.payloads = append(t.payloads, clone)
	return len(p), nil
}
