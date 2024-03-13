package sink_test

import (
	"testing"

	"rpb.dev/bbufsink/sink"
)

func Test_Smoke(t *testing.T) {
	s := sink.New()
	if s == nil {
		t.Fatalf("sink.New: got nil")
	}
}
