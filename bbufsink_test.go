package bbufsink_test

import (
	"testing"

	"rpb.dev/bbufsink"
)

func Test_Smoke(t *testing.T) {
	if got, want := bbufsink.Hello(), "hello"; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}
