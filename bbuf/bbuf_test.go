package bbuf_test

import (
	"bytes"
	"testing"

	"rpb.dev/bbufsink/bbuf"
)

func Test_ReadMyWrites(t *testing.T) {
	b := bbuf.New(10)

	w, err := b.Reserve(4)
	if err != nil {
		t.Fatalf("b.Reserve: %v", err)
	}
	copy(w, []byte("abcd"))

	if err := b.Commit(4); err != nil {
		t.Fatalf("b.Commit: %v", err)
	}

	r := b.Read()
	if got, want := r, "abcd"; !bytes.Equal(got, []byte(want)) {
		t.Fatalf("got %v, want %v", got, want)
	}

	if err := b.Release(4); err != nil {
		t.Fatalf("b.Release: %v", err)
	}
}
