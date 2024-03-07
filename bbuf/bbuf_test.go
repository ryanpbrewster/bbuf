package bbuf_test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
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
	if got, want := r.Bytes, "abcd"; !bytes.Equal(got, []byte(want)) {
		t.Fatalf("got %v, want %v", got, want)
	}

	if err := b.Release(r); err != nil {
		t.Fatalf("b.Release: %v", err)
	}
}

func Test_InterleavedReadsAndWrites(t *testing.T) {
	b := bbuf.New(10)

	// Write 4 bytes
	w1, err := b.Reserve(4)
	if err != nil {
		t.Fatalf("b.Reserve: %v", err)
	}
	copy(w1, []byte("aaaa"))
	if err := b.Commit(4); err != nil {
		t.Fatalf("b.Commit: %v", err)
	}

	// Read 4 bytes, but don't release it yet
	r1 := b.Read()

	w2, err := b.Reserve(4)
	if err != nil {
		t.Fatalf("b.Reserve: %v", err)
	}
	copy(w2, []byte("bbbb"))
	if err := b.Commit(4); err != nil {
		t.Fatalf("b.Commit: %v", err)
	}

	// Now check r1 after we're written new data. It should still be valid.
	if got, want := r1.Bytes, []byte("aaaa"); !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if err := b.Release(r1); err != nil {
		t.Fatalf("b.Release: %v", err)
	}

	// And another read should see "bbbb"
	r2 := b.Read()
	if got, want := r2.Bytes, []byte("bbbb"); !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if err := b.Release(r2); err != nil {
		t.Fatalf("b.Release: %v", err)
	}
}

func Test_Wraparound(t *testing.T) {
	b := bbuf.New(10)

	// Write & release 5 bytes
	w1, err := b.Reserve(5)
	if err != nil {
		t.Fatalf("b.Reserve: %v", err)
	}
	copy(w1, []byte("aaaaa"))
	if err := b.Commit(4); err != nil {
		t.Fatalf("b.Commit: %v", err)
	}
	r1 := b.Read()
	if got, want := r1.Bytes, []byte("aaaaa"); !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if err := b.Release(r1); err != nil {
		t.Fatalf("b.Release: %v", err)
	}

	// Now write 4 bytes, twice. That should wrap us around the end of the buffer.
	w2, err := b.Reserve(4)
	if err != nil {
		t.Fatalf("b.Reserve: %v", err)
	}
	copy(w2, []byte("bbbb"))
	if err := b.Commit(4); err != nil {
		t.Fatalf("b.Commit: %v", err)
	}
	w3, err := b.Reserve(4)
	if err != nil {
		t.Fatalf("b.Reserve: %v", err)
	}
	copy(w3, []byte("cccc"))
	if err := b.Commit(4); err != nil {
		t.Fatalf("b.Commit: %v", err)
	}

	// Because it wrapped around, the reads will necessarily be split.
	r2 := b.Read()
	if got, want := r2.Bytes, []byte("bbbb"); !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if err := b.Release(r2); err != nil {
		t.Fatalf("b.Release: %v", err)
	}

	r3 := b.Read()
	if got, want := r3.Bytes, []byte("cccc"); !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if err := b.Release(r3); err != nil {
		t.Fatalf("b.Release: %v", err)
	}
}

func Test_OutOfSpace_EdgeCases(t *testing.T) {
	b := bbuf.New(10)

	// We don't allow completely filling the buffer
	if _, err := b.Reserve(10); err == nil {
		t.Fatalf("b.Reserve: expected err")
	}

	// 9/10 is allowed
	w1, err := b.Reserve(9)
	if err != nil {
		t.Fatalf("b.Reserve: %v", err)
	}
	payload1 := bytes.Repeat([]byte("a"), len(w1))
	copy(w1, payload1)
	if err := b.Commit(len(w1)); err != nil {
		t.Fatalf("b.Commit: %v", err)
	}
	r1 := b.Read()
	if got, want := r1.Bytes, payload1; !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	b.Release(r1)

	// But now the buffer is "split" and you can't write 9/10 again
	if _, err := b.Reserve(9); err == nil {
		t.Fatalf("b.Reserve: expected err")
	}

	// 8/10 is allowed
	if _, err := b.Reserve(8); err != nil {
		t.Fatalf("b.Reserve: %v", err)
	}
}

func Test_Write1kEntries(t *testing.T) {
	b := bbuf.New(97)
	actual := new(bytes.Buffer)
	expected := new(bytes.Buffer)

	drain := func() {
		for {
			r := b.Read()
			if r == nil {
				return
			}
			actual.Write(r.Bytes)
			b.Release(r)
		}
	}

	prng := rand.NewSource(42)
	for i := 0; i < 1_000; i++ {
		payload := []byte(fmt.Sprintf("payload-%d\n", prng.Int63()))
		expected.Write(payload)

		w, err := b.Reserve(len(payload))
		if errors.Is(err, bbuf.ErrNotEnoughSpace) {
			drain()
			w, err = b.Reserve(len(payload))
		}
		if err != nil {
			t.Fatalf("b.Reserve(%d): %v (%+v)", len(payload), err, b)
		}
		copy(w, payload)
		if err := b.Commit(len(payload)); err != nil {
			t.Fatalf("b.Commit: %v", err)
		}
	}
	drain()

	if got, want := actual.String(), expected.String(); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func Test_WritesDontClobberReads(t *testing.T) {
	b := bbuf.New(10)

	payload := bytes.Repeat([]byte("a"), 7)
	w, err := b.Reserve(len(payload))
	if err != nil {
		t.Fatalf("b.Reserve: %v", err)
	}
	copy(w, payload)
	b.Commit(len(payload))

	r := b.Read()

	// We can't reserve this much space yet, it would need to invert & would clobber our live read
	if _, err := b.Reserve(5); err == nil {
		t.Fatalf("should not be able to reserve 5 bytes yet: %+v", b)
	}

	// For example, we might want to use our live read like this:
	if got, want := r.Bytes, payload; !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	b.Release(r)

	// _Now_ we can reserve 5 bytes
	if _, err := b.Reserve(5); err != nil {
		t.Fatalf("b.Reserve: %v", err)
	}
}
