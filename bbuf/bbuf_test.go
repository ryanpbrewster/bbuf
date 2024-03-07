package bbuf_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"rpb.dev/bbufsink/bbuf"
)

func Test_ReadMyWrites(t *testing.T) {
	b := bbuf.New(10)

	w := b.Reserve(4)
	if w == nil {
		t.Fatal("b.Reserve returned nil")
	}
	copy(w.Bytes, []byte("abcd"))

	if err := b.Commit(w); err != nil {
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
	w1 := b.Reserve(4)
	if w1 == nil {
		t.Fatal("b.Reserve returned nil")
	}
	copy(w1.Bytes, []byte("aaaa"))
	if err := b.Commit(w1); err != nil {
		t.Fatalf("b.Commit: %v", err)
	}

	// Read 4 bytes, but don't release it yet
	r1 := b.Read()
	if r1 == nil {
		t.Fatal("b.Read returned nil")
	}

	w2 := b.Reserve(4)
	if w2 == nil {
		t.Fatal("b.Reserve returned nil")
	}
	copy(w2.Bytes, []byte("bbbb"))
	if err := b.Commit(w2); err != nil {
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
	w1 := b.Reserve(5)
	if w1 == nil {
		t.Fatalf("b.Reserve returned nil")
	}
	copy(w1.Bytes, []byte("aaaaa"))
	if err := b.Commit(w1); err != nil {
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
	w2 := b.Reserve(4)
	if w2 == nil {
		t.Fatalf("b.Reserve returned nil")
	}
	copy(w2.Bytes, []byte("bbbb"))
	if err := b.Commit(w2); err != nil {
		t.Fatalf("b.Commit: %v", err)
	}
	w3 := b.Reserve(4)
	if w3 == nil {
		t.Fatalf("b.Reserve returned nil")
	}
	copy(w3.Bytes, []byte("cccc"))
	if err := b.Commit(w3); err != nil {
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
	if w := b.Reserve(10); w != nil {
		t.Fatalf("b.Reserve: expected nil, got %+v", w)
	}

	// 9/10 is allowed
	payload1 := bytes.Repeat([]byte("a"), 9)
	w1 := b.Reserve(len(payload1))
	if w1 == nil {
		t.Fatalf("b.Reserve returned nil")
	}
	copy(w1.Bytes, payload1)
	if err := b.Commit(w1); err != nil {
		t.Fatalf("b.Commit: %v", err)
	}
	r1 := b.Read()
	if got, want := r1.Bytes, payload1; !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	b.Release(r1)

	// But now the buffer is "split" and you can't write 9/10 again
	if w := b.Reserve(9); w != nil {
		t.Fatalf("b.Reserve: expected nil, got %+v", w)
	}

	// 8/10 is allowed
	if w := b.Reserve(8); w == nil {
		t.Fatalf("b.Reserve: returned nil")
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

		w := b.Reserve(len(payload))
		if w == nil {
			drain()
			w = b.Reserve(len(payload))
		}
		copy(w.Bytes, payload)
		if err := b.Commit(w); err != nil {
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
	if w := b.Reserve(len(payload)); w == nil {
		t.Fatalf("b.Reserve: returned nil")
	} else {
		copy(w.Bytes, payload)
		b.Commit(w)
	}

	r := b.Read()

	// We can't reserve this much space yet, it would need to invert & would clobber our live read
	if w := b.Reserve(5); w != nil {
		t.Fatalf("b.Reserve: got %v, want nil", w)
	}

	// For example, we might want to use our live read like this:
	if got, want := r.Bytes, payload; !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	b.Release(r)

	// _Now_ we can reserve 5 bytes
	if w := b.Reserve(5); w == nil {
		t.Fatalf("b.Reserve returned nil")
	}
}

func Test_ReadsDoNotSeeUncommittedWrites(t *testing.T) {
	b := bbuf.New(10)

	payload := bytes.Repeat([]byte("a"), 3)
	w := b.Reserve(len(payload))
	if w == nil {
		t.Fatalf("b.Reserve returned nil")
	}

	// The write hasn't even been performed yet, we don't want to see unitialized data
	if r := b.Read(); r != nil {
		t.Fatalf("b.Read should be nil, got %+v", r)
	}

	copy(w.Bytes, payload)
	b.Commit(w)

	// Now that the write has been committed we can read it.
	r := b.Read()
	if got, want := r.Bytes, payload; !bytes.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
