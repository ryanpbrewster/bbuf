package bbuf

import (
	"fmt"
)

type Buffer struct {
	// the actual data
	buf []byte

	// where the next write will start
	write int

	// where the next read will start
	read int

	// if inverted, this is the end of the written data
	// if not inverted, this is len(buf)
	end int
}

func New(sz int) *Buffer {
	return &Buffer{
		buf:   make([]byte, sz),
		write: 0,
		read:  0,
		end:   sz,
	}
}

var ErrNotEnoughSpace = fmt.Errorf("not enough space")

func (b *Buffer) Reserve(sz int) ([]byte, error) {
	if b.write < b.read {
		// We are inverted.
		// We can't invert to get extra space, so we either have the capacity or we don't.
		if b.write+sz < b.read {
			// We have the space!
			start := b.write
			b.write += sz
			return b.buf[start:b.write], nil
		} else {
			return nil, ErrNotEnoughSpace
		}
	} else {
		// We are not inverted
		// If we don't have enough space, we can try inverting to get extra.
		if b.write+sz < len(b.buf) {
			start := b.write
			b.write += sz
			// We have the space!
			return b.buf[start:b.write], nil
		} else if sz <= b.read {
			// We don't have space here, but we have enough at the start. Time to invert.
			b.end = b.write
			b.write = sz
			return b.buf[0:b.write], nil
		} else {
			// No space anywhere
			return nil, ErrNotEnoughSpace
		}
	}
}

func (b *Buffer) Commit(sz int) error {
	// do nothing?
	return nil
}

func (b *Buffer) Read() []byte {
	start, end := b.read, b.write
	if b.write < b.read {
		// We are inverted.
		// We should read until b.end
		end = b.end
		b.read = 0
	} else {
		b.read = end
	}
	if start == end {
		return nil
	}
	return b.buf[start:end]
}

func (b *Buffer) Release(sz int) error {
	// do nothing?
	return nil
}
