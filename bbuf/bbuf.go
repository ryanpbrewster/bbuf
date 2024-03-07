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

	// if inverted, this is the watermark of the written data
	// if not inverted, this is len(buf)
	watermark int
}

func New(sz int) *Buffer {
	return &Buffer{
		buf:       make([]byte, sz),
		write:     0,
		read:      0,
		watermark: sz,
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
		} else if sz < b.read {
			// We don't have space here, but we have enough at the start. Time to invert.
			fmt.Printf("[RPB] inverting, end=%d\n", b.write)
			b.watermark = b.write
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

type Lease struct {
	Bytes []byte
	end   int
}

func (b *Buffer) Read() *Lease {
	start, end := b.read, b.write
	if b.write < b.read {
		// We are inverted.
		if start < b.watermark {
			// If there is data available, we should read until b.watermark
			end = b.watermark
		} else {
			// If there's no data, skip to reading from the front of the buf
			start = 0
		}
	}
	if start == end {
		return nil
	}
	return &Lease{Bytes: b.buf[start:end], end: end}
}

func (b *Buffer) Release(r *Lease) error {
	b.read = r.end
	return nil
}
