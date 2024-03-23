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

func (b *Buffer) Reserve(sz int) *Lease {
	if b.write < b.read {
		// We are inverted.
		// We can't invert to get extra space, so we either have the capacity or we don't.
		if b.write+sz < b.read {
			// We have the space!
			start, end := b.write, b.write+sz
			return &Lease{Bytes: b.buf[start:end], end: end}
		} else {
			return nil
		}
	} else {
		// We are not inverted
		// If we don't have enough space, we can try inverting to get extra.
		if b.write+sz < len(b.buf) {
			start, end := b.write, b.write+sz
			// We have the space!
			return &Lease{Bytes: b.buf[start:end], end: end}
		} else if sz < b.read {
			// We don't have space here, but we have enough at the start. Time to invert.
			start, end := 0, sz
			return &Lease{Bytes: b.buf[start:end], end: end}
		} else {
			// No space anywhere
			return nil
		}
	}
}

func (b *Buffer) Commit(l *Lease) error {
	if l.end < b.write {
		// We must have inverted, record the watermark so that the reader knows where to stop
		b.watermark = b.write
	}
	b.write = l.end
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
	if r.end == b.write {
		b.read, b.write = 0, 0
	} else if r.end == b.watermark {
		b.read = 0
	} else {
		b.read = r.end
	}
	return nil
}
