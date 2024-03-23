package bbuf

import (
	"fmt"
)

type Buffer struct {
	// the actual data
	buf []byte

	// where the next write will start
	write int
	// how many bytes are current out on reserve, potentially being written to (0 if no write is live)
	writing int

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
	if b.writing != 0 {
		return nil
	}
	if b.write < b.read {
		// We are inverted.
		// We can't invert to get extra space, so we either have the capacity or we don't.
		if b.write+sz < b.read {
			// We have the space!
			start, end := b.write, b.write+sz
			b.writing = sz
			return &Lease{Bytes: b.buf[start:end], end: end}
		} else {
			return nil
		}
	}
	// We are not inverted
	// If we don't have enough space, we can try inverting to get extra.
	if b.write+sz < len(b.buf) {
		start, end := b.write, b.write+sz
		// We have the space!
		b.writing = sz
		return &Lease{Bytes: b.buf[start:end], end: end}
	} else if sz < b.read {
		// We don't have space here, but we have enough at the start. Time to invert.
		start, end := 0, sz
		b.writing = sz
		return &Lease{Bytes: b.buf[start:end], end: end}
	} else {
		// No space anywhere
		return nil
	}
}

func (b *Buffer) Commit(l *Lease) {
	if b.writing == 0 {
		panic("commit without a write lease")
	}
	if l.end < b.write {
		// We must have inverted, record the watermark so that the reader knows where to stop
		b.watermark = b.write
	}
	b.write = l.end
	b.writing = 0
}
func (b *Buffer) Rollback(l *Lease) {
	if b.writing == 0 {
		panic("rollback without a write lease")
	}
	b.writing = 0
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

func (b *Buffer) Release(r *Lease) {
	if r.end == b.write && b.writing == 0 {
		// Optimization: if we have caught up to the writer, reset everything
		b.read, b.write = 0, 0
	} else if r.end == b.watermark {
		// Optimization: if we know that the writer has already inverted,
		// and that there's no data left for us to read on this end, we'll proactively
		// move the reader to the start
		b.read = 0
	} else {
		b.read = r.end
	}
}
