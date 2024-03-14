package sink

import (
	"io"
	"log"
	"sync"
	"sync/atomic"

	"rpb.dev/bbufsink/bbuf"
)

type Sink struct {
	// inner must only be accessed while holding innerLock
	innerLock sync.Mutex
	inner     io.Writer

	// buf, closed must only be accessed while holding bufLock
	bufLock sync.Mutex
	buf     *bbuf.Buffer
	closed  bool

	// done represents whether the bgloop is exited
	done chan struct{}

	// dirtyWrite indicates that a write has happened (used to eagerly trigger reads)
	// It can only be closed while holding bufLock (to ensure it is only closed once)
	dirtyWrite chan struct{}

	// flushAttempts records how many times we've attempted to flush the buffer
	flushAttempts conflatedInt64
}

type config struct {
	capacity int
}
type Option func(*config)

func WithCapacity(capacity int) Option {
	return func(c *config) {
		c.capacity = capacity
	}
}

func New(inner io.Writer, opts ...Option) *Sink {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	s := &Sink{
		inner:      inner,
		buf:        bbuf.New(cfg.capacity),
		dirtyWrite: make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
	go s.bgloop()
	return s
}

func defaultConfig() *config {
	return &config{
		capacity: 256 << 10,
	}
}

func (s *Sink) bgloop() {
	defer close(s.done)
	for range s.dirtyWrite {
		s.flush()
	}
	// By the time we exit here, the sink is closed. Once we acquire s.bufLock, we are guaranteed
	// that it will never have any more data added to it.
	if s.flush() {
		s.flush()
	}
	s.buf = nil
}

// Flush is only ever invoked by the `bgloop`, it has concurrency <= 1
func (s *Sink) flush() bool {
	s.bufLock.Lock()
	l := s.buf.Read()
	s.bufLock.Unlock()
	if l == nil {
		return false
	}
	s.writeInner(l.Bytes)
	s.buf.Release(l)
	return true
}

func (s *Sink) Close() {
	s.bufLock.Lock()
	defer s.bufLock.Unlock()
	if !s.closed {
		s.closed = true
		close(s.dirtyWrite)
	}
}

func (s *Sink) Sync() error {
	// TODO: does this have to be 3?
	for i := 0; i < 3; i++ {
		// TODO: is it possible to avoid panicking if we call Close?
		select {
		case s.dirtyWrite <- struct{}{}:
		case <-s.done:
			return nil
		}
	}
	return nil
}

func (s *Sink) Write(p []byte) (int, error) {
	s.bufLock.Lock()
	var l *bbuf.Lease
	if !s.closed {
		l = s.buf.Reserve(len(p))
	} else {
		log.Println("s.closed, skipping buffer")
	}
	if l != nil {
		copy(l.Bytes, p)
		s.buf.Commit(l)
		select {
		case s.dirtyWrite <- struct{}{}:
		default:
		}
		s.bufLock.Unlock()
		return len(p), nil
	}

	// If we get here, there isn't room in the buffer for the payload, so we're going to write it directly.
	s.bufLock.Unlock()
	return s.writeInner(p)
}

func (s *Sink) writeInner(p []byte) (int, error) {
	s.innerLock.Lock()
	defer s.innerLock.Unlock()
	return s.inner.Write(p)
}

type conflatedInt64 struct {
	state atomic.Pointer[conflatedInt64State]
}

type conflatedInt64State struct {
	cur   int64
	dirty chan struct{}
}
