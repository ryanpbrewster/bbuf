package sink

import (
	"io"
	"sync"

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

	pending chan struct{}
	closing chan struct{}
	done    chan struct{}
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
		inner: inner,
		buf:   bbuf.New(cfg.capacity),

		pending: make(chan struct{}, 1),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
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
	defer func() {
		// By the time we exit here, the sink is closed. Once we acquire s.bufLock, we are guaranteed
		// that it will never have any more data added to it.
		if s.flush() {
			s.flush()
		}
		s.buf = nil
		close(s.done)
	}()

	for {
		select {
		case <-s.closing:
			return
		case <-s.pending:
			s.flush()
		}
	}
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
	if !s.closed {
		s.closed = true
		close(s.closing)
	}
	s.bufLock.Unlock()
	<-s.done
}

func (s *Sink) Sync() error {
	// TODO: does this have to be 3?
	for i := 0; i < 3; i++ {
		select {
		case s.pending <- struct{}{}:
		case <-s.done:
			return nil
		}
	}
	return nil
}

func (s *Sink) Write(p []byte) (int, error) {
	if s.tryBuffer(p) {
		return len(p), nil
	}
	// If we get here, there isn't room in the buffer for the payload, so we're going to write it directly.
	return s.writeInner(p)
}

func (s *Sink) tryBuffer(p []byte) bool {
	s.bufLock.Lock()
	defer s.bufLock.Unlock()
	if s.closed {
		return false
	}
	l := s.buf.Reserve(len(p))
	if l == nil {
		return false
	}
	copy(l.Bytes, p)
	s.buf.Commit(l)
	select {
	case s.pending <- struct{}{}:
	default:
	}
	return true
}

func (s *Sink) writeInner(p []byte) (int, error) {
	s.innerLock.Lock()
	defer s.innerLock.Unlock()
	return s.inner.Write(p)
}
