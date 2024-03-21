package sink

import (
	"io"
	"sync"

	"rpb.dev/bbuf"
)

type Sink struct {
	// inner must only be accessed while holding innerLock
	innerLock sync.Mutex
	inner     io.Writer

	// buf, closed must only be accessed while holding bufLock
	bufLock sync.Mutex
	buf     *bbuf.Buffer
	closed  bool

	// pending is a buffered channel used to indicate that there may be data pending in the buffer
	// it is only ever read by s.bgloop()
	pending chan struct{}
	// closing is closed when s.Close() is invoked, and it represents that no more data will be be
	// added to the buffer. s.bgloop() uses this as a signal to stop
	closing chan struct{}
	// done is closed when s.bgloop() exits
	done chan struct{}
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
	// The worst case here is:
	// a) s.pending is empty
	// b) both buffer segments are populated
	// In this case we'll need to trigger two flushes.
	//   1. The first add does nothing
	//   2. the second add waits until the in-progress flush is complete
	//   3. the third padd waits until the next flush is complete
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
