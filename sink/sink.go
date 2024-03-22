package sink

import (
	"io"
	"sync"
	"sync/atomic"

	"rpb.dev/bbuf"
)

type Sink struct {
	cfg          config
	numOverflows atomic.Uint64

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
	behavior OverflowBehavior
}

// OverflowBehavior describes what should happen if
// a write happens and the bbuf.Buffer does not have
// any capacity available. The default behavios is Flush.
type OverflowBehavior string

const (
	// Flush means that if there isn't any capacity
	// the caller should directly grab the inner writer
	// and flush the payload
	Flush OverflowBehavior = "flush"
	// Discard means that if there isn't any capacity
	// the payload is discarded
	Discard OverflowBehavior = "discard"
)

type Option func(*config)

func WithCapacity(capacity int) Option {
	return func(c *config) {
		c.capacity = capacity
	}
}

func WithOverflow(behavior OverflowBehavior) Option {
	return func(c *config) {
		c.behavior = behavior
	}
}

func New(inner io.Writer, opts ...Option) *Sink {
	cfg := config{
		capacity: 256 << 10,
		behavior: Flush,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	s := &Sink{
		cfg: cfg,

		inner: inner,
		buf:   bbuf.New(cfg.capacity),

		pending: make(chan struct{}, 1),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}
	go s.bgloop()
	return s
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

	s.bufLock.Lock()
	s.buf.Release(l)
	s.bufLock.Unlock()

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
	//   3. the third add waits until the next flush is complete
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
	// If we get here, there isn't room in the buffer for the payload.
	s.numOverflows.Add(1)
	switch s.cfg.behavior {
	case Discard:
		// This is technically not a lie, it's just that the semantics of the
		// writer mean that we "wrote" n bytes by discarding them.
		// This is also required by the io.Writer contract.
		return len(p), nil
	case Flush:
	}
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

func (s *Sink) NumOverflows() uint64 {
	return s.numOverflows.Load()
}
