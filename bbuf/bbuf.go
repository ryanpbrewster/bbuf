package bbuf

import "fmt"

type Buffer struct {
	// the actual data
	buf []byte
	// where the next write will start
	w int
	// where the next read will start
	r int
}

func New(sz int) *Buffer {
	return &Buffer{
		buf: make([]byte, sz),
		w:   0,
		r:   0,
	}
}

func (b *Buffer) Reserve(sz int) ([]byte, error) {
	return nil, fmt.Errorf("Reserve unimplemented")
}

func (b *Buffer) Commit(sz int) error {
	return fmt.Errorf("Commit unimplemented")
}

func (b *Buffer) Read() ([]byte, error) {
	return nil, fmt.Errorf("Read unimplemented")
}

func (b *Buffer) Release(sz int) error {
	return fmt.Errorf("Release unimplemented")
}
