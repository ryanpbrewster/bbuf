package main

import (
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/ryanpbrewster/bbuf/sink"
)

func main() {
	buffered := flag.Bool("buffered", false, "should we use a bbuf.Sink or not?")
	flag.Parse()

	var out io.Writer = &slowDevNull{}
	if *buffered {
		fmt.Println("creating a buffered out")
		s := sink.New(out, sink.WithOverflow(sink.Discard))
		defer s.Close()
		out = s
	}

	payload := make([]byte, 100)

	t := time.NewTicker(1 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		out.Write(payload)
	}
}

type slowDevNull struct{}

func (s *slowDevNull) Write(p []byte) (n int, err error) {
	time.Sleep(1_000 * time.Millisecond)
	fmt.Printf("%d bytes\n", len(p))
	return len(p), nil
}
