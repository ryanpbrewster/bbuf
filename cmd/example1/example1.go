package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/ryanpbrewster/bbuf/sink"
)

func main() {
	delay := flag.Duration("delay", 100*time.Millisecond, "how slow to make the flush")
	buffered := flag.Bool("buffered", false, "should we use a bbuf.Sink or not?")
	flag.Parse()

	var out io.Writer = &slowDevNull{delay: *delay}
	if *buffered {
		log.Println("creating a buffered out")
		s := sink.New(out, sink.WithCapacity(8096), sink.WithOverflow(sink.Flush))
		defer s.Close()
		out = s
	} else {
		log.Println("leaving output unbuffered")
	}

	payload := make([]byte, 1)

	t := time.NewTicker(1 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		out.Write(payload)
	}
}

type slowDevNull struct {
	delay time.Duration
}

func (s *slowDevNull) Write(p []byte) (n int, err error) {
	time.Sleep(s.delay)
	fmt.Printf("%d bytes\n", len(p))
	return len(p), nil
}
