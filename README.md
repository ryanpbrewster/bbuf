# What is this?

`bbuf` is an implementation of bi-partite buffers (as far as I know, the
original implementation is
[here](https://www.codeproject.com/Articles/3479/The-Bip-Buffer-The-Circular-Buffer-with-a-Twist)).

I discovered it via this article
https://ferrous-systems.com/blog/lock-free-ring-buffer/, which has a reference
implementation [here](https://github.com/jamesmunns/bbqueue/).

The main motivation for this project is "asynchronous" logging. I'm looking for
an implementation of `io.Writer` that:

1. never blocks under normal operations (flushes data in the background)

2. flushes data promptly

3. never splits a payload into multiple `Write` calls

This implementation uses a bi-partite buffer to avoid splitting payloads, and a
background goroutine to flush data eagerly when it becomes available.

# API

`sink.New` is safe to share across goroutines, it is internally synchronized.

The `Close()` method will block until all of the buffered data has been flushed,
free the underlying buffer, and any subsequent calls to `Write` will be passed
through directly to the underlying writer.

The caller may specify the desired "overflow" behavior. That controls what
happens if the underlying buffer cannot fit a payload, either because it is
larger than the buffer or because the background flusher is not keeping up. The
two currently supported options are `Flush` and `Discard`

- `Flush` means "immediately attempt to write the payload directly to the
underlying writer". This will probably block. This can be helpful if you want
backpressure.

- `Discard` means "drop the payload without writing it". This guarantees that
the `Write` method will never be blocked by a slow underlying writer, but means
that you may lose data.

Regardless of which overflow behavior you specify, you may monitor for how often
the sink is hitting overflows using the `NumOverflows()` method.