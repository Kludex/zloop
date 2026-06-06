---
icon: simple/python
---

# With asyncio

zloop is an `asyncio.AbstractEventLoop`. Not "asyncio-like" — the real thing. So
the entire `asyncio` standard-library surface works on top of it.

Let's go through the pieces you reach for most often, so you can see there are no
surprises.

## Tasks and futures

```python
import asyncio

import zloop


async def work(n: int) -> int:
    await asyncio.sleep(0.01 * n)
    return n


async def main():
    # create_task uses zloop's loop, but returns a normal asyncio.Task
    task = asyncio.create_task(work(3))

    # futures, gather, wait_for — all standard
    results = await asyncio.gather(work(1), work(2), task)
    return results


print(asyncio.run(main(), loop_factory=zloop.new_event_loop))
#> [1, 2, 3]
```

!!! info "Why this works"
    zloop reuses CPython's own `asyncio.Future` and `asyncio.Task` — the same
    C-accelerated objects the default loop uses. zloop only replaces the
    *engine* (scheduling and I/O), not the coroutine machinery. More on that in
    [What zloop reuses](../architecture/reuse.md).

## Timers

```python
async def main():
    loop = asyncio.get_running_loop()

    loop.call_soon(print, "now")
    loop.call_later(0.1, print, "in 100ms")
    loop.call_at(loop.time() + 0.2, print, "at an absolute time")

    await asyncio.sleep(0.3)
```

Timers fire in `(deadline, insertion order)` — the ordering asyncio guarantees.
The heap that backs them lives in Zig.

## Streams

The high-level streams API works unchanged:

```python
async def main():
    # an echo server
    async def handle(reader, writer):
        data = await reader.read(100)
        writer.write(data)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 8888)

    # ... and a client talking to it
    reader, writer = await asyncio.open_connection("127.0.0.1", 8888)
    writer.write(b"hello")
    await writer.drain()
    print(await reader.read(100))  #> b'hello'

    writer.close()
    server.close()


asyncio.run(main(), loop_factory=zloop.new_event_loop)
```

`start_server`, `open_connection`, `StreamReader`, `StreamWriter` — they're built
on the loop's `create_server` / `create_connection`, which zloop implements with
its Zig transports.

## Running blocking code in threads

`run_in_executor` and `asyncio.to_thread` work, which means you can safely call
blocking code from async land:

```python
import time


async def main():
    loop = asyncio.get_running_loop()

    # the classic
    result = await loop.run_in_executor(None, time.sleep, 0.1)

    # the modern shortcut
    await asyncio.to_thread(time.sleep, 0.1)
```

!!! tip "This relies on a real detail"
    For executors to work, the loop must release the GIL while it waits for
    I/O — otherwise the worker threads could never run. zloop does exactly that
    around its blocking poll. (uvloop does too; it's easy to get wrong, so it's
    worth knowing it's handled.)

## Signals

`add_signal_handler` works on the main thread, just like asyncio's default loop:

```python
import signal


async def main():
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    loop.add_signal_handler(signal.SIGINT, stop.set)
    loop.add_signal_handler(signal.SIGTERM, stop.set)

    print("running — press Ctrl+C to stop")
    await stop.wait()
    print("shutting down gracefully ✨")


asyncio.run(main(), loop_factory=zloop.new_event_loop)
```

## TLS

`create_server(ssl=...)` and `create_connection(ssl=...)` work, so anything that
speaks TLS over asyncio (HTTPS servers, secure clients) works on zloop.

```python
import ssl


async def main():
    ctx = ssl.create_default_context()
    reader, writer = await asyncio.open_connection("example.com", 443, ssl=ctx)
    writer.write(b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n")
    await writer.drain()
    print((await reader.read(200)).split(b"\r\n")[0])  #> b'HTTP/1.0 200 OK' (or similar)
    writer.close()


asyncio.run(main(), loop_factory=zloop.new_event_loop)
```

!!! note
    zloop reuses asyncio's own TLS state machine (`asyncio.sslproto`) on top of
    its Zig transports — so TLS behaves identically to the default loop.

## What about the low-level loop methods?

Methods like `loop.add_reader`, `loop.remove_reader`, `loop.add_writer`,
`loop.create_future`, and `loop.create_task` are all implemented. The handful of
rarely-used `sock_*` coroutine helpers (`sock_recv`, `sock_sendall`, …) raise
`NotImplementedError`, matching the behavior of an unsupported method — see
[Compatibility](../reference/compatibility.md) for the full matrix.
