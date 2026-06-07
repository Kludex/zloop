---
icon: simple/python
---

# AsyncIO & AnyIO

zloop is an `asyncio.AbstractEventLoop`. Not "asyncio-like" - the real thing. So
the asyncio APIs you reach for day to day - tasks, futures, timers, streams,
executors, signals, TLS - work on top of it. And because
[AnyIO](https://anyio.readthedocs.io) runs on asyncio, AnyIO programs (Starlette,
FastAPI, HTTPX2) run on zloop too - covered [further down](#with-anyio).

Let's walk through them, so you can see there are no surprises. (A handful of
lower-level loop APIs aren't implemented yet; the
[compatibility matrix](../reference/compatibility.md) is the full picture.)

!!! note "Reading the examples"
    The first example below is complete. The later snippets show just the
    interesting `async def main(): ...` body - run any of them by dropping it
    into the same skeleton:

    ```python
    import asyncio

    import zloop

    # async def main(): ...  <- the snippet goes here

    asyncio.run(main(), loop_factory=zloop.new_event_loop)
    ```

    `asyncio.Runner(loop_factory=...)` works too if you want to reuse one loop
    across several `run()` calls (see [First steps](first-steps.md)); the body is
    unchanged.

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

    # futures, gather, wait_for - all standard
    results = await asyncio.gather(work(1), work(2), task)
    return results


print(asyncio.run(main(), loop_factory=zloop.new_event_loop))
#> [1, 2, 3]
```

!!! info "Why this works"
    zloop reuses CPython's own `asyncio.Future` and `asyncio.Task` - the same
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

Timers fire in `(deadline, insertion order)` - the ordering asyncio guarantees.
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

`start_server`, `open_connection`, `StreamReader`, `StreamWriter` - they're built
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
    I/O - otherwise the worker threads could never run. zloop does exactly that
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

    print("running - press Ctrl+C to stop")
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
    status_line = (await reader.read(200)).split(b"\r\n")[0]
    print(status_line)  # the HTTP status line from the TLS connection
    writer.close()


asyncio.run(main(), loop_factory=zloop.new_event_loop)
```

!!! note
    zloop reuses asyncio's own TLS state machine (`asyncio.sslproto`) on top of
    its Zig transports, so the handshake and record framing behave like the
    default loop. A couple of TLS *timeout* knobs aren't wired through yet - see
    [Compatibility](../reference/compatibility.md).

## With AnyIO

[AnyIO](https://anyio.readthedocs.io) is the structured-concurrency layer that
sits on top of asyncio. It's what Starlette, FastAPI, and HTTPX2 use internally -
so making AnyIO run on zloop means a *lot* of the ecosystem runs on zloop.

The good news: AnyIO's asyncio backend accepts a **loop factory**, so this is a
clean one-liner.

```python title="main.py" hl_lines="17"
import anyio

import zloop


async def main():
    async with anyio.create_task_group() as tg:
        tg.start_soon(anyio.sleep, 0.1)
        tg.start_soon(anyio.sleep, 0.2)
    return "structured concurrency on a Zig loop ✨"


print(
    anyio.run(
        main,
        backend="asyncio",
        backend_options={"loop_factory": zloop.new_event_loop},  # (1)!
    )
)
```

1.  AnyIO's asyncio backend forwards `loop_factory` straight to an
    `asyncio.Runner`. Same mechanism as everywhere else.

### In tests (`pytest` + `anyio`)

If you test with the `anyio` pytest plugin, you select the backend with the
`anyio_backend` fixture. Return a tuple to pass options - including the loop
factory:

```python title="conftest.py"
import pytest

import zloop


@pytest.fixture
def anyio_backend():
    return ("asyncio", {"loop_factory": zloop.new_event_loop})  # (1)!
```

1.  Now every `@pytest.mark.anyio` test in your suite runs on zloop. This is
    exactly how we run uvicorn's own test suite against zloop.

```python title="test_something.py"
import asyncio

import pytest


@pytest.mark.anyio
async def test_runs_on_zloop():
    assert type(asyncio.get_running_loop()).__module__.startswith("zloop")
```

!!! tip
    This fixture trick is the easiest way to run an *existing* asyncio/AnyIO test
    suite on zloop and see what happens. Suites that stick to TCP/TLS sockets,
    streams, tasks, and timers should pass unchanged; if a suite reaches for
    UDP, subprocesses, pipes, or the `sock_*` helpers it'll hit zloop's
    [unimplemented APIs](../reference/compatibility.md). 🙂

## What about the low-level loop methods?

Methods like `loop.add_reader`, `loop.remove_reader`, `loop.add_writer`,
`loop.create_future`, and `loop.create_task` are all implemented. The handful of
rarely-used `sock_*` coroutine helpers (`sock_recv`, `sock_sendall`, …) raise
`NotImplementedError`, matching the behavior of an unsupported method - see
[Compatibility](../reference/compatibility.md) for the full matrix.
