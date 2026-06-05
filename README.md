# zloop

An [asyncio](https://docs.python.org/3/library/asyncio.html)-compatible event
loop whose engine is written in [Zig](https://ziglang.org). It is to asyncio
what [uvloop](https://github.com/MagicStack/uvloop) is - a drop-in
`asyncio.AbstractEventLoop` implementation - except the engine is a hand-written
kqueue/epoll reactor in Zig rather than libuv wrapped in Cython.

```python
import asyncio
import zloop

async def main():
    await asyncio.sleep(0.1)
    return "hello from a Zig event loop"

print(asyncio.run(main(), loop_factory=zloop.new_event_loop))
```

With [uvicorn](https://www.uvicorn.org):

```bash
uvicorn app:app --loop zloop:new_event_loop
```

## Status

- Runs [uvicorn](https://github.com/encode/uvicorn)'s **entire test suite**
  (1048 passed, 14 platform-skips) - identical to running on stock asyncio.
- The Python edge has **100% test coverage** via high-level behavioural tests
  that exercise the Zig core end to end (real sockets, TLS, signals, threads,
  timers).
- HTTP/1.1, WebSockets, TLS, Unix sockets, flow control, signals, executors.

## How it works

The event loop *engine* lives in Zig; CPython is reused only for the two things
it already does correctly and that would be reckless to reimplement: driving
coroutines (`asyncio.Future` / `asyncio.Task`) and the TLS state machine
(`asyncio.sslproto`). This is exactly uvloop's boundary.

```
  zloop/__init__.py         new_event_loop() factory          Python edge
  zloop/_io.py              connection-setup choreography
  --------------------------------------------------------------------------
  src/python/*.zig          CPython C-API adapter             adapter
    Loop (AbstractEventLoop), Handle, Transport
    reuses asyncio.Future/Task + asyncio.sslproto
  --------------------------------------------------------------------------
  src/core/loop.zig         the event loop (run-once engine)  domain
  src/python/transport_obj  buffered socket I/O + flow control
  --------------------------------------------------------------------------
  src/core/reactor.zig      kqueue / epoll demultiplexer      platform
  src/core/timers.zig       monotonic timer min-heap
  src/core/sys.zig          libc syscall wrappers
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design.

## Building

```bash
./build_ext.sh                 # build against ./.venv (or pass a python path)
ZLOOP_BUILD_MODE=Debug ./build_ext.sh   # debug build with safety checks
zig build test                 # run the pure-Zig core unit tests
```

The build produces `zloop/_zloop<EXT_SUFFIX>.so`, importable as `zloop`.

## Testing

```bash
# zloop's own suite + 100% coverage gate
python -m coverage run --source=zloop -m pytest tests/
python -m coverage report --fail-under=100
```

## Platforms

macOS / BSD (kqueue) and Linux (epoll). Requires CPython 3.10+.
