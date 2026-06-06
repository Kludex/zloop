# zloop

A drop-in [asyncio](https://docs.python.org/3/library/asyncio.html) event loop
whose engine is written in [Zig](https://ziglang.org). It's to asyncio what
[uvloop](https://github.com/MagicStack/uvloop) is - a real
`asyncio.AbstractEventLoop` - except the engine is a hand-written kqueue/epoll
reactor in Zig rather than libuv wrapped in Cython.

```python
import asyncio
import zloop

print(asyncio.run(asyncio.sleep(0, "hello from a Zig loop"), loop_factory=zloop.new_event_loop))
```

With [uvicorn](https://www.uvicorn.org):

```bash
uvicorn app:app --loop zloop:new_event_loop
```

## Why

- **Drop-in.** A genuine `AbstractEventLoop`, so the asyncio ecosystem -
  uvicorn, FastAPI, AnyIO, HTTPX - runs on it unchanged.
- **Correct.** Passes [uvicorn](https://github.com/encode/uvicorn)'s **entire**
  test suite (1048 tests), identical to stock asyncio, plus its own suite at
  **100%** coverage.
- **Fast.** Faster than uvloop on the workloads measured so far - scheduling,
  timers, and small/medium-message socket throughput (e.g. `call_soon` +46%,
  1 KiB echo +16% on CPython 3.14 / macOS arm64). `create_future` ties, because
  all three loops reuse CPython's C-accelerated `_asyncio.Future`.

## How it works

The loop *engine* lives in Zig; CPython is reused only where reimplementing
would be reckless: driving coroutines (`asyncio.Future` / `asyncio.Task`) and
the TLS state machine (`asyncio.sslproto`). That's exactly uvloop's boundary.

```
zloop/            Python edge - new_event_loop() factory, connection setup
src/python/*.zig  CPython C-API adapter - Loop, Handle, Transport
src/core/*.zig    pure-Zig domain - run-once engine, kqueue/epoll reactor, timer heap
```

## Develop

```bash
scripts/install   # venv + deps, then build the Zig extension
scripts/check     # ruff, mypy, zig fmt
scripts/test      # pytest under coverage
scripts/coverage  # 100% gate
```

CI runs these on macOS (kqueue) and Linux (epoll) across CPython 3.10-3.14.

## Docs

Full usage and architecture docs (with diagrams) build with
[Zensical](https://zensical.org): `scripts/build`, or `zensical serve` for a
live preview. See [ARCHITECTURE.md](ARCHITECTURE.md) for the design in one file.

## Platforms

macOS / BSD (kqueue) and Linux (epoll). Requires CPython 3.10+. MIT licensed.
