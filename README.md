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

## Benchmarks

The fairest comparison is uvloop's *own* echo benchmark, run unchanged except
for a `--zloop` server flag mirroring `--uvloop` (the client is byte-for-byte
uvloop's). Requests/sec, higher is better - macOS arm64, CPython 3.14, 3
workers, best of 3:

| Message | Server mode | asyncio | uvloop | zloop | zloop vs uvloop |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 KiB | proto | 113k | 113k | **121k** | **+7%** |
| 1 KiB | buffered | 115k | 115k | **123k** | **+7%** |
| 1 KiB | streams | 83k | 90k | **103k** | **+14%** |
| 10 KiB | proto | 105k | 110k | **113k** | **+3%** |
| 10 KiB | buffered | 105k | 105k | **124k** | **+18%** |
| 10 KiB | streams | 81k | 86k | **95k** | **+11%** |

For the 1-10 KiB messages common in HTTP, WebSocket frames, and RPC, zloop leads
uvloop in every cell. The 100 KiB row is omitted: at that size the test measures
loopback bandwidth, not the loop, and all three swing wildly run-to-run.

Reproduce it with `scripts/bench` (or `bash bench_uvloop/run_matrix.sh` for the
full matrix); the **Benchmark** CI workflow runs it on Linux and posts the table
to the run summary.

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
[Zensical](https://zensical.org): `scripts/docs` renders the site, or
`zensical serve` for a live preview. See [ARCHITECTURE.md](ARCHITECTURE.md) for
the design in one file.

[zloop.marcelotryle.com](https://zloop.marcelotryle.com) is published to
Cloudflare on every push to `main` (`.github/workflows/docs.yml`); pull requests
get a versioned preview. Both need the `CLOUDFLARE_API_TOKEN` and
`CLOUDFLARE_ACCOUNT_ID` repository secrets.

## Platforms

macOS / BSD (kqueue) and Linux (epoll). Requires CPython 3.12+. MIT licensed.
