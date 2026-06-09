# zloop

<p align="center">
  <img src="https://raw.githubusercontent.com/Kludex/zloop/main/docs/assets/logo.png" alt="zloop" height="190">
</p>

> [!WARNING]
> zloop is experimental. The API and behaviour may change at any time, and it is not yet ready for production use.

A drop-in [asyncio](https://docs.python.org/3/library/asyncio.html) event loop
whose engine is written in [Zig](https://ziglang.org). It's to asyncio what
[uvloop](https://github.com/MagicStack/uvloop) is - a real
`asyncio.AbstractEventLoop` - except the engine is hand-written in Zig (a
kqueue/epoll reactor, plus an opt-in io_uring backend on Linux) rather than
libuv wrapped in Cython.

```python
import asyncio
import zloop


async def main() -> None:
    await asyncio.sleep(0)
    print("hello from a Zig loop")


asyncio.run(main(), loop_factory=zloop.new_event_loop)
```

With [uvicorn](https://www.uvicorn.org):

```bash
uvicorn app:app --loop zloop:new_event_loop
```

## Why

- **Drop-in.** A genuine `AbstractEventLoop`, so the asyncio ecosystem -
  uvicorn, FastAPI, AnyIO, HTTPX2 - runs on it unchanged.
- **Correct.** Passes [uvicorn](https://github.com/encode/uvicorn)'s **entire**
  test suite (1048 tests), identical to stock asyncio, plus its own suite at
  **100%** coverage.
- **Fast.** The hot paths - scheduling, timers, and socket I/O - run in Zig, not
  Python. See [Performance](#performance) for benchmarks against asyncio and
  uvloop, including an opt-in io_uring backend for free-threaded CPython.

## Performance

The fairest comparison is uvloop's *own* echo benchmark, run unchanged except
for a `--zloop` server flag mirroring `--uvloop` (the client is byte-for-byte
uvloop's). Requests/sec, higher is better.

zloop has two backends: the default readiness reactor (epoll/kqueue) and an
opt-in io_uring **completion** backend (`ZLOOP_IO_URING=completion`, Linux only).
Both are shown below; the **fastest** in each row is bold.

### Single loop (the default, GIL on)

Linux, CPython 3.14, 3 workers, best of 3, requests/sec:

![Echo throughput: uvloop vs zloop across server modes and message sizes](https://raw.githubusercontent.com/Kludex/zloop/main/docs/assets/echo-bench.svg)

| mode    | size    | asyncio | uvloop  | zloop epoll | zloop io_uring |
| ------- | ------: | ------: | ------: | ----------: | -------------: |
| proto   | 1 KB    | 135,393 | 127,872 |     132,223 |    **138,778** |
| proto   | 100 KiB |  61,441 |  57,158 |  **66,188** |         22,280 |
| streams | 1 KB    | 105,250 | 116,043 | **122,236** |        116,720 |
| streams | 100 KiB |  43,312 |  40,582 |  **46,862** |         19,679 |

For a single GIL-bound loop the **default epoll backend beats uvloop**. The
io_uring completion backend is *slower* here - its submit/reap overhead and the
64 KiB buffer-ring copy aren't amortized when one serialized loop is the
bottleneck (it fragments 100 KiB messages badly). Completion's win is parallel
free-threaded loops, below.

### Free-threaded parallel loops (GIL off)

Under free-threaded CPython (3.14t), N independent loops on N threads stop being
serialized by the GIL - and the leaner completion path (batched submits,
multishot recv, ring writes, cached `data_received`) **beats uvloop at every
thread count**. 1 KB messages, 8 conns/thread, requests/sec:

![Free-threaded throughput: uvloop vs zloop epoll vs zloop io_uring across parallel loops](https://raw.githubusercontent.com/Kludex/zloop/main/docs/assets/ft-bench.svg)

| loops | uvloop    | zloop epoll | zloop io_uring |
| ----: | --------: | ----------: | -------------: |
|     1 |   174,185 |     163,900 |    **209,071** |
|     4 |   606,261 |     535,556 |    **742,847** |
|     8 |   934,289 |     783,205 |  **1,142,655** |
|    16 | 1,148,609 |     815,971 |  **1,182,234** |

_(Linux io_uring kernel 6.10, 3-sample medians on a 12-CPU host; directional -
measured in a VM, not bare metal. The 16-loop margin is thin: 16 loops
oversubscribe 12 cores.)_

Reproduce the single-loop matrix with `scripts/bench`; the free-threaded numbers
come from `bench_uvloop/ft_parallel_bench.py` on a `python3.14t` build. Full
tables and caveats are in [the performance docs](https://zloop.marcelotryle.com/reference/performance/).

## How it works

The loop *engine* lives in Zig; CPython is reused only where reimplementing
would be reckless: driving coroutines (`asyncio.Future` / `asyncio.Task`) and
the TLS state machine (`asyncio.sslproto`). That's exactly uvloop's boundary.

```
zloop/            Python edge - new_event_loop() factory, connection setup
src/python/*.zig  CPython C-API adapter - Loop, Handle, Transport
src/core/*.zig    pure-Zig domain - run-once engine, kqueue/epoll reactor, timer heap
```
