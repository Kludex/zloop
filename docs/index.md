---
icon: lucide/zap
---

# zloop

<p align="center">
    <img src="assets/logo.png" alt="zloop" width="320">
</p>

<p align="center">
    <em>An asyncio event loop with a Zig core! :zap:</em>
</p>

---

**Source Code**: <a href="https://github.com/Kludex/zloop" target="_blank">https://github.com/Kludex/zloop</a>

---

!!! warning "Experimental"
    zloop is experimental. The API and behaviour may change at any time, and it is not yet ready for production use.

zloop is an [asyncio](https://docs.python.org/3/library/asyncio.html) event loop whose
engine is written in [Zig](https://ziglang.org). It is to asyncio what
[uvloop](https://github.com/MagicStack/uvloop) is: a **drop-in replacement** for the
default event loop that you can use without changing your application code.

The difference is what's underneath. uvloop wraps libuv from Cython; zloop is a
hand-written reactor in Zig - kqueue/epoll, plus an opt-in io_uring backend on
Linux - bound to CPython through a thin adapter.

The key features are:

* **Drop-in**: it's a normal `asyncio.AbstractEventLoop`. For the common
  server/client workloads - TCP, TLS, Unix sockets, the streams and transport
  APIs - your code runs unchanged. (A few rarely-used loop APIs aren't
  implemented yet; see [Compatibility](reference/compatibility.md).)
* **Fast**: the hot paths - scheduling, timers, and socket I/O - run in Zig, not
  Python. See [Performance](reference/performance.md) for benchmarks against
  asyncio and uvloop.
* **Familiar**: works the same way uvloop does, so the tools you already know
  ([uvicorn](usage/servers.md), [AnyIO](usage/asyncio.md#with-anyio),
  [FastAPI](usage/servers.md#fastapi-starlette)) just pick it up.
* **Tested**: it passes [uvicorn](https://github.com/encode/uvicorn)'s **entire**
  test suite, plus its own suite at 100% coverage.

## Installation

```console
$ pip install zloop
```

!!! note "Requirements"
    zloop needs **CPython 3.12+** and runs on **macOS / BSD** (kqueue) and
    **Linux** (epoll).

## Example

Let's create the simplest possible thing: run a coroutine on zloop.

```python title="main.py" hl_lines="10"
import asyncio

import zloop


async def main():
    print("Hello from a Zig event loop 👋")


asyncio.run(main(), loop_factory=zloop.new_event_loop)  # (1)!
```

1.  This is the one line that matters. `loop_factory` tells `asyncio.run()`
    *which* loop to build - and `zloop.new_event_loop` builds a zloop one.

    Everything else is plain asyncio. That's the whole point.

Run it:

```console
$ python main.py

Hello from a Zig event loop 👋
```

That's it. The coroutine ran, but the loop driving it - the timers, the
callback scheduling, the I/O polling - was all Zig. 🎉

!!! note "Python 3.12+"
    These docs use `asyncio.run(..., loop_factory=...)`, which needs **Python
    3.12+**. On 3.10 / 3.11 the loop works the same - you just start it
    differently; [First steps](usage/first-steps.md) shows how.

## Where to go next

<div class="grid cards" markdown>

-   :material-rocket-launch: **[First steps](usage/first-steps.md)**

    ---

    The 30-second tour: how to actually plug zloop into your program.

-   :material-server-network: **[Servers & clients](usage/servers.md)**

    ---

    The most common reason to use zloop. One CLI flag.

-   :material-sitemap: **[Architecture](architecture/overview.md)**

    ---

    How a Zig reactor becomes a Python event loop, with diagrams.

-   :material-speedometer: **[Performance](reference/performance.md)**

    ---

    The benchmarks, the methodology, and the honest caveats.

</div>
