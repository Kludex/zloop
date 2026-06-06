---
icon: lucide/check-check
---

# Compatibility

zloop aims to be a faithful `asyncio.AbstractEventLoop`. This page is the honest
map of what's supported.

## Supported

These work, and are tested (zloop runs uvicorn's full suite, plus its own at 100%
coverage):

| Area | Methods |
| --- | --- |
| **Scheduling** | `call_soon` · `call_later` · `call_at` · `call_soon_threadsafe` · `time` |
| **Lifecycle** | `run_until_complete` · `run_forever` · `stop` · `is_running` · `is_closed` · `close` |
| **Futures & tasks** | `create_future` · `create_task` · `set_task_factory` · `get_task_factory` |
| **I/O readiness** | `add_reader` · `remove_reader` · `add_writer` · `remove_writer` |
| **Servers & connections** | `create_server` · `create_unix_server` · `create_connection` |
| **Name resolution** | `getaddrinfo` · `getnameinfo` |
| **Executors** | `run_in_executor` · `set_default_executor` · `shutdown_default_executor` |
| **Signals** | `add_signal_handler` · `remove_signal_handler` (main thread) |
| **Errors & debug** | `call_exception_handler` · `set_exception_handler` · `get_exception_handler` · `default_exception_handler` · `set_debug` · `get_debug` |
| **Shutdown** | `shutdown_asyncgens` · `shutdown_default_executor` |
| **TLS** | `create_server(ssl=...)` · `create_connection(ssl=...)` |

The full asyncio **Transport** interface is implemented too - see
[Transports](../architecture/transports.md).

## Partially supported

* **`create_connection`** honors `local_addr`. Some less-common parameters
  (`happy_eyeballs_delay`, `interleave`, `all_errors`) are accepted but not yet
  acted on.

## Not implemented

* The low-level **`sock_*` coroutine helpers** (`sock_recv`, `sock_sendall`,
  `sock_connect`, `sock_accept`, …) raise `NotImplementedError`. Most programs use
  the higher-level streams/transports instead.
* **Datagram / UDP** (`create_datagram_endpoint`) and **pipes / subprocess
  transports** are not implemented yet.
* **Windows** - zloop is kqueue/epoll only. (`IOCP` is a different I/O model.)

## Platforms & Python versions

| | |
| --- | --- |
| **Python** | CPython 3.10+ |
| **macOS / BSD** | ✅ via `kqueue` |
| **Linux** | ✅ via `epoll` |
| **Windows** | ❌ not supported |

!!! note "A word on honesty"
    This page lists what *isn't* done as plainly as what is. zloop's goal is to be
    a trustworthy drop-in for the common case (ASGI servers, HTTP clients,
    structured concurrency) - not to claim 100% of asyncio's surface on day one.
    If you hit a gap that matters to you,
    [open an issue](https://github.com/Kludex/zloop).
