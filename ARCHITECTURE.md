# zloop architecture

`zloop` is an [asyncio](https://docs.python.org/3/library/asyncio.html)-compatible
event loop whose engine is written in [Zig](https://ziglang.org). It is to asyncio
what [uvloop](https://github.com/MagicStack/uvloop) is: a drop-in
`asyncio.AbstractEventLoop` implementation that uvicorn (and any asyncio program)
can use unchanged. The difference is the engine - uvloop wraps libuv from Cython;
zloop is a hand-written kqueue/epoll reactor in Zig, bound to CPython through a
thin C-API adapter.

## Design goal

The loop *engine* lives entirely in Zig. CPython is used for the two things it
already does correctly and that would be reckless to reimplement: driving
coroutines (`asyncio.Future` / `asyncio.Task`) and the TLS state machine
(`asyncio.sslproto`). This mirrors uvloop exactly - uvloop also delegates
`Future`/`Task` to CPython's `_asyncio` and only reimplements the loop mechanics,
timers, I/O and transports.

## Layering (ports & adapters)

The code is layered so that each layer depends only on the one below it, and the
*domain* (the event loop) has no knowledge of CPython. CPython is an adapter at
the edge.

```
   +-------------------------------------------------------------+
   |  zloop/__init__.py        new_event_loop() factory          |   Python edge
   +-------------------------------------------------------------+
   |  src/python/*.zig         CPython C-API adapter             |   adapter
   |    - Loop object (implements AbstractEventLoop)             |
   |    - Handle / TimerHandle wrapping Python callables         |
   |    - Transport objects bridging to asyncio.Protocol         |
   |    - reuses asyncio.Future/Task + asyncio.sslproto          |
   +-------------------------------------------------------------+
   |  src/core/loop.zig        the event loop (run-once engine)  |   domain
   |  src/core/transport.zig   buffered socket I/O + flow ctrl   |
   |  src/core/server.zig      listening socket -> accept loop   |
   +-------------------------------------------------------------+
   |  src/core/reactor.zig     kqueue/epoll demultiplexer        |   platform
   |  src/core/timers.zig      monotonic min-heap of deadlines   |
   |  src/core/queue.zig       intrusive callback FIFO           |
   +-------------------------------------------------------------+
   |  kqueue (macOS/BSD)  /  epoll (Linux)                       |   OS
   +-------------------------------------------------------------+
```

### Platform layer

- **`reactor.zig`** - the *Reactor* pattern. Owns the kqueue/epoll fd. Exposes a
  backend-agnostic API: `register(fd, interest)`, `modify(fd, interest)`,
  `unregister(fd)`, `poll(timeout_ns) -> []Event`. `interest` is a bitset
  `{read, write}`. Knows nothing about Python or callbacks - it maps fds to a
  caller-supplied opaque token and reports readiness. Pure, deterministic,
  unit-testable with real socketpairs.
- **`timers.zig`** - a binary min-heap of `(deadline_ns, seq, token)`. Supports
  push, peek-min, pop, and lazy cancellation. `seq` breaks ties so timers with
  equal deadlines fire in insertion order (asyncio guarantee).

### Domain layer

- **`loop.zig`** - the event loop proper. Holds the reactor, the timer heap, and
  a FIFO of ready callbacks. `runOnce()` is the canonical asyncio step:
  1. compute `timeout` = time until nearest timer (0 if ready queue non-empty),
  2. `reactor.poll(timeout)`,
  3. translate readiness -> schedule the fd's read/write callback,
  4. move all due timers into the ready queue,
  5. drain the ready queue (snapshot length, run that many - new callbacks wait).
  Also owns the self-pipe used by `call_soon_threadsafe` to wake a blocked poll.
- **`transport.zig`** - a connected socket. Buffered writes with a high/low
  watermark for flow control, `pause_reading`/`resume_reading`, `write_eof`,
  graceful `close` (flush then shutdown) vs `abort`.
- **`server.zig`** - a listening socket registered for read; on readiness it
  `accept()`s and hands the new fd up to the adapter to build a transport +
  protocol.

### Adapter layer (`src/python/`)

The only layer that includes `Python.h`. It defines the CPython types and
translates between Python objects and Zig domain calls:

- **`module.zig`** - `PyInit__zloop`, module-level `new_event_loop`.
- **`loop_obj.zig`** - the `Loop` Python type. Each method (`call_soon`,
  `call_later`, `create_server`, `run_until_complete`, ...) validates Python
  args, then calls into `loop.zig`. Callbacks are stored as `Handle` objects.
- **`handle.zig`** - `Handle` / `TimerHandle` Python types wrapping
  `(callback, args, context)`; `_run()` invokes the callable under its
  contextvars context and routes exceptions to `call_exception_handler`.
- **`transport_obj.zig`** - the `_SelectorSocketTransport`-equivalent Python
  type exposing `write/writelines/close/abort/is_closing/pause_reading/`
  `resume_reading/get_extra_info/set_protocol/write_eof`. Bridges Zig transport
  events to `protocol.data_received/connection_made/connection_lost/`
  `pause_writing/resume_writing`.
- **`future.zig`** - `create_future()` returns `asyncio.Future`; `create_task()`
  returns `asyncio.Task`. We do not reimplement these.

## What is reused from CPython, and why

| Concern               | Owner   | Rationale                                            |
|-----------------------|---------|-----------------------------------------------------|
| fd readiness, timers, run loop, callback scheduling | **Zig** | this is "the event loop" |
| socket transports, flow control, accept loop        | **Zig** | hot-path I/O, the point of the project |
| `Future` / `Task` / coroutine stepping              | CPython | subtle, correct, C-accelerated; uvloop reuses it too |
| TLS (`SSLProtocol`)                                  | CPython | `asyncio.sslproto` is loop-agnostic; only needs `call_soon`/`call_later` and a transport |
| executors (`run_in_executor`)                        | CPython | `ThreadPoolExecutor` + `Future` wrapping |
| DNS (`getaddrinfo`)                                  | CPython | delegated to a thread via the executor |

## Testing strategy

Two independent test surfaces, both required to be green:

1. **zloop's own tests** (`tests/`) - high-level, behavioural, run with pytest,
   targeting 100% coverage of the Python edge and exercising the Zig engine
   through real sockets, timers, servers, TLS, signals and threads.
2. **uvicorn's suite** - uvicorn run with `loop="zloop:new_event_loop"` (and the
   suite itself driven on zloop) must pass unchanged.

The Zig core additionally has `test {}` blocks compiled with `zig build test` for
the pure data structures (reactor, timer heap, queue).
