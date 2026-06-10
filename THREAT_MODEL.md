# Threat Model

This document describes zloop's security posture: what it is responsible for, the
trust boundaries it sits across, the threats it considers, and what it explicitly
leaves to its dependencies. zloop is an `asyncio.AbstractEventLoop` with a Zig
engine. It is plumbing, not policy - it moves bytes between sockets and Python
callbacks. Most application-level security (authentication, authorization,
request parsing, TLS certificate policy) lives above it.

> zloop is experimental and not yet ready for production use. This threat model
> describes intent and current design; it is not an audit or a guarantee.

## Scope

In scope:

- The Zig core (`src/core/*.zig`): the run-once engine, the kqueue/epoll reactor,
  the io_uring completion backend, timers, queues, and buffered socket transport.
- The CPython C-API adapter (`src/python/*.zig`): argument translation, reference
  counting, GIL handling, and exception routing across the Python/Zig boundary.
- The Python edge (`zloop/`): the loop factory, connection setup, signal
  handling, executor delegation, and TLS wiring.

Out of scope:

- CPython itself, including `asyncio.Future`/`asyncio.Task` stepping and the
  `asyncio.sslproto` TLS state machine, which zloop reuses verbatim.
- The OS kernel, its syscall surface (epoll/kqueue/io_uring, sockets), and its
  TLS, DNS, and crypto libraries.
- Application code: protocols, request parsers, authentication, and the policy a
  server enforces on the bytes zloop delivers.

## Assets

- **Process integrity.** zloop runs in-process with the host application. A
  memory-safety bug in the Zig core or the C-API adapter is a host-process
  compromise, not a sandboxed failure.
- **Connection data in flight.** Bytes buffered in transports between the socket
  and the protocol callback. zloop treats these as opaque; it does not parse,
  log, or persist them.
- **File descriptors.** Listening, accepted, and connected sockets, plus the
  signal self-pipe and executor machinery. Mishandling fd lifetime risks
  use-after-free, cross-connection data delivery, or descriptor leaks.

## Trust boundaries

### 1. Network <-> Zig core

Untrusted bytes arrive from `recv()` (readiness backend) or a kernel-filled
buffer ring (io_uring completion backend). The core's contract is deliberately
narrow: it delivers bytes as opaque `bytes` to `protocol.data_received()` and
never parses, frames, or interprets them. There is no hand-rolled string parsing
of network input in the core. On the completion path, kernel-provided buffers are
sliced into `bytes`, not copied through manual pointer arithmetic on
attacker-controlled lengths.

Threats considered: buffer overruns on receive (kernel reports the length; we
slice within it), unbounded write-buffer growth (bounded by flow-control
watermarks, below), and stale completions after fd reuse (below).

### 2. Python <-> Zig (the C-API adapter)

Every Python argument is validated and translated before the core touches it, and
every value handed back to Python is a correctly reference-counted object. The GIL
is released only around the blocking poll and reacquired before any Python object
is touched. Errors raised in user callbacks are caught and routed to
`loop.call_exception_handler()` rather than crossing the Zig boundary as
exceptions.

Threats considered: reference-count errors (use-after-free / leak of Python
objects), touching Python state without the GIL, and exceptions escaping into Zig
frames that cannot unwind them.

### 3. Application callables

Protocol factories, callbacks, signal handlers, and exception handlers are
arbitrary user code. zloop runs them under the appropriate `contextvars` context
and isolates their failures: a raising callback is reported, not fatal. zloop does
not attempt to sandbox this code - it is trusted to the same degree as the rest of
the host application.

### 4. File descriptor lifetime

Descriptors come from the kernel via `accept()`/`connect()` and are owned by
Python `socket` objects whose lifetime guards against double-close. On the
io_uring backend, completions carry a generation-tagged identity
(`[gen | kind | fd]` packed into `user_data`); a completion that arrives after an
fd has been closed and its number reused is detected by a stale generation and
dropped. This prevents delivering one connection's bytes to another - the class
of bug that motivated the per-op identity scheme.

### 5. Signals and address resolution

Signal handlers are dispatched through a self-pipe: the handler writes only the
signal number, and the real callback runs synchronously on the loop, respecting
Python's signal-safety rules. Name resolution is delegated to
`socket.getaddrinfo()` in an executor thread (the system resolver); zloop does not
implement its own DNS and inherits the resolver's trust properties.

## Threats and mitigations

| Threat | Surface | Mitigation |
| --- | --- | --- |
| Memory corruption from untrusted bytes | Zig core receive paths | No parsing in the core; bytes delivered opaque; kernel-reported lengths bound every slice. |
| Use-after-free on fd reuse | io_uring completion backend | Generation-tagged `user_data`; stale completions dropped. |
| Cross-connection data delivery | Completion backend buffer rings | Per-operation identity ties each completion to its originating fd and generation. |
| Unbounded memory growth | Transport write buffering | High/low watermark flow control; `pause_writing`/`resume_writing` backpressure. |
| Resource exhaustion on accept | Listening sockets | `EMFILE`/`ENFILE`/`ENOBUFS`/`ENOMEM` handled distinctly so an fd-table-full condition does not tear down the listener. |
| Reference-count errors | Python/Zig adapter | Refcounts managed explicitly at every boundary crossing; GIL held whenever Python objects are touched. |
| Exceptions crossing into Zig | C-API adapter | User-callback exceptions caught and routed to `call_exception_handler()`. |
| Interrupted syscalls | Core syscall wrappers | `EINTR` retried on `read`/`write`/`accept`. |
| Descriptor leaks | Socket/pipe setup | `O_NONBLOCK`/`FD_CLOEXEC` set on created descriptors. |

## Non-goals

zloop does not provide: TLS certificate validation policy (it wires CPython's SSL
into `asyncio.sslproto`; policy is the caller's), request/response parsing or
size limits at the protocol layer, DoS protection such as connection-rate
limiting or slow-loris defense, DNS resolution hardening beyond what the system
resolver provides, or any sandboxing of application callables. These belong to the
server and application built on top of zloop.

## Reporting

zloop is experimental and pre-production. If you find a memory-safety issue in the
Zig core or the C-API adapter, or any defect that lets one connection's data reach
another, please report it privately to the maintainer rather than opening a public
issue.
