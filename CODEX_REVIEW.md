# zloop production-readiness/correctness review

Scope: CPython reference ownership in `src/python/*.zig`, ReleaseFast memory safety in the Zig core and transport, GIL/thread interactions, the unrun Linux/epoll path, failure cleanup, and asyncio API fidelity.

## Findings

### 1. Critical: `TimerHandle` keeps a borrowed loop pointer and can use-after-free it

`HandleObject.loop_obj` is explicitly borrowed (`src/python/handle.zig:23-25`) and `handle.create()` stores it without an incref (`src/python/handle.zig:58`). Timer cancellation later dereferences that raw pointer through `cancelTimerOnLoop()` (`src/python/handle.zig:156-164`, `src/python/handle.zig:181-184`). The loop can be deallocated independently of a user-held timer handle; `LoopObject.dealloc()` destroys the engine and frees the loop object (`src/python/loop_obj.zig:117-128`), while engine shutdown only drops the engine's scheduled handle ref.

Reproducer shape:

```python
loop = zloop.new_event_loop()
h = loop.call_later(3600, lambda: None)
loop.close()
del loop
h.cancel()  # dereferences freed loop_obj
```

Consequence: use-after-free in a CPython extension. `Handle`/`TimerHandle` should own a loop reference for as long as `cancel()` or exception reporting can touch the loop, or the loop must actively poison/clear all surviving handles before it can die.

### 2. Critical: failed `addReader`/`addWriter` leaves stale callbacks in the core and can double-decref freed Python handles

`Loop.addReader()` and `Loop.addWriter()` mutate `self.fds` before synchronizing the reactor (`src/core/loop.zig:204-217`). If `syncFdReg()` fails (`src/core/loop.zig:238-243`), the `FdState` still contains the new callback. The Python adapter assumes registration failed atomically and decrefs the new `Handle` immediately (`src/python/loop_obj.zig:556-560`).

That leaves `self.fds` holding an `IoCallback.ctx` pointer to a Python handle whose reference was already released. A later readiness dispatch or `Loop.deinit()` disposal (`src/core/loop.zig:141-145`) can call into a freed `HandleObject` and decref it again.

This is especially dangerous on Linux: `EpollReactor.register()` performs `epoll_ctl(ADD)` before `interest.put()` (`src/core/reactor.zig:196-200`). If the hash-map allocation fails after the kernel registration succeeds, the fd is live in epoll while the Python layer has already decref'd the handle.

Consequence: UAF/double-decref on OOM or reactor errors, plus kernel registration leaks. Registration needs transactional rollback in both `Loop` and reactor layers.

### 3. High: active transports are not owned by the loop; accepted connections can close immediately

`State.self_obj` is documented as a borrowed back-pointer (`src/python/transport_obj.zig:31`), and the reactor stores only `ctx = st` (`src/python/transport_obj.zig:293-295`, `src/python/transport_obj.zig:352-355`). The loop/core therefore do not own a Python reference to active transports.

On the server path, `_connect_accepted()` discards the return value of `_make_transport()` (`zloop/_io.py:395-410`). If a protocol's `connection_made()` does not store the transport, the newly returned transport loses its last Python reference immediately after `_make_transport()` returns. `t_dealloc()` then unregisters the fd and closes it (`src/python/transport_obj.zig:532-542`).

Consequence: valid asyncio protocols that do not retain `transport` themselves can have accepted connections closed right after `connection_made()`. Asyncio event loops keep transports alive until `connection_lost`; zloop needs equivalent ownership, e.g. a self-reference released in `_deliver_connection_lost()` or a loop-owned active transport table.

### 4. High: `Transport.get_protocol()` can crash after `connection_lost`

`_deliver_connection_lost()` clears `st.protocol` after invoking the protocol callback (`src/python/transport_obj.zig:486-498`). `get_protocol()` unconditionally calls `Py_NewRef(st.protocol)` (`src/python/transport_obj.zig:706-708`). If user code keeps the transport and calls `transport.get_protocol()` after close/abort/EOF delivery, this is `Py_NewRef(NULL)`.

Consequence: null dereference / process crash. Return `None` when `st.protocol == null`, or preserve asyncio's protocol reference semantics.

### 5. High: closed loops still accept scheduling and fd registration

`Loop.close()` only sets flags (`src/python/loop_obj.zig:504-510`, `src/core/loop.zig:350-352`). `call_soon()`, `call_soon_threadsafe()`, `call_at()`, `add_reader()`, `add_writer()`, and `_make_transport()` check only `self.engine != null`, not `self.closed` or `eng.closed` (`src/python/loop_obj.zig:184-191`, `src/python/loop_obj.zig:207-218`, `src/python/loop_obj.zig:268-275`, `src/python/loop_obj.zig:552-562`, `src/python/loop_obj.zig:610-611`).

Consequence: after `loop.close()`, user code can still enqueue Python handles, register fds, and create transports on a loop that `run_forever()` will refuse to run. This leaks callbacks/fds until object deallocation and violates asyncio's "Event loop is closed" behavior.

### 6. High: unchecked integer/float casts are unsafe under `ReleaseFast`

`fdFromObject()` converts arbitrary Python integers to `sys.fd_t` with `@intCast(v)` and no range or negativity checks (`src/python/loop_obj.zig:585-599`). `fdToken()` then casts `fd_t` to `usize` (`src/core/loop.zig:255-256`). With safety checks removed, huge or negative Python fd values can truncate/wrap before reaching libc/reactor code.

Timer conversion has the same issue: `secondsToNs()` and `call_later()` convert floats to `u64` without validating finite/range (`src/python/loop_obj.zig:248-251`, `src/python/loop_obj.zig:283-286`). `NaN`, `inf`, or very large deadlines are not rejected before `@intFromFloat`.

Consequence: invalid fd registration/removal, wrong tokens, traps in safe builds, and undefined/miscompiled behavior in ReleaseFast. Validate fd bounds and reject non-finite/out-of-range times like CPython asyncio does.

### 7. High: many C-API allocation failures are not checked before use/decref

Representative examples:

- `new()` stores `self.asyncgens = PySet_New(null)` without checking for null (`src/python/loop_obj.zig:90-94`), then may return a non-null object with a pending Python exception.
- `create_future()` creates `kwargs = PyDict_New()` and immediately defers `Py_DECREF(kwargs)` and writes into it without a null check (`src/python/loop_obj.zig:294-299`).
- `run_until_complete()` creates `ef_args` and `ef_kwargs` and uses/decrefs them without null checks (`src/python/loop_obj.zig:383-388`).
- Similar unchecked `PyDict_SetItemString()` calls ignore failures while potentially leaving exceptions set (`src/python/loop_obj.zig:298-299`, `src/python/loop_obj.zig:324-325`, `src/python/handle.zig:136-140`, `src/python/transport_obj.zig:513-518`).

Consequence: OOM can become `Py_DECREF(NULL)`, writes through null, or "returned a result with an exception set" `SystemError`. C extensions need strict null/return-code handling even for "unlikely" allocation paths.

### 8. Medium-high: core initialization leaks self-pipe fds on partial failure

`Loop.init()` creates the wake pipe (`src/core/loop.zig:112`) and then performs several fallible operations (`src/core/loop.zig:113-117`) without `errdefer` cleanup for `pipe[0]`/`pipe[1]`. `errdefer r.deinit()` only covers the reactor (`src/core/loop.zig:109-110`).

Consequence: failures in `setNonBlocking`, `setCloexec`, or `reactor.register()` leak one or both wake fds. Use `errdefer sys.close(pipe[0])` / `errdefer sys.close(pipe[1])` immediately after pipe creation.

### 9. Medium-high: `BufferedProtocol.get_buffer()` zero-length buffers are mishandled

`deliverBuffered()` obtains a writable `Py_buffer`, casts `view.buf`, and reads `cap = view.len` bytes (`src/python/transport_obj.zig:230-237`). It never validates that `cap > 0`. If a protocol returns a zero-length buffer, POSIX `read(fd, ..., 0)` returns 0, and zloop treats that as EOF (`src/python/transport_obj.zig:250-257`). Depending on the exporter, `view.buf` can also be null for an empty buffer, which is unsafe when unwrapped/cast.

Consequence: a protocol bug or edge case is converted into a false EOF or a null-pointer crash instead of being reported as the documented BufferedProtocol error.

### 10. Medium: cross-thread inbox can lose Python handle refs on OOM

`drainXthread()` moves tokens out of the cross-thread inbox and then pushes them into `ready`; if `ready.push()` fails, the catch block discards the error and the token (`src/core/loop.zig:172-183`). Timer promotion has a related failure mode: `popDue()` removes a timer before `ready.push()` (`src/core/loop.zig:318-321`), so an OOM during the push loses the engine-owned token.

Consequence: leaked Python `Handle` refs and callbacks that never run under memory pressure. The engine must either keep ownership until enqueue succeeds or call `dispatcher.drop()` for tokens it cannot enqueue.

### 11. Medium: connection setup leaks sockets on Python-level failures

On the client path, after a successful connect, `create_connection()` calls `protocol_factory()`, builds `extra`, and creates the transport without a cleanup guard (`zloop/_io.py:453-475`). If `protocol_factory()` raises, or `_make_transport()` fails before the transport owns/closes the socket, the connected socket leaks. The accepted-server path has the same shape around `protocol_factory()` / `_make_transport()` (`zloop/_io.py:395-410`).

Consequence: fd leaks on realistic application errors during protocol construction or `connection_made()`. Wrap socket ownership transfer in `try/except`/`finally` so the socket is closed until the transport has definitely adopted it.

### 12. Medium: Linux epoll backend has untested API mismatches and failure-state leaks

Besides the transactional failure in finding 2, the epoll backend is not behaviorally equivalent to kqueue:

- `modify(fd, token, .{})` leaves the fd registered for `EPOLL.RDHUP` because `maskOf(.{})` still returns `EPOLL.RDHUP` (`src/core/reactor.zig:189-193`, `src/core/reactor.zig:203-207`). The kqueue backend deletes both filters for empty interest (`src/core/reactor.zig:90-99`). The reactor tests call this path (`src/core/reactor.zig:286-288`), so Linux can still report hangup/error events for an fd whose interest is "empty".
- `poll()` floors nanosecond timeouts to milliseconds (`src/core/reactor.zig:218-221`), so sub-millisecond timer waits become zero-timeout polls. That can spin until the deadline actually passes.

Consequence: Linux can produce readiness for "empty" interest and can busy-poll short timers. These should be covered by Linux CI with reactor tests and fixed for backend parity.

### 13. Medium: asyncio API fidelity gaps likely break real programs

`create_connection()` accepts but silently ignores `local_addr`, `happy_eyeballs_delay`, `interleave`, and `all_errors` (`zloop/_io.py:412-430`). `create_server()` accepts but does not implement several server/TLS lifecycle details (`zloop/_io.py:279-296`). `_Server.close_clients()` and `abort_clients()` are stubs (`zloop/_io.py:123-127`). The loop also does not implement common socket coroutine APIs such as `sock_recv`, `sock_sendall`, and related methods, so inherited `AbstractEventLoop` methods will raise `NotImplementedError`.

Consequence: libraries that use less-minimal asyncio APIs may appear to work until they hit ignored parameters or unimplemented methods. For production readiness, either implement these or reject unsupported arguments loudly.

## Notes

The normal-path reference ownership around scheduled callbacks is mostly coherent: `call_soon()`/`call_at()` give one ref to the caller and one to the engine, dispatch drops the engine ref, and timer cancellation drops the engine ref exactly once. The dangerous areas are lifecycle edges: borrowed loop pointers in surviving handles, non-transactional I/O registration failures, active transport ownership, and unchecked C-API error paths.
