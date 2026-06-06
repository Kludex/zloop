# zloop Re-review

Scope: verification of the previously reported fixes plus a fresh pass over CPython refcounting, transport/handle lifetime, ReleaseFast casts, GIL/threading edges, error paths, and asyncio fidelity.

I could not run the pytest suite because this environment's `python3` lacks `pytest` (`No module named pytest`). I did run targeted `python3` probes for handle/loop lifetime and closed-loop transport tracking while reviewing the source.

## Verification Verdicts

1. **PARTIAL: Handle owns its loop reference, so the cancel-after-loop-freed UAF is gone, but pending handles can now leak cycles.**
   `HandleObject.loop_obj` is now documented and stored as an owned reference (`src/python/handle.zig:23`, `src/python/handle.zig:59`) and released in dealloc (`src/python/handle.zig:154`). Timer cancellation uses that owned loop object (`src/python/handle.zig:162`, `src/python/handle.zig:183`, `src/python/handle.zig:185`), so the original raw borrowed-loop UAF is fixed.

   The new issue is that `Handle`/`TimerHandle` are not GC types: their specs only use `Py_TPFLAGS_DEFAULT` and have no traverse/clear slots (`src/python/handle.zig:202`, `src/python/handle.zig:218`, `src/python/handle.zig:208`, `src/python/handle.zig:226`). The engine owns pending ready/timer handle refs until dispatch/deinit (`src/python/loop_obj.zig:193`, `src/python/loop_obj.zig:220`, `src/core/loop.zig:138`, `src/core/loop.zig:140`), while each pending handle owns the loop. If user code drops both a loop and an unrun handle, the cycle is not collectable unless the handle is cancelled or run.

2. **PARTIAL: Loop-held transport lifetime works on the normal path, but closed-loop connection teardown leaks tracked transports.**
   The normal lifetime fix is present: Python stores active transports in `_transports` (`zloop/_io.py:169`, `zloop/_io.py:175`), Zig tracks after successful startup (`src/python/transport_obj.zig:157`, `src/python/transport_obj.zig:165`) and untracks after `connection_lost` (`src/python/transport_obj.zig:529`, `src/python/transport_obj.zig:533`). Transport GC also visits/clears its loop edge (`src/python/transport_obj.zig:586`, `src/python/transport_obj.zig:594`, `src/python/transport_obj.zig:599`, `src/python/transport_obj.zig:618`), so dropping an otherwise unreachable loop/transport cycle can close and collect it.

   The remaining leak is after `loop.close()`: `close()` only marks the engine closed (`src/python/loop_obj.zig:529`, `src/python/loop_obj.zig:534`, `src/core/loop.zig:356`). If a transport then closes, `scheduleConnectionLost()` calls `loop.call_soon` (`src/python/transport_obj.zig:500`, `src/python/transport_obj.zig:514`), which now raises on the closed loop; that error is cleared (`src/python/transport_obj.zig:515`) and `_untrack_transport()` is never reached because it only runs in `_deliver_connection_lost()` (`src/python/transport_obj.zig:518`, `src/python/transport_obj.zig:533`). The fd is closed, but the transport/protocol remain in `loop._transports` for as long as the loop object lives.

3. **FIXED: `get_protocol()` null-guard is present.**
   `get_protocol()` now returns `None` after `connection_lost` clears `st.protocol` (`src/python/transport_obj.zig:529`, `src/python/transport_obj.zig:742`, `src/python/transport_obj.zig:744`).

4. **FIXED: Closed-loop guards are present on the requested APIs.**
   `call_soon` checks `raiseIfClosed` directly (`src/python/loop_obj.zig:176`, `src/python/loop_obj.zig:178`), `call_soon_threadsafe` does the same (`src/python/loop_obj.zig:264`, `src/python/loop_obj.zig:266`), `add_reader`/`add_writer` share the guard in `ioRegister` (`src/python/loop_obj.zig:561`, `src/python/loop_obj.zig:563`), and `_make_transport` checks it (`src/python/loop_obj.zig:640`, `src/python/loop_obj.zig:642`). `call_later` and `call_at` route through `scheduleTimerNs`, which checks `raiseIfClosed` before creating/scheduling the timer handle (`src/python/loop_obj.zig:202`, `src/python/loop_obj.zig:209`, `src/python/loop_obj.zig:210`, `src/python/loop_obj.zig:241`, `src/python/loop_obj.zig:261`).

5. **FIXED: fd validation and float clamping are present.**
   fd narrowing now goes through `validFd()` for ints and `fileno()` results (`src/python/loop_obj.zig:611`, `src/python/loop_obj.zig:615`, `src/python/loop_obj.zig:623`, `src/python/loop_obj.zig:630`). Timer conversion clamps NaN/negative to zero and huge values to `maxInt(u64)` before `@intFromFloat` (`src/python/loop_obj.zig:258`, `src/python/loop_obj.zig:293`, `src/python/loop_obj.zig:301`).

6. **FIXED: epoll empty mask and timeout rounding are corrected.**
   `maskOf(.{})` now returns `0` instead of including `EPOLL.RDHUP` (`src/core/reactor.zig:189`, `src/core/reactor.zig:190`). Epoll timeout conversion rounds positive sub-millisecond waits up to 1ms and clamps to `i32` (`src/core/reactor.zig:219`, `src/core/reactor.zig:222`, `src/core/reactor.zig:224`, `src/core/reactor.zig:225`).

7. **FIXED: Empty `get_buffer()` output is guarded.**
   `deliverBuffered()` now checks `cap == 0` or null `view.buf`, releases the buffer, raises a protocol error, and avoids treating `read(fd, _, 0)` as EOF (`src/python/transport_obj.zig:253`, `src/python/transport_obj.zig:258`, `src/python/transport_obj.zig:262`, `src/python/transport_obj.zig:263`, `src/python/transport_obj.zig:265`).

8. **FIXED: Socket leaks on factory/setup errors are guarded.**
   `_connect_accepted()` owns the accepted socket until adoption and closes it on failure (`zloop/_io.py:419`, `zloop/_io.py:421`, `zloop/_io.py:435`, `zloop/_io.py:436`, `zloop/_io.py:438`). `create_connection()` does the same for connected sockets (`zloop/_io.py:483`, `zloop/_io.py:485`, `zloop/_io.py:500`, `zloop/_io.py:507`, `zloop/_io.py:509`, `zloop/_io.py:511`).

9. **FIXED: OOM token drops and wake-pipe init cleanup are addressed.**
   `Loop.init()` now has `errdefer` cleanup immediately after creating the wake pipe (`src/core/loop.zig:112`, `src/core/loop.zig:113`, `src/core/loop.zig:114`). `drainXthread()` leaves tokens queued if `toOwnedSlice` fails and drops token resources if ready-queue promotion fails (`src/core/loop.zig:174`, `src/core/loop.zig:176`, `src/core/loop.zig:183`, `src/core/loop.zig:186`). Timer promotion similarly drops the popped timer token on ready-queue OOM (`src/core/loop.zig:322`, `src/core/loop.zig:326`).

## New / Remaining Findings

### High: Pending handles form an uncollectable loop cycle

This is the remaining part of item 1. A pending handle owns the loop (`src/python/handle.zig:59`) and the loop engine owns the pending handle token/ref (`src/python/loop_obj.zig:193`, `src/python/loop_obj.zig:220`, `src/core/loop.zig:138`, `src/core/loop.zig:140`). Since `Handle` and `TimerHandle` are not GC-tracked and do not traverse `loop_obj` (`src/python/handle.zig:202`, `src/python/handle.zig:218`, `src/python/handle.zig:208`, `src/python/handle.zig:226`), dropping a loop with pending unrun callbacks/timers can leak the loop, engine, handles, callbacks, args, and contexts.

Concrete shape:

```python
loop = zloop.Loop()
h = loop.call_later(3600, lambda: None)
del loop, h
gc.collect()  # loop remains alive because engine -> handle -> loop is invisible to GC
```

The fix needs either GC participation for `Loop`/`Handle` and their C-held references, or a `Loop.close()`/dealloc strategy that breaks pending handle ownership cycles reliably.

### High: Failed `add_reader`/`add_writer` registration leaves stale engine callbacks

`Loop.addReader()` and `Loop.addWriter()` mutate `self.fds` before reactor registration/modify succeeds (`src/core/loop.zig:208`, `src/core/loop.zig:212`, `src/core/loop.zig:213`, `src/core/loop.zig:216`, `src/core/loop.zig:220`, `src/core/loop.zig:221`). On error, the Python wrapper decrefs the handle it believes was not adopted (`src/python/loop_obj.zig:582`, `src/python/loop_obj.zig:584`, `src/python/loop_obj.zig:585`), but the core map may still contain an `IoCallback` pointing at that handle.

Impact: a closed/bad-but-nonnegative fd can make `add_reader()` raise while leaving a dangling callback in `fds`. Later loop deinit or `remove_reader()` can dispose the same handle pointer again, causing double-decref/use-after-free risk. Registration should be transactional: only publish the new callback after reactor success, or roll back/dispose consistently on failure.

### Medium: Closing transports after `loop.close()` leaks `_transports` entries

This is the remaining part of item 2. After `loop.close()`, `Transport.close()`/`abort()` closes the fd but cannot schedule `_deliver_connection_lost()` because `call_soon` now rejects the closed loop (`src/python/loop_obj.zig:176`, `src/python/loop_obj.zig:178`, `src/python/transport_obj.zig:500`, `src/python/transport_obj.zig:514`, `src/python/transport_obj.zig:515`). Since untracking only happens in `_deliver_connection_lost()` (`src/python/transport_obj.zig:518`, `src/python/transport_obj.zig:533`), the transport remains strongly referenced by `loop._transports` (`zloop/_io.py:169`, `zloop/_io.py:175`).

Impact: long-lived closed loop objects retain closed transports, protocols, extras, sockets, and contexts. Either untrack synchronously when scheduling `connection_lost` fails due to a closed loop, or have `Loop.close()` drain/close/untrack active transports.

### Low: Closed-loop guard ordering differs from asyncio for invalid scheduling calls

For valid calls, closed-loop scheduling is rejected correctly. For invalid timer calls, `call_later()`/`call_at()` validate the callback before reaching `scheduleTimerNs()` and `raiseIfClosed()` (`src/python/loop_obj.zig:230`, `src/python/loop_obj.zig:235`, `src/python/loop_obj.zig:241`, `src/python/loop_obj.zig:244`, `src/python/loop_obj.zig:249`, `src/python/loop_obj.zig:261`). A closed loop therefore raises `TypeError: a callable is required` for `loop.call_later(1, 123)` instead of `RuntimeError: Event loop is closed`. This is lower severity, but it is an asyncio fidelity difference.
