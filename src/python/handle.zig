//! Handle / TimerHandle: Python objects wrapping a (callback, args, context)
//! triple, mirroring asyncio.Handle. The Zig loop stores a Handle's pointer as
//! its opaque token; dispatching the token runs the handle. asyncio and uvicorn
//! call handle.cancel(); TimerHandle additionally exposes when() and supports
//! ordering, which asyncio's heap relies on - though our timers live in Zig, we
//! keep the interface for compatibility.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;

// Free-threading guards (ft_critical.c). Each takes a critical section keyed on
// the Handle so dispatch reading (callback, args, context) cannot race with
// cancel clearing them on a no-GIL build. No-ops under the GIL.
extern fn zloop_handle_snapshot(op: py.Object, callback: *py.Object, args: *py.Object, context: *py.Object) c_int;
extern fn zloop_handle_mark_cancelled(op: py.Object) c_int;
extern fn zloop_handle_clear_refs(op: py.Object) void;

pub const HandleObject = extern struct {
    ob_base: c.PyObject,
    callback: py.Object,
    args: py.Object,
    context: py.Object,
    cancelled: u8,
    is_timer: u8,
    /// For TimerHandle: absolute deadline in seconds (loop.time units) and the
    /// Zig timer seq used to cancel the scheduled timer.
    when: f64,
    timer_seq: u64,
    /// Owned reference to the loop, so cancel() / exception reporting can touch
    /// it safely even if the user drops the loop while holding the handle
    /// (matches asyncio.Handle, which keeps self._loop).
    loop_obj: py.Object,
};

comptime {
    // ft_critical.c reinterprets a Handle through ZloopHandleHead, which mirrors
    // these first fields in order. Keep the two layouts in lockstep.
    const head = @offsetOf(HandleObject, "ob_base");
    std.debug.assert(head == 0);
    std.debug.assert(@offsetOf(HandleObject, "callback") == @sizeOf(c.PyObject));
    std.debug.assert(@offsetOf(HandleObject, "args") > @offsetOf(HandleObject, "callback"));
    std.debug.assert(@offsetOf(HandleObject, "context") > @offsetOf(HandleObject, "args"));
    std.debug.assert(@offsetOf(HandleObject, "cancelled") > @offsetOf(HandleObject, "context"));
}

var handle_type: py.Object = null;
var timer_type: py.Object = null;

pub fn handleType() py.Object {
    return handle_type;
}
pub fn timerType() py.Object {
    return timer_type;
}

/// Create a Handle (or TimerHandle if `is_timer`). Steals no references; it
/// incref's callback, args and context. Returns a new reference.
pub fn create(
    callback: py.Object,
    args: py.Object,
    context: py.Object,
    is_timer: bool,
    loop_obj: py.Object,
) py.Object {
    const tp = if (is_timer) timer_type else handle_type;
    const obj = py.allocInstance(tp);
    if (obj == null) return null;
    const h: *HandleObject = @ptrCast(obj);
    h.callback = py.newRef(callback);
    h.args = py.newRef(args);
    h.context = if (py.isNone(context) or context == null) makeContext() else py.newRef(context);
    h.cancelled = 0;
    h.is_timer = if (is_timer) 1 else 0;
    h.when = 0;
    h.timer_seq = 0;
    h.loop_obj = py.newRef(loop_obj); // owned - keeps the loop alive for cancel()
    // tp_alloc already GC-tracks the instance on this CPython; do not track again.
    return obj;
}

/// A fresh copy of the current contextvars context, matching asyncio's default
/// of running callbacks in contextvars.copy_context() when none is given.
/// PyContext_CopyCurrent is the raw C-API that copy_context() wraps - using it
/// directly skips a Python-level function call per handle (what uvloop does).
fn makeContext() py.Object {
    const ctx = c.PyContext_CopyCurrent();
    if (ctx == null) {
        py.c.PyErr_Clear();
        return py.none();
    }
    return ctx;
}

/// Execute the handle: call context.run(callback, *args), routing any exception
/// to the loop's exception handler. Safe to call on a cancelled handle (no-op).
pub fn run(self: *HandleObject) void {
    // Snapshot (callback, args, context) under the Handle's critical section,
    // taking our own references so a concurrent cancel() on another thread
    // cannot free them mid-call on a no-GIL build. No-op lock under the GIL.
    var callback: py.Object = null;
    var args: py.Object = null;
    var context: py.Object = null;
    if (zloop_handle_snapshot(@ptrCast(self), &callback, &args, &context) == 0) return;
    defer py.xdecref(callback);
    defer py.xdecref(args);
    defer py.xdecref(context);

    const result = if (context != null and !py.isNone(context))
        runInContext(callback, args, context)
    else
        py.callTuple(callback, args);

    if (result == null) {
        reportException(self);
    } else {
        py.decref(result);
    }
}

fn runInContext(callback: py.Object, args: py.Object, context: py.Object) py.Object {
    // Enter the captured context, invoke callback(*args) directly, then exit -
    // the raw C-API path (no `context.run` attribute lookup or extra arg tuple).
    if (c.PyContext_Enter(context) != 0) return null;
    const result = py.callTuple(callback, args);
    // Exit even on error; preserve the callback's exception across the exit.
    const exc = py.fetchException();
    if (c.PyContext_Exit(context) != 0) {
        // A failed exit is unexpected; surface it if the callback itself was OK.
        if (exc != null) py.decref(exc);
        py.xdecref(result);
        return null;
    }
    if (exc != null) {
        py.restoreException(exc);
        return null; // result is null here
    }
    return result;
}

fn reportException(self: *HandleObject) void {
    // Fetch the active exception and hand it to loop.call_exception_handler.
    const exc = py.fetchException();
    if (exc == null) return;
    defer py.decref(exc);

    if (self.loop_obj == null) {
        py.displayException(exc);
        return;
    }
    const handler = py.getAttr(self.loop_obj, "call_exception_handler");
    if (handler == null) {
        c.PyErr_Clear();
        py.displayException(exc);
        return;
    }
    defer py.decref(handler);

    const ctx = c.PyDict_New();
    if (ctx == null) {
        c.PyErr_Clear();
        return;
    }
    defer py.decref(ctx);
    const message = py.fromStrZ("Exception in callback");
    _ = c.PyDict_SetItemString(ctx, "message", message);
    py.xdecref(message);
    _ = c.PyDict_SetItemString(ctx, "exception", exc);
    _ = c.PyDict_SetItemString(ctx, "handle", @ptrCast(self));

    const r = py.callOneArg(handler, ctx);
    if (r == null) c.PyErr_Clear() else py.decref(r);
}

// -- Python type methods ------------------------------------------------------

fn dealloc(self_obj: ?*c.PyObject) callconv(.c) void {
    const self: *HandleObject = @ptrCast(self_obj.?);
    py.xdecref(self.callback);
    py.xdecref(self.args);
    py.xdecref(self.context);
    py.xdecref(self.loop_obj);
    py.freeInstance(@ptrCast(self));
}

fn cancel(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *HandleObject = @ptrCast(self_obj.?);
    // Mark cancelled and clear the callback refs under the Handle's critical
    // section (no-op lock under the GIL) so this can't race a concurrent run()
    // on the loop thread. The timer-heap cancel runs between the two, outside
    // the lock, because it is only ever touched on the loop's own thread.
    if (zloop_handle_mark_cancelled(@ptrCast(self)) != 0) {
        if (self.is_timer != 0 and self.loop_obj != null) {
            cancelTimerOnLoop(self);
        }
        zloop_handle_clear_refs(@ptrCast(self));
    }
    return py.none();
}

fn cancelled_method(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *HandleObject = @ptrCast(self_obj.?);
    return py.boolean(self.cancelled != 0);
}

fn when_method(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *HandleObject = @ptrCast(self_obj.?);
    return py.fromF64(self.when);
}

/// Provided by loop_obj.zig to avoid a cyclic import at module scope.
pub var cancel_timer_hook: ?*const fn (loop_obj: py.Object, token: usize, seq: u64) void = null;

fn cancelTimerOnLoop(self: *HandleObject) void {
    if (cancel_timer_hook) |hook| {
        hook(self.loop_obj, @intFromPtr(self), self.timer_seq);
    }
}

var handle_methods = [_]py.MethodDef{
    .{ .ml_name = "cancel", .ml_meth = cancel, .ml_flags = c.METH_NOARGS, .ml_doc = null },
    .{ .ml_name = "cancelled", .ml_meth = cancelled_method, .ml_flags = c.METH_NOARGS, .ml_doc = null },
    .{ .ml_name = null, .ml_meth = null, .ml_flags = 0, .ml_doc = null },
};

var timer_methods = [_]py.MethodDef{
    .{ .ml_name = "cancel", .ml_meth = cancel, .ml_flags = c.METH_NOARGS, .ml_doc = null },
    .{ .ml_name = "cancelled", .ml_meth = cancelled_method, .ml_flags = c.METH_NOARGS, .ml_doc = null },
    .{ .ml_name = "when", .ml_meth = when_method, .ml_flags = c.METH_NOARGS, .ml_doc = null },
    .{ .ml_name = null, .ml_meth = null, .ml_flags = 0, .ml_doc = null },
};

var handle_slots = [_]py.Slot{
    .{ .slot = c.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&dealloc)) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&handle_methods) },
    .{ .slot = 0, .pfunc = null },
};

var timer_slots = [_]py.Slot{
    .{ .slot = c.Py_tp_dealloc, .pfunc = @ptrCast(@constCast(&dealloc)) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&timer_methods) },
    .{ .slot = 0, .pfunc = null },
};

// Handles are deliberately NOT GC types: they are the hottest allocation in the
// loop and GC tracking measurably slows call_soon/call_later below uvloop. The
// engine->Handle->loop reference cycle is instead broken by Loop.close()'s
// dropPending() (asyncio likewise clears _ready/_scheduled on close). The only
// residual leak is abandoning a loop WITHOUT closing it while callbacks/timers
// are still pending - a misuse; always close (or use asyncio.run, which does).
var handle_spec = py.Spec{
    .name = "zloop.Handle",
    .basicsize = @sizeOf(HandleObject),
    .itemsize = 0,
    .flags = c.Py_TPFLAGS_DEFAULT,
    .slots = &handle_slots,
};

var timer_spec = py.Spec{
    .name = "zloop.TimerHandle",
    .basicsize = @sizeOf(HandleObject),
    .itemsize = 0,
    .flags = c.Py_TPFLAGS_DEFAULT,
    .slots = &timer_slots,
};

pub fn registerTypes(module: py.Object) bool {
    handle_type = py.typeFromSpec(&handle_spec);
    if (handle_type == null) return false;
    timer_type = py.typeFromSpec(&timer_spec);
    if (timer_type == null) return false;
    _ = module;
    return true;
}
