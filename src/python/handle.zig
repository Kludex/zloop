//! Handle / TimerHandle: Python objects wrapping a (callback, args, context)
//! triple, mirroring asyncio.Handle. The Zig loop stores a Handle's pointer as
//! its opaque token; dispatching the token runs the handle. asyncio and uvicorn
//! call handle.cancel(); TimerHandle additionally exposes when() and supports
//! ordering, which asyncio's heap relies on - though our timers live in Zig, we
//! keep the interface for compatibility.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;

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
    /// Back-pointer to the owning loop object, borrowed, so cancel() can reach
    /// the Zig timer queue. Null for plain (call_soon) handles.
    loop_obj: py.Object,
};

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
    h.loop_obj = loop_obj; // borrowed
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
    if (self.cancelled != 0) return;

    const result = if (!py.isNone(self.context))
        runInContext(self)
    else
        py.callTuple(self.callback, self.args);

    if (result == null) {
        reportException(self);
    } else {
        py.decref(result);
    }
}

fn runInContext(self: *HandleObject) py.Object {
    // Enter the captured context, invoke callback(*args) directly, then exit -
    // the raw C-API path (no `context.run` attribute lookup or extra arg tuple).
    if (c.PyContext_Enter(self.context) != 0) return null;
    const result = py.callTuple(self.callback, self.args);
    // Exit even on error; preserve the callback's exception across the exit.
    const exc = c.PyErr_GetRaisedException();
    if (c.PyContext_Exit(self.context) != 0) {
        // A failed exit is unexpected; surface it if the callback itself was OK.
        if (exc != null) py.decref(exc);
        py.xdecref(result);
        return null;
    }
    if (exc != null) {
        c.PyErr_SetRaisedException(exc);
        return null; // result is null here
    }
    return result;
}

fn reportException(self: *HandleObject) void {
    // Fetch the active exception and hand it to loop.call_exception_handler.
    const exc = c.PyErr_GetRaisedException();
    if (exc == null) return;
    defer py.decref(exc);

    if (self.loop_obj == null) {
        c.PyErr_DisplayException(exc);
        return;
    }
    const handler = py.getAttr(self.loop_obj, "call_exception_handler");
    if (handler == null) {
        c.PyErr_Clear();
        c.PyErr_DisplayException(exc);
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
    py.freeInstance(@ptrCast(self));
}

fn cancel(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *HandleObject = @ptrCast(self_obj.?);
    if (self.cancelled == 0) {
        self.cancelled = 1;
        if (self.is_timer != 0 and self.loop_obj != null) {
            cancelTimerOnLoop(self);
        }
        py.clearCallbackRefs(self);
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
