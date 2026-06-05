//! The Loop Python object: zloop's asyncio.AbstractEventLoop implementation.
//! It validates Python arguments and delegates scheduling and I/O to the Zig
//! engine (core/loop.zig). Coroutine driving (Future/Task) and TLS (sslproto)
//! are reused from CPython's asyncio, matching uvloop's boundary.

const std = @import("std");
const py = @import("py.zig");
const c = py.c;
const handle = @import("handle.zig");
const core_root = @import("core");
const core = core_root.loop;
const clock = core_root.clock;
const sys = core_root.sys;

const gpa = std.heap.c_allocator;

pub const LoopObject = extern struct {
    ob_base: c.PyObject,
    /// Heap-allocated Zig engine. Pointer so the extern struct stays POD.
    engine: ?*core.Loop,
    /// asyncio bits cached on the instance (all owned references).
    exception_handler: py.Object,
    task_factory: py.Object,
    default_executor: py.Object,
    asyncgens: py.Object, // a set
    debug: u8,
    closed: u8,
    /// Thread id that is running the loop, or 0 when not running. Used by
    /// get_running_loop/_check_running semantics.
    thread_id: u64,
};

var loop_type: py.Object = null;

pub fn loopType() py.Object {
    return loop_type;
}

// ---------------------------------------------------------------------------
// dispatcher: bridges the Zig engine to Python Handles
// ---------------------------------------------------------------------------

fn dispatchRun(_: *anyopaque, token: usize) void {
    const h: *handle.HandleObject = @ptrFromInt(token);
    handle.run(h);
    py.decref(@as(py.Object, @ptrCast(h))); // the schedule held a ref
}

fn dispatchDrop(_: *anyopaque, token: usize) void {
    const h: *handle.HandleObject = @ptrFromInt(token);
    py.decref(@as(py.Object, @ptrCast(h)));
}

fn cancelTimerHook(loop_obj: py.Object, token: usize, seq: u64) void {
    const self: *LoopObject = @ptrCast(loop_obj);
    if (self.engine) |eng| {
        if (eng.cancelTimer(token, seq)) {
            // The engine still holds the schedule ref; release it now.
            py.decref(@as(py.Object, @ptrFromInt(token)));
        }
    }
}

// ---------------------------------------------------------------------------
// construction
// ---------------------------------------------------------------------------

fn new(tp: [*c]c.PyTypeObject, _: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const obj = tp.*.tp_alloc.?(tp, 0);
    if (obj == null) return null;
    const self: *LoopObject = @ptrCast(obj);
    self.engine = null;
    self.exception_handler = py.none();
    self.task_factory = py.none();
    self.default_executor = py.none();
    self.asyncgens = c.PySet_New(null);
    self.debug = 0;
    self.closed = 0;
    self.thread_id = 0;

    const engine = gpa.create(core.Loop) catch {
        py.decref(obj);
        return py.raiseRuntime("zloop: failed to allocate engine");
    };
    engine.* = core.Loop.init(gpa, .{
        .ctx = engine,
        .run = dispatchRun,
        .drop = dispatchDrop,
    }) catch {
        gpa.destroy(engine);
        py.decref(obj);
        return py.raiseRuntime("zloop: failed to initialise engine");
    };
    self.engine = engine;
    return obj;
}

fn dealloc(self_obj: ?*c.PyObject) callconv(.c) void {
    const self: *LoopObject = @ptrCast(self_obj.?);
    if (self.engine) |eng| {
        eng.deinit();
        gpa.destroy(eng);
        self.engine = null;
    }
    py.xdecref(self.exception_handler);
    py.xdecref(self.task_factory);
    py.xdecref(self.default_executor);
    py.xdecref(self.asyncgens);
    py.freeInstance(@ptrCast(self));
}

inline fn engineOf(self: *LoopObject) ?*core.Loop {
    return self.engine;
}

// ---------------------------------------------------------------------------
// scheduling
// ---------------------------------------------------------------------------

/// Build a Handle for (callback, args-from-tuple-after-first, context) and
/// schedule it via `schedule`. `args` is the full method args tuple; callback
/// is at index `cb_index`, callback args follow.
fn makeHandle(self: *LoopObject, callback: py.Object, cb_args: py.Object, context: py.Object, is_timer: bool) py.Object {
    return handle.create(callback, cb_args, context, is_timer, @ptrCast(self));
}

/// Slice a method-args tuple to the callback's positional args (everything
/// after `from`). Returns a new tuple reference.
fn sliceArgs(args: py.Object, from: py.ssize) py.Object {
    const total = py.tupleSize(args);
    const n = total - from;
    const out = py.tupleNew(n);
    if (out == null) return null;
    var i: py.ssize = 0;
    while (i < n) : (i += 1) {
        py.tupleSet(out, i, py.newRef(py.tupleGet(args, from + i)));
    }
    return out;
}

fn extractContext(kwargs: ?*c.PyObject) py.Object {
    if (kwargs == null) return py.none();
    const ctx = c.PyDict_GetItemString(kwargs, "context"); // borrowed
    if (ctx == null) return py.none();
    return py.newRef(ctx);
}

fn call_soon(self_obj: ?*c.PyObject, args: ?*c.PyObject, kwargs: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    if (py.tupleSize(args) < 1) return py.raiseType("call_soon requires a callback");
    const callback = py.tupleGet(args, 0);
    if (!py.isCallable(callback)) return py.raiseType("a callable is required");

    const cb_args = sliceArgs(args, 1);
    if (cb_args == null) return null;
    defer py.decref(cb_args);
    const context = extractContext(kwargs);
    defer py.decref(context);

    const h = makeHandle(self, callback, cb_args, context, false);
    if (h == null) return null;

    const eng = engineOf(self) orelse return py.raiseRuntime("loop is closed");
    py.incref(h); // schedule holds a ref, released by dispatch
    eng.callSoon(@intFromPtr(h)) catch {
        py.decref(h);
        py.decref(h);
        return py.raiseRuntime("zloop: failed to schedule callback");
    };
    return h; // caller gets the other ref
}

fn scheduleTimer(self: *LoopObject, when_s: f64, callback: py.Object, cb_args: py.Object, context: py.Object) py.Object {
    const h = makeHandle(self, callback, cb_args, context, true);
    if (h == null) return null;
    const ho: *handle.HandleObject = @ptrCast(h);
    ho.when = when_s;

    const eng = engineOf(self) orelse {
        py.decref(h);
        return py.raiseRuntime("loop is closed");
    };
    const when_ns = secondsToNs(when_s);
    py.incref(h);
    const seq = eng.callAt(when_ns, @intFromPtr(h)) catch {
        py.decref(h);
        py.decref(h);
        return py.raiseRuntime("zloop: failed to schedule timer");
    };
    ho.timer_seq = seq;
    return h;
}

fn call_at(self_obj: ?*c.PyObject, args: ?*c.PyObject, kwargs: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    if (py.tupleSize(args) < 2) return py.raiseType("call_at(when, callback, ...)");
    const when = py.asF64(py.tupleGet(args, 0)) orelse return null;
    const callback = py.tupleGet(args, 1);
    if (!py.isCallable(callback)) return py.raiseType("a callable is required");
    const cb_args = sliceArgs(args, 2);
    if (cb_args == null) return null;
    defer py.decref(cb_args);
    const context = extractContext(kwargs);
    defer py.decref(context);
    return scheduleTimer(self, when, callback, cb_args, context);
}

fn call_later(self_obj: ?*c.PyObject, args: ?*c.PyObject, kwargs: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    if (py.tupleSize(args) < 2) return py.raiseType("call_later(delay, callback, ...)");
    const delay = py.asF64(py.tupleGet(args, 0)) orelse return null;
    const callback = py.tupleGet(args, 1);
    if (!py.isCallable(callback)) return py.raiseType("a callable is required");
    const cb_args = sliceArgs(args, 2);
    if (cb_args == null) return null;
    defer py.decref(cb_args);
    const context = extractContext(kwargs);
    defer py.decref(context);
    const when = clock.nowSeconds() + @max(delay, 0);
    return scheduleTimer(self, when, callback, cb_args, context);
}

fn call_soon_threadsafe(self_obj: ?*c.PyObject, args: ?*c.PyObject, kwargs: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    const h = call_soon(self_obj, args, kwargs);
    if (h == null) return null;
    if (engineOf(self)) |eng| eng.wakeup();
    return h;
}

fn time_method(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    _ = self_obj;
    return py.fromF64(clock.nowSeconds());
}

fn secondsToNs(s: f64) u64 {
    if (s <= 0) return clock.nowNs();
    return @intFromFloat(s * std.time.ns_per_s);
}

// ---------------------------------------------------------------------------
// futures & tasks (reuse asyncio)
// ---------------------------------------------------------------------------

fn create_future(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const cls = py.importFrom("asyncio", "Future");
    if (cls == null) return null;
    defer py.decref(cls);
    const args = py.tupleNew(0);
    defer py.decref(args);
    const kwargs = c.PyDict_New();
    defer py.decref(kwargs);
    _ = c.PyDict_SetItemString(kwargs, "loop", self_obj);
    return py.callTupleKw(cls, args, kwargs);
}

fn create_task(self_obj: ?*c.PyObject, args: ?*c.PyObject, kwargs: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    if (py.tupleSize(args) < 1) return py.raiseType("create_task(coro, ...)");
    const coro = py.tupleGet(args, 0);

    if (!py.isNone(self.task_factory)) {
        // task_factory(loop, coro, **kwargs)
        const fargs = py.tupleNew(2);
        if (fargs == null) return null;
        defer py.decref(fargs);
        py.tupleSet(fargs, 0, py.newRef(self_obj));
        py.tupleSet(fargs, 1, py.newRef(coro));
        return py.callTupleKw(self.task_factory, fargs, kwargs);
    }

    const cls = py.importFrom("asyncio", "Task");
    if (cls == null) return null;
    defer py.decref(cls);
    const targs = py.tupleNew(1);
    if (targs == null) return null;
    defer py.decref(targs);
    py.tupleSet(targs, 0, py.newRef(coro));
    const tkwargs = if (kwargs != null) py.newRef(kwargs) else c.PyDict_New();
    defer py.decref(tkwargs);
    _ = c.PyDict_SetItemString(tkwargs, "loop", self_obj);
    return py.callTupleKw(cls, targs, tkwargs);
}

fn set_task_factory(self_obj: ?*c.PyObject, factory: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    if (!py.isNone(factory.?) and !py.isCallable(factory.?))
        return py.raiseType("task factory must be a callable or None");
    py.clear(&self.task_factory);
    self.task_factory = py.newRef(factory.?);
    return py.none();
}

fn get_task_factory(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    return py.newRef(self.task_factory);
}

// ---------------------------------------------------------------------------
// run loop
// ---------------------------------------------------------------------------

fn is_running(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    return py.boolean(self.thread_id != 0);
}

fn is_closed(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    return py.boolean(self.closed != 0);
}

fn stop(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    if (engineOf(self)) |eng| eng.stop();
    return py.none();
}

fn run_forever(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    const eng = engineOf(self) orelse return py.raiseRuntime("loop is closed");
    if (self.thread_id != 0) return py.raiseRuntime("This event loop is already running");

    setRunningLoop(self_obj);
    self.thread_id = currentThreadId();
    eng.runForever() catch {
        self.thread_id = 0;
        clearRunningLoop();
        return py.raiseRuntime("zloop: run_forever failed");
    };
    self.thread_id = 0;
    clearRunningLoop();
    if (py.errOccurred()) return null;
    return py.none();
}

fn run_until_complete(self_obj: ?*c.PyObject, future: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    _ = self;
    // future = ensure_future(future, loop=self)
    const ensure = py.importFrom("asyncio", "ensure_future");
    if (ensure == null) return null;
    defer py.decref(ensure);
    const ef_args = py.tupleNew(1);
    defer py.decref(ef_args);
    py.tupleSet(ef_args, 0, py.newRef(future.?));
    const ef_kwargs = c.PyDict_New();
    defer py.decref(ef_kwargs);
    _ = c.PyDict_SetItemString(ef_kwargs, "loop", self_obj);
    const fut = py.callTupleKw(ensure, ef_args, ef_kwargs);
    if (fut == null) return null;
    defer py.decref(fut);

    // fut.add_done_callback(lambda _: loop.stop()) -> use a bound stop wrapper
    const stop_cb = makeStopCallback(self_obj);
    if (stop_cb == null) return null;
    defer py.decref(stop_cb);
    const add_done = py.getAttr(fut, "add_done_callback");
    if (add_done == null) return null;
    defer py.decref(add_done);
    const adc_res = py.callOneArg(add_done, stop_cb);
    if (adc_res == null) return null;
    py.decref(adc_res);

    const rf = run_forever(self_obj, null);
    if (rf == null) return null;
    py.decref(rf);

    // return fut.result()
    const result_attr = py.getAttr(fut, "result");
    if (result_attr == null) return null;
    defer py.decref(result_attr);
    return py.callNoArgs(result_attr);
}

/// A small callable that calls loop.stop() ignoring its argument, used as the
/// future done-callback in run_until_complete.
fn makeStopCallback(loop_obj: py.Object) py.Object {
    // functools.partial(getattr(loop, '_run_until_complete_cb')) is overkill;
    // we use a tiny C method bound to the loop via a closure object.
    return StopCallback.create(loop_obj);
}

const StopCallback = struct {
    var sc_type: py.Object = null;

    const Obj = extern struct {
        ob_base: c.PyObject,
        loop: py.Object,
    };

    fn scCall(self_obj: ?*c.PyObject, _: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
        const o: *Obj = @ptrCast(self_obj.?);
        const lo: *LoopObject = @ptrCast(o.loop);
        if (engineOf(lo)) |eng| eng.stop();
        return py.none();
    }
    fn scDealloc(self_obj: ?*c.PyObject) callconv(.c) void {
        const o: *Obj = @ptrCast(self_obj.?);
        py.xdecref(o.loop);
        py.freeInstance(@ptrCast(o));
    }
    var sc_slots = [_]py.Slot{
        .{ .slot = c.Py_tp_call, .pfunc = @constCast(@ptrCast(&scCall)) },
        .{ .slot = c.Py_tp_dealloc, .pfunc = @constCast(@ptrCast(&scDealloc)) },
        .{ .slot = 0, .pfunc = null },
    };
    var sc_spec = py.Spec{
        .name = "zloop._StopCallback",
        .basicsize = @sizeOf(Obj),
        .itemsize = 0,
        .flags = c.Py_TPFLAGS_DEFAULT,
        .slots = &sc_slots,
    };
    fn ensureType() bool {
        if (sc_type == null) sc_type = py.typeFromSpec(&sc_spec);
        return sc_type != null;
    }
    fn create(loop_obj: py.Object) py.Object {
        if (!ensureType()) return null;
        const obj = py.allocInstance(sc_type);
        if (obj == null) return null;
        const o: *Obj = @ptrCast(obj);
        o.loop = py.newRef(loop_obj);
        return obj;
    }
};

fn close_method(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    if (self.thread_id != 0) return py.raiseRuntime("Cannot close a running event loop");
    if (self.closed != 0) return py.none();
    self.closed = 1;
    if (engineOf(self)) |eng| eng.close();
    return py.none();
}

// ---------------------------------------------------------------------------
// debug + exception handling
// ---------------------------------------------------------------------------

fn get_debug(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    return py.boolean(self.debug != 0);
}
fn set_debug(self_obj: ?*c.PyObject, arg: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    self.debug = if (py.isTrue(arg.?)) 1 else 0;
    return py.none();
}

fn get_exception_handler(self_obj: ?*c.PyObject, _: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    return py.newRef(self.exception_handler);
}
fn set_exception_handler(self_obj: ?*c.PyObject, h: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    if (!py.isNone(h.?) and !py.isCallable(h.?))
        return py.raiseType("A callable object or None is expected");
    py.clear(&self.exception_handler);
    self.exception_handler = py.newRef(h.?);
    return py.none();
}

fn default_exception_handler(self_obj: ?*c.PyObject, context: ?*c.PyObject) callconv(.c) py.Object {
    _ = self_obj;
    // Delegate to a tiny Python helper that formats like asyncio does.
    const helper = py.importFrom("zloop._support", "default_exception_handler");
    if (helper == null) return null;
    defer py.decref(helper);
    return py.callOneArg(helper, context.?);
}

fn call_exception_handler(self_obj: ?*c.PyObject, context: ?*c.PyObject) callconv(.c) py.Object {
    const self: *LoopObject = @ptrCast(self_obj.?);
    if (py.isNone(self.exception_handler)) {
        return default_exception_handler(self_obj, context);
    }
    const r = py.callOneArg(self.exception_handler, context.?);
    if (r == null) {
        // Exception in custom handler: fall back, mirroring asyncio.
        c.PyErr_Clear();
        return default_exception_handler(self_obj, context);
    }
    return r;
}

// ---------------------------------------------------------------------------
// running-loop registration (so asyncio.get_running_loop() works)
// ---------------------------------------------------------------------------

fn setRunningLoop(self_obj: ?*c.PyObject) void {
    const f = py.importFrom("asyncio.events", "_set_running_loop");
    if (f == null) {
        c.PyErr_Clear();
        return;
    }
    defer py.decref(f);
    const r = py.callOneArg(f, self_obj.?);
    if (r == null) c.PyErr_Clear() else py.decref(r);
}

fn clearRunningLoop() void {
    const f = py.importFrom("asyncio.events", "_set_running_loop");
    if (f == null) {
        c.PyErr_Clear();
        return;
    }
    defer py.decref(f);
    const r = py.callOneArg(f, py.c.Py_None());
    if (r == null) c.PyErr_Clear() else py.decref(r);
}

fn currentThreadId() u64 {
    return @intCast(c.PyThread_get_thread_ident());
}

// ---------------------------------------------------------------------------
// method table + type
// ---------------------------------------------------------------------------

const KW = c.METH_VARARGS | c.METH_KEYWORDS;
const NOARGS = c.METH_NOARGS;
const O = c.METH_O;

var methods = [_]py.MethodDef{
    .{ .ml_name = "call_soon", .ml_meth = @ptrCast(&call_soon), .ml_flags = KW, .ml_doc = null },
    .{ .ml_name = "call_later", .ml_meth = @ptrCast(&call_later), .ml_flags = KW, .ml_doc = null },
    .{ .ml_name = "call_at", .ml_meth = @ptrCast(&call_at), .ml_flags = KW, .ml_doc = null },
    .{ .ml_name = "call_soon_threadsafe", .ml_meth = @ptrCast(&call_soon_threadsafe), .ml_flags = KW, .ml_doc = null },
    .{ .ml_name = "time", .ml_meth = time_method, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "create_future", .ml_meth = create_future, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "create_task", .ml_meth = @ptrCast(&create_task), .ml_flags = KW, .ml_doc = null },
    .{ .ml_name = "set_task_factory", .ml_meth = set_task_factory, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = "get_task_factory", .ml_meth = get_task_factory, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "run_forever", .ml_meth = run_forever, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "run_until_complete", .ml_meth = run_until_complete, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = "stop", .ml_meth = stop, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "is_running", .ml_meth = is_running, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "is_closed", .ml_meth = is_closed, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "close", .ml_meth = close_method, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "get_debug", .ml_meth = get_debug, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "set_debug", .ml_meth = set_debug, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = "get_exception_handler", .ml_meth = get_exception_handler, .ml_flags = NOARGS, .ml_doc = null },
    .{ .ml_name = "set_exception_handler", .ml_meth = set_exception_handler, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = "default_exception_handler", .ml_meth = default_exception_handler, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = "call_exception_handler", .ml_meth = call_exception_handler, .ml_flags = O, .ml_doc = null },
    .{ .ml_name = null, .ml_meth = null, .ml_flags = 0, .ml_doc = null },
};

var slots = [_]py.Slot{
    .{ .slot = c.Py_tp_new, .pfunc = @constCast(@ptrCast(&new)) },
    .{ .slot = c.Py_tp_dealloc, .pfunc = @constCast(@ptrCast(&dealloc)) },
    .{ .slot = c.Py_tp_methods, .pfunc = @ptrCast(&methods) },
    .{ .slot = 0, .pfunc = null },
};

var spec = py.Spec{
    .name = "zloop.Loop",
    .basicsize = @sizeOf(LoopObject),
    .itemsize = 0,
    .flags = c.Py_TPFLAGS_DEFAULT | c.Py_TPFLAGS_BASETYPE,
    .slots = &slots,
};

pub fn registerType(module: py.Object) bool {
    const base = py.importFrom("asyncio", "AbstractEventLoop");
    if (base == null) return false;
    defer py.decref(base);
    loop_type = py.typeFromSpecWithBase(&spec, base);
    if (loop_type == null) return false;
    handle.cancel_timer_hook = cancelTimerHook;
    _ = module;
    return true;
}
