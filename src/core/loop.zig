//! The event loop engine. It owns the reactor, the timer queue and the ready
//! FIFO, and implements the canonical asyncio "run once" iteration. It has no
//! knowledge of Python.
//!
//! There are two kinds of work:
//!   * Deferred callbacks (call_soon / call_at / timers) are opaque `usize`
//!     tokens executed via the `Dispatcher` the embedder supplies. The adapter
//!     maps a token to a Python Handle.
//!   * I/O readiness callbacks (add_reader / add_writer) are native Zig
//!     closures (`IoCallback`). Transports register these directly so socket
//!     I/O never round-trips through Python; the Python add_reader wrapper
//!     installs a closure that just enqueues a Handle.

const std = @import("std");
const reactor_mod = @import("reactor.zig");
const timers_mod = @import("timers.zig");
const queue_mod = @import("queue.zig");
const clock = @import("clock.zig");
const sys = @import("sys.zig");

const Reactor = reactor_mod.Reactor;
const Interest = reactor_mod.Interest;
const TimerQueue = timers_mod.TimerQueue;
const ReadyQueue = queue_mod.ReadyQueue;

/// Executes / releases deferred-callback tokens (Python Handles), and brackets
/// the blocking poll so the embedder can release a global lock (the CPython
/// GIL) while the loop waits, letting other threads run and signals be handled.
pub const Dispatcher = struct {
    ctx: *anyopaque,
    run: *const fn (ctx: *anyopaque, token: usize) void,
    drop: *const fn (ctx: *anyopaque, token: usize) void,
    /// Called immediately before a blocking poll; returns an opaque state passed
    /// back to `resume_`. May be null (no bracketing).
    suspend_: ?*const fn (ctx: *anyopaque) ?*anyopaque = null,
    resume_: ?*const fn (ctx: *anyopaque, state: ?*anyopaque) void = null,
};

/// Readiness reported to an I/O callback.
pub const IoEvent = struct {
    readable: bool = false,
    writable: bool = false,
    hup: bool = false,
};

/// A native I/O readiness callback owned by the registrant (e.g. a transport).
/// `dispose` is called when the callback is removed so the owner can release
/// resources; it may be null.
pub const IoCallback = struct {
    func: *const fn (ctx: *anyopaque, ev: IoEvent) void,
    ctx: *anyopaque,
    dispose: ?*const fn (ctx: *anyopaque) void = null,

    fn fire(self: IoCallback, ev: IoEvent) void {
        self.func(self.ctx, ev);
    }
    fn dispose_(self: IoCallback) void {
        if (self.dispose) |d| d(self.ctx);
    }
};

/// A minimal spinlock for the brief cross-thread inbox critical section.
const SpinLock = struct {
    state: std.atomic.Mutex = .unlocked,
    fn lock(self: *SpinLock) void {
        while (!self.state.tryLock()) std.atomic.spinLoopHint();
    }
    fn unlock(self: *SpinLock) void {
        self.state.unlock();
    }
};

const FdState = struct {
    reader: ?IoCallback = null,
    writer: ?IoCallback = null,

    fn interest(self: FdState) Interest {
        return .{ .read = self.reader != null, .write = self.writer != null };
    }
    fn isEmpty(self: FdState) bool {
        return self.reader == null and self.writer == null;
    }
};

pub const Loop = struct {
    allocator: std.mem.Allocator,
    reactor: Reactor,
    timers: TimerQueue,
    ready: ReadyQueue,
    fds: std.AutoHashMap(sys.fd_t, FdState),
    dispatcher: Dispatcher,

    running: bool = false,
    stopping: bool = false,
    closed: bool = false,
    dropped: bool = false,

    /// Cross-thread inbox: call_soon_threadsafe appends here under `xlock` and
    /// wakes the loop; runOnce drains it into `ready` on the loop thread. The
    /// main ready queue is single-threaded and must never be touched off-thread.
    /// The critical section is a few instructions, so a spinlock is appropriate.
    xthread: std.ArrayList(usize),
    xlock: SpinLock = .{},

    wake_r: sys.fd_t,
    wake_w: sys.fd_t,
    const WAKE_TOKEN: usize = std.math.maxInt(usize);

    pub fn init(allocator: std.mem.Allocator, dispatcher: Dispatcher) !Loop {
        var r = try Reactor.init(allocator);
        errdefer r.deinit();

        const pipe = try sys.pipe();
        errdefer sys.close(pipe[0]);
        errdefer sys.close(pipe[1]);
        try sys.setNonBlocking(pipe[0]);
        try sys.setNonBlocking(pipe[1]);
        try sys.setCloexec(pipe[0]);
        try sys.setCloexec(pipe[1]);
        try r.register(pipe[0], WAKE_TOKEN, .{ .read = true });

        return .{
            .allocator = allocator,
            .reactor = r,
            .timers = TimerQueue.init(allocator),
            .ready = ReadyQueue.init(allocator),
            .fds = std.AutoHashMap(sys.fd_t, FdState).init(allocator),
            .dispatcher = dispatcher,
            .xthread = .empty,
            .wake_r = pipe[0],
            .wake_w = pipe[1],
        };
    }

    /// Release every deferred-callback token the engine still owns (ready queue,
    /// timer heap, cross-thread inbox) via the embedder's drop hook. Called from
    /// both close() and deinit(): dropping these on close breaks the
    /// engine->Handle->loop reference cycle so the loop can be collected even
    /// when it is closed while callbacks/timers are still pending.
    fn dropPending(self: *Loop) void {
        // Run exactly once. close() drops pending work to break the cycle; deinit()
        // also calls this for loops that were never closed. Without this guard a
        // closed-then-freed loop drops twice - and a token that is both a cancelled
        // timer and still referenced elsewhere gets double-decref'd into a
        // use-after-free (a crash only ReleaseFast exposes; Debug zeroing hides it).
        if (self.dropped) return;
        self.dropped = true;
        self.drainXthread(); // moves cross-thread tokens into `ready`
        while (self.ready.pop()) |token| self.dispatcher.drop(self.dispatcher.ctx, token);
        for (self.timers.heap.items) |timer| {
            if (!timer.cancelled) self.dispatcher.drop(self.dispatcher.ctx, timer.token);
        }
        self.timers.heap.clearRetainingCapacity();
    }

    pub fn deinit(self: *Loop) void {
        self.dropPending();
        var it = self.fds.valueIterator();
        while (it.next()) |st| {
            if (st.reader) |cb| cb.dispose_();
            if (st.writer) |cb| cb.dispose_();
        }
        sys.close(self.wake_r);
        sys.close(self.wake_w);
        self.reactor.deinit();
        self.timers.deinit();
        self.ready.deinit();
        self.xthread.deinit(self.allocator);
        self.fds.deinit();
    }

    // -- deferred callbacks ---------------------------------------------------

    pub fn callSoon(self: *Loop, token: usize) !void {
        try self.ready.push(token);
    }

    /// Schedule a token from any thread. Appends under the cross-thread lock and
    /// wakes the loop. Returns an error only on OOM.
    pub fn callSoonThreadsafe(self: *Loop, token: usize) !void {
        {
            self.xlock.lock();
            defer self.xlock.unlock();
            // Check closed under the same lock that close()/drainXthread take, so
            // a token is never stranded by racing close(): either we observe the
            // close and reject (caller releases the token), or we append before
            // close and drainXthread drops it. Without this, a free-threaded
            // call_soon_threadsafe racing close() leaks the Handle.
            if (self.closed) return error.LoopClosed;
            try self.xthread.append(self.allocator, token);
        }
        self.wakeup();
    }

    fn drainXthread(self: *Loop) void {
        self.xlock.lock();
        const items = self.xthread.toOwnedSlice(self.allocator) catch {
            // On OOM, leave items queued for the next iteration.
            self.xlock.unlock();
            return;
        };
        self.xlock.unlock();
        defer self.allocator.free(items);
        for (items) |token| {
            // If the ready queue can't grow (OOM), release the token's resources
            // (the embedder's Handle ref) rather than leaking it.
            self.ready.push(token) catch self.dispatcher.drop(self.dispatcher.ctx, token);
        }
    }

    pub fn callAt(self: *Loop, when_ns: u64, token: usize) !u64 {
        return self.timers.push(when_ns, token);
    }

    pub fn cancelTimer(self: *Loop, token: usize, seq: u64) bool {
        return self.timers.cancel(token, seq);
    }

    pub fn now(_: *Loop) u64 {
        return clock.nowNs();
    }

    pub fn wakeup(self: *Loop) void {
        _ = sys.write(self.wake_w, "\x00") catch {};
    }

    // -- I/O readiness --------------------------------------------------------

    pub fn addReader(self: *Loop, fd: sys.fd_t, cb: IoCallback) !void {
        try self.addIo(fd, cb, .read);
    }

    pub fn addWriter(self: *Loop, fd: sys.fd_t, cb: IoCallback) !void {
        try self.addIo(fd, cb, .write);
    }

    const Dir = enum { read, write };

    /// Register an I/O callback transactionally: sync the reactor against the
    /// would-be new interest FIRST, and only commit to `self.fds` (and dispose
    /// any replaced callback) once that succeeds. On failure nothing is mutated
    /// and `cb` is still owned by the caller - so the caller's error handling
    /// (which releases `cb`) never races a stale entry in `fds`.
    fn addIo(self: *Loop, fd: sys.fd_t, cb: IoCallback, dir: Dir) !void {
        const gop = try self.fds.getOrPut(fd);
        if (!gop.found_existing) gop.value_ptr.* = .{};
        var next = gop.value_ptr.*;
        switch (dir) {
            .read => next.reader = cb,
            .write => next.writer = cb,
        }
        self.syncFdReg(fd, next, !gop.found_existing) catch |err| {
            if (!gop.found_existing) _ = self.fds.remove(fd);
            return err;
        };
        // Reactor committed; now it is safe to dispose the replaced callback and
        // publish the new state.
        const old = switch (dir) {
            .read => gop.value_ptr.reader,
            .write => gop.value_ptr.writer,
        };
        if (old) |o| o.dispose_();
        gop.value_ptr.* = next;
    }

    pub fn removeReader(self: *Loop, fd: sys.fd_t) bool {
        const entry = self.fds.getEntry(fd) orelse return false;
        const had = entry.value_ptr.reader != null;
        if (entry.value_ptr.reader) |old| old.dispose_();
        entry.value_ptr.reader = null;
        self.syncFdUnreg(fd, entry.value_ptr.*);
        return had;
    }

    pub fn removeWriter(self: *Loop, fd: sys.fd_t) bool {
        const entry = self.fds.getEntry(fd) orelse return false;
        const had = entry.value_ptr.writer != null;
        if (entry.value_ptr.writer) |old| old.dispose_();
        entry.value_ptr.writer = null;
        self.syncFdUnreg(fd, entry.value_ptr.*);
        return had;
    }

    fn syncFdReg(self: *Loop, fd: sys.fd_t, state: FdState, is_new: bool) !void {
        if (is_new) {
            try self.reactor.register(fd, fdToken(fd), state.interest());
        } else {
            try self.reactor.modify(fd, fdToken(fd), state.interest());
        }
    }

    fn syncFdUnreg(self: *Loop, fd: sys.fd_t, state: FdState) void {
        if (state.isEmpty()) {
            self.reactor.unregister(fd) catch {};
            _ = self.fds.remove(fd);
        } else {
            self.reactor.modify(fd, fdToken(fd), state.interest()) catch {};
        }
    }

    fn fdToken(fd: sys.fd_t) usize {
        return @intCast(fd);
    }

    // -- the run loop ---------------------------------------------------------

    pub fn isRunning(self: *const Loop) bool {
        return self.running;
    }
    pub fn isClosed(self: *const Loop) bool {
        return self.closed;
    }

    pub fn stop(self: *Loop) void {
        self.stopping = true;
        self.wakeup();
    }

    pub fn runForever(self: *Loop) !void {
        if (self.running) return error.AlreadyRunning;
        if (self.closed) return error.LoopClosed;
        self.running = true;
        self.stopping = false;
        defer self.running = false;
        while (!self.stopping) {
            try self.runOnce(null);
        }
    }

    pub fn runOnce(self: *Loop, max_wait_ns: ?u64) !void {
        self.drainXthread();
        const timeout = self.computeTimeout(max_wait_ns);

        // Release the embedder's global lock (GIL) only when we will actually
        // block, so other threads can run and post work via call_soon_threadsafe
        // and so signals can be delivered.
        const will_block = timeout == null or timeout.? > 0;
        var resume_state: ?*anyopaque = null;
        if (will_block) {
            if (self.dispatcher.suspend_) |s| resume_state = s(self.dispatcher.ctx);
        }

        var events: [256]reactor_mod.Event = undefined;
        const poll_result = self.reactor.poll(&events, timeout);

        if (will_block) {
            if (self.dispatcher.resume_) |r| r(self.dispatcher.ctx, resume_state);
        }

        const ready_events = try poll_result;

        for (ready_events) |ev| {
            if (ev.token == WAKE_TOKEN) {
                self.drainWake();
                continue;
            }
            const fd: sys.fd_t = @intCast(ev.token);
            const state = self.fds.get(fd) orelse continue;
            const io: IoEvent = .{ .readable = ev.readable, .writable = ev.writable, .hup = ev.hup };
            if (ev.readable or ev.hup) if (state.reader) |cb| cb.fire(io);
            if (ev.writable or ev.hup) if (state.writer) |cb| cb.fire(io);
        }

        const t_now = clock.nowNs();
        while (self.timers.popDue(t_now)) |timer| {
            // The timer is already popped; on OOM release its token rather than
            // erroring out of the loop and leaking the Handle ref.
            self.ready.push(timer.token) catch self.dispatcher.drop(self.dispatcher.ctx, timer.token);
        }

        var n = self.ready.len();
        while (n > 0) : (n -= 1) {
            const token = self.ready.pop() orelse break;
            self.dispatcher.run(self.dispatcher.ctx, token);
        }
    }

    fn computeTimeout(self: *Loop, max_wait_ns: ?u64) ?u64 {
        if (!self.ready.isEmpty() or self.stopping) return 0;
        var timeout: ?u64 = null;
        if (self.timers.peekDeadline()) |deadline| {
            const t_now = clock.nowNs();
            timeout = if (deadline > t_now) deadline - t_now else 0;
        }
        if (max_wait_ns) |cap| {
            timeout = if (timeout) |t| @min(t, cap) else cap;
        }
        return timeout;
    }

    fn drainWake(self: *Loop) void {
        var buf: [256]u8 = undefined;
        while (true) {
            _ = sys.read(self.wake_r, &buf) catch break;
        }
    }

    pub fn close(self: *Loop) void {
        if (self.closed) return;
        // Publish `closed` under the xlock so a concurrent call_soon_threadsafe
        // either sees it (and rejects) or has already appended (and we drain it).
        {
            self.xlock.lock();
            defer self.xlock.unlock();
            self.closed = true;
        }
        // Drop pending callbacks/timers now (asyncio clears _ready/_scheduled on
        // close) so a closed loop with queued work can still be collected.
        self.dropPending();
    }
};

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

const testing = std.testing;

const TestDispatcher = struct {
    runs: std.ArrayList(usize) = .empty,
    allocator: std.mem.Allocator,

    fn run(ctx: *anyopaque, token: usize) void {
        const self: *TestDispatcher = @ptrCast(@alignCast(ctx));
        self.runs.append(self.allocator, token) catch unreachable;
    }
    fn drop(_: *anyopaque, _: usize) void {}
    fn dispatcher(self: *TestDispatcher) Dispatcher {
        return .{ .ctx = self, .run = run, .drop = drop };
    }
    fn deinit(self: *TestDispatcher) void {
        self.runs.deinit(self.allocator);
    }
};

const IoCounter = struct {
    reads: u32 = 0,
    writes: u32 = 0,
    fn cb(ctx: *anyopaque, ev: IoEvent) void {
        const self: *IoCounter = @ptrCast(@alignCast(ctx));
        if (ev.readable) self.reads += 1;
        if (ev.writable) self.writes += 1;
    }
    fn callback(self: *IoCounter) IoCallback {
        return .{ .func = cb, .ctx = self };
    }
};

test "call_soon runs in fifo on next iteration" {
    var td = TestDispatcher{ .allocator = testing.allocator };
    defer td.deinit();
    var loop = try Loop.init(testing.allocator, td.dispatcher());
    defer loop.deinit();

    try loop.callSoon(1);
    try loop.callSoon(2);
    try loop.runOnce(0);
    try testing.expectEqualSlices(usize, &.{ 1, 2 }, td.runs.items);
}

test "timer fires after deadline" {
    var td = TestDispatcher{ .allocator = testing.allocator };
    defer td.deinit();
    var loop = try Loop.init(testing.allocator, td.dispatcher());
    defer loop.deinit();

    _ = try loop.callAt(loop.now() + 5 * std.time.ns_per_ms, 42);
    try loop.runOnce(50 * std.time.ns_per_ms);
    try testing.expectEqualSlices(usize, &.{42}, td.runs.items);
}

test "reader fires on readable fd then removeReader stops it" {
    var td = TestDispatcher{ .allocator = testing.allocator };
    defer td.deinit();
    var loop = try Loop.init(testing.allocator, td.dispatcher());
    defer loop.deinit();

    const fds = try sys.pipe();
    defer sys.close(fds[0]);
    defer sys.close(fds[1]);

    var counter = IoCounter{};
    try loop.addReader(fds[0], counter.callback());
    _ = try sys.write(fds[1], "data");
    try loop.runOnce(10 * std.time.ns_per_ms);
    try testing.expectEqual(@as(u32, 1), counter.reads);

    try testing.expect(loop.removeReader(fds[0]));
    try testing.expect(!loop.removeReader(fds[0]));
}

test "writer fires on writable socket" {
    var td = TestDispatcher{ .allocator = testing.allocator };
    defer td.deinit();
    var loop = try Loop.init(testing.allocator, td.dispatcher());
    defer loop.deinit();

    const fds = try sys.socketPair();
    defer sys.close(fds[0]);
    defer sys.close(fds[1]);

    var counter = IoCounter{};
    try loop.addWriter(fds[0], counter.callback());
    try loop.runOnce(10 * std.time.ns_per_ms);
    try testing.expect(counter.writes >= 1);
}

test "wakeup interrupts an otherwise-blocking poll" {
    var td = TestDispatcher{ .allocator = testing.allocator };
    defer td.deinit();
    var loop = try Loop.init(testing.allocator, td.dispatcher());
    defer loop.deinit();

    loop.wakeup();
    try loop.runOnce(null);
    try testing.expectEqual(@as(usize, 0), td.runs.items.len);
}
