//! The event loop engine. It owns the reactor, the timer queue and the ready
//! FIFO, and implements the canonical asyncio "run once" iteration. It has no
//! knowledge of Python: callbacks are opaque `usize` tokens, and executing one
//! is delegated to a `Dispatcher` supplied by the embedder (dependency
//! inversion - the adapter layer plugs CPython in here).

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

/// The embedder plugs in how a token is executed and how a token is released
/// when it is dropped without running (e.g. a reader replaced, a cancelled
/// timer reclaimed). Both receive the embedder's `ctx`.
pub const Dispatcher = struct {
    ctx: *anyopaque,
    /// Run the callback identified by `token`.
    run: *const fn (ctx: *anyopaque, token: usize) void,
    /// The token will never run; release any resources it owns (e.g. decref a
    /// Python Handle). May be a no-op.
    drop: *const fn (ctx: *anyopaque, token: usize) void,
};

/// Reader and writer callbacks registered for one fd via add_reader/add_writer.
const FdState = struct {
    reader: ?usize = null,
    writer: ?usize = null,

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

    /// Self-pipe: writing a byte to `wake_w` makes a blocked poll return, so
    /// call_soon_threadsafe and signal delivery can interrupt the loop.
    wake_r: sys.fd_t,
    wake_w: sys.fd_t,
    /// Token used internally to mark the self-pipe readable; never dispatched.
    const WAKE_TOKEN: usize = std.math.maxInt(usize);

    pub fn init(allocator: std.mem.Allocator, dispatcher: Dispatcher) !Loop {
        var r = try Reactor.init(allocator);
        errdefer r.deinit();

        const pipe = try sys.pipe();
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
            .wake_r = pipe[0],
            .wake_w = pipe[1],
        };
    }

    pub fn deinit(self: *Loop) void {
        sys.close(self.wake_r);
        sys.close(self.wake_w);
        self.reactor.deinit();
        self.timers.deinit();
        self.ready.deinit();
        self.fds.deinit();
    }

    // -- scheduling -----------------------------------------------------------

    /// Schedule `token` to run on the next iteration.
    pub fn callSoon(self: *Loop, token: usize) !void {
        try self.ready.push(token);
    }

    /// Schedule `token` to run at absolute monotonic time `when_ns`. Returns the
    /// seq for cancellation.
    pub fn callAt(self: *Loop, when_ns: u64, token: usize) !u64 {
        return self.timers.push(when_ns, token);
    }

    pub fn cancelTimer(self: *Loop, token: usize, seq: u64) bool {
        return self.timers.cancel(token, seq);
    }

    pub fn now(_: *Loop) u64 {
        return clock.nowNs();
    }

    /// Wake a blocked poll from another thread.
    pub fn wakeup(self: *Loop) void {
        _ = sys.write(self.wake_w, "\x00") catch {};
    }

    // -- fd readiness ---------------------------------------------------------

    pub fn addReader(self: *Loop, fd: sys.fd_t, token: usize) !void {
        const gop = try self.fds.getOrPut(fd);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .reader = token };
            try self.reactor.register(fd, fd_token(fd), gop.value_ptr.interest());
        } else {
            if (gop.value_ptr.reader) |old| self.dispatcher.drop(self.dispatcher.ctx, old);
            gop.value_ptr.reader = token;
            try self.reactor.modify(fd, fd_token(fd), gop.value_ptr.interest());
        }
    }

    pub fn addWriter(self: *Loop, fd: sys.fd_t, token: usize) !void {
        const gop = try self.fds.getOrPut(fd);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .writer = token };
            try self.reactor.register(fd, fd_token(fd), gop.value_ptr.interest());
        } else {
            if (gop.value_ptr.writer) |old| self.dispatcher.drop(self.dispatcher.ctx, old);
            gop.value_ptr.writer = token;
            try self.reactor.modify(fd, fd_token(fd), gop.value_ptr.interest());
        }
    }

    /// Returns true if a reader was present and removed.
    pub fn removeReader(self: *Loop, fd: sys.fd_t) bool {
        const entry = self.fds.getEntry(fd) orelse return false;
        const had = entry.value_ptr.reader != null;
        if (entry.value_ptr.reader) |old| self.dispatcher.drop(self.dispatcher.ctx, old);
        entry.value_ptr.reader = null;
        self.syncFd(fd, entry.value_ptr.*);
        return had;
    }

    pub fn removeWriter(self: *Loop, fd: sys.fd_t) bool {
        const entry = self.fds.getEntry(fd) orelse return false;
        const had = entry.value_ptr.writer != null;
        if (entry.value_ptr.writer) |old| self.dispatcher.drop(self.dispatcher.ctx, old);
        entry.value_ptr.writer = null;
        self.syncFd(fd, entry.value_ptr.*);
        return had;
    }

    fn syncFd(self: *Loop, fd: sys.fd_t, state: FdState) void {
        if (state.isEmpty()) {
            self.reactor.unregister(fd) catch {};
            _ = self.fds.remove(fd);
        } else {
            self.reactor.modify(fd, fd_token(fd), state.interest()) catch {};
        }
    }

    // Reader/writer fds use the fd value (shifted) as their reactor token so
    // poll() events map straight back to the FdState. Callback tokens are the
    // separate reader/writer fields.
    fn fd_token(fd: sys.fd_t) usize {
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

    /// Run iterations until `stop()` is called.
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

    /// One iteration. `max_wait_ns` caps the poll wait even when a longer or
    /// indefinite wait would otherwise apply (used by the embedder to bound the
    /// loop for run_until_complete polling). null means no extra cap.
    pub fn runOnce(self: *Loop, max_wait_ns: ?u64) !void {
        const timeout = self.computeTimeout(max_wait_ns);

        var events: [256]reactor_mod.Event = undefined;
        const ready_events = try self.reactor.poll(&events, timeout);

        for (ready_events) |ev| {
            if (ev.token == WAKE_TOKEN) {
                self.drainWake();
                continue;
            }
            const fd: sys.fd_t = @intCast(ev.token);
            const state = self.fds.get(fd) orelse continue;
            // On hangup/error, fire both directions so reads see EOF and writes
            // see the error.
            if ((ev.readable or ev.hup)) if (state.reader) |tok| try self.ready.push(tok);
            if ((ev.writable or ev.hup)) if (state.writer) |tok| try self.ready.push(tok);
        }

        // Promote all due timers.
        const t_now = clock.nowNs();
        while (self.timers.popDue(t_now)) |timer| {
            try self.ready.push(timer.token);
        }

        // Run a snapshot of the ready queue; callbacks scheduled during this
        // drain wait until the next iteration (asyncio semantics).
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
        self.closed = true;
    }
};

// ---------------------------------------------------------------------------
// tests - exercise the engine with a Zig dispatcher counting executions.
// ---------------------------------------------------------------------------

const testing = std.testing;

const TestDispatcher = struct {
    runs: std.ArrayList(usize) = .empty,
    drops: std.ArrayList(usize) = .empty,
    allocator: std.mem.Allocator,

    fn run(ctx: *anyopaque, token: usize) void {
        const self: *TestDispatcher = @ptrCast(@alignCast(ctx));
        self.runs.append(self.allocator, token) catch unreachable;
    }
    fn drop(ctx: *anyopaque, token: usize) void {
        const self: *TestDispatcher = @ptrCast(@alignCast(ctx));
        self.drops.append(self.allocator, token) catch unreachable;
    }
    fn dispatcher(self: *TestDispatcher) Dispatcher {
        return .{ .ctx = self, .run = run, .drop = drop };
    }
    fn deinit(self: *TestDispatcher) void {
        self.runs.deinit(self.allocator);
        self.drops.deinit(self.allocator);
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
    try testing.expectEqual(@as(usize, 0), td.runs.items.len);
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

    try loop.addReader(fds[0], 0xBEEF);
    _ = try sys.write(fds[1], "data");
    try loop.runOnce(10 * std.time.ns_per_ms);
    try testing.expectEqualSlices(usize, &.{0xBEEF}, td.runs.items);

    try testing.expect(loop.removeReader(fds[0]));
    try testing.expect(!loop.removeReader(fds[0]));
}

test "wakeup interrupts an otherwise-blocking poll" {
    var td = TestDispatcher{ .allocator = testing.allocator };
    defer td.deinit();
    var loop = try Loop.init(testing.allocator, td.dispatcher());
    defer loop.deinit();

    // No timers, no ready work: poll would block forever. Pre-arm the wakeup so
    // the self-pipe is readable and runOnce returns promptly.
    loop.wakeup();
    try loop.runOnce(null);
    try testing.expectEqual(@as(usize, 0), td.runs.items.len);
}

test "replacing a reader drops the old token" {
    var td = TestDispatcher{ .allocator = testing.allocator };
    defer td.deinit();
    var loop = try Loop.init(testing.allocator, td.dispatcher());
    defer loop.deinit();
    const fds = try sys.pipe();
    defer sys.close(fds[0]);
    defer sys.close(fds[1]);

    try loop.addReader(fds[0], 100);
    try loop.addReader(fds[0], 200); // replaces 100
    try testing.expectEqualSlices(usize, &.{100}, td.drops.items);
}
