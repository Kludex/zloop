//! The Reactor: a backend-agnostic event demultiplexer over the platform's
//! readiness API (kqueue on macOS/BSD, epoll on Linux). It maps file
//! descriptors to a caller-supplied opaque token and reports which become
//! ready for reading and/or writing. It knows nothing of callbacks, Python, or
//! the event loop - it is a pure I/O notification primitive.

const std = @import("std");
const builtin = @import("builtin");
const c = std.c;
const sys = @import("sys.zig");

/// What a caller is interested in for a given fd.
pub const Interest = packed struct {
    read: bool = false,
    write: bool = false,

    pub fn isEmpty(self: Interest) bool {
        return !self.read and !self.write;
    }

    pub fn eql(self: Interest, other: Interest) bool {
        return self.read == other.read and self.write == other.write;
    }
};

/// A readiness notification for one fd.
pub const Event = struct {
    token: usize,
    readable: bool,
    writable: bool,
    /// Peer hangup or error; the loop should let pending reads/writes observe
    /// the EOF/error rather than treating it as spurious.
    hup: bool,
};

pub const Reactor = switch (builtin.os.tag) {
    .macos, .freebsd, .netbsd, .openbsd, .dragonfly => KqueueReactor,
    .linux => EpollReactor,
    else => @compileError("zloop: unsupported platform for reactor"),
};

// ---------------------------------------------------------------------------
// kqueue backend (macOS / BSD)
// ---------------------------------------------------------------------------

const KqueueReactor = struct {
    kq: sys.fd_t,
    /// Changes batched until the next poll, applied atomically by kevent.
    changes: std.ArrayList(c.Kevent),
    /// Current interest per fd, so modify() can emit DELETE for dropped filters.
    interest: std.AutoHashMap(sys.fd_t, Interest),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !KqueueReactor {
        const kq = c.kqueue();
        if (kq < 0) return error.ReactorInitFailed;
        try sys.setCloexec(kq);
        return .{
            .kq = kq,
            .changes = .empty,
            .interest = std.AutoHashMap(sys.fd_t, Interest).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *KqueueReactor) void {
        sys.close(self.kq);
        self.changes.deinit(self.allocator);
        self.interest.deinit();
    }

    pub fn register(self: *KqueueReactor, fd: sys.fd_t, token: usize, interest: Interest) !void {
        if (self.interest.contains(fd)) return error.AlreadyRegistered;
        try self.interest.put(fd, interest);
        try self.applyDiff(fd, token, .{}, interest);
    }

    pub fn modify(self: *KqueueReactor, fd: sys.fd_t, token: usize, interest: Interest) !void {
        const entry = self.interest.getEntry(fd) orelse return error.NotRegistered;
        const old = entry.value_ptr.*;
        entry.value_ptr.* = interest;
        try self.applyDiff(fd, token, old, interest);
    }

    pub fn unregister(self: *KqueueReactor, fd: sys.fd_t) !void {
        const old = self.interest.fetchRemove(fd) orelse return error.NotRegistered;
        try self.applyDiff(fd, 0, old.value, .{});
    }

    fn applyDiff(self: *KqueueReactor, fd: sys.fd_t, token: usize, old: Interest, new: Interest) !void {
        if (old.read != new.read) try self.pushChange(fd, token, c.EVFILT.READ, new.read);
        if (old.write != new.write) try self.pushChange(fd, token, c.EVFILT.WRITE, new.write);
    }

    fn pushChange(self: *KqueueReactor, fd: sys.fd_t, token: usize, filter: i16, enable: bool) !void {
        const flags: u16 = if (enable)
            c.EV.ADD | c.EV.ENABLE
        else
            c.EV.DELETE;
        try self.changes.append(self.allocator, .{
            .ident = @intCast(fd),
            .filter = filter,
            .flags = flags,
            .fflags = 0,
            .data = 0,
            .udata = token,
        });
    }

    /// Block until at least one fd is ready or `timeout_ns` elapses (null =
    /// forever, 0 = poll).
    pub fn poll(self: *KqueueReactor, out: []Event, timeout_ns: ?u64) ![]Event {
        var events: [256]c.Kevent = undefined;
        const max = @min(out.len, events.len);

        var ts: c.timespec = undefined;
        const ts_ptr: ?*const c.timespec = if (timeout_ns) |ns| blk: {
            ts = .{ .sec = @intCast(ns / std.time.ns_per_s), .nsec = @intCast(ns % std.time.ns_per_s) };
            break :blk &ts;
        } else null;

        while (true) {
            const n = c.kevent(
                self.kq,
                self.changes.items.ptr,
                @intCast(self.changes.items.len),
                &events,
                @intCast(max),
                ts_ptr,
            );
            if (n < 0) {
                const e: c.E = @enumFromInt(c._errno().*);
                if (e == .INTR) {
                    // changes were already consumed by the kernel before EINTR
                    self.changes.clearRetainingCapacity();
                    return out[0..0];
                }
                return error.PollFailed;
            }
            self.changes.clearRetainingCapacity();

            var count: usize = 0;
            for (events[0..@intCast(n)]) |ev| {
                const is_eof = (ev.flags & c.EV.EOF) != 0;
                const is_err = (ev.flags & c.EV.ERROR) != 0;
                // A stale DELETE for a closed fd surfaces as EV_ERROR with
                // ENOENT; skip those rather than reporting a phantom event.
                if (is_err and ev.data != 0 and !self.interest.contains(@intCast(ev.ident))) continue;
                out[count] = .{
                    .token = ev.udata,
                    .readable = ev.filter == c.EVFILT.READ,
                    .writable = ev.filter == c.EVFILT.WRITE,
                    .hup = is_eof or is_err,
                };
                count += 1;
            }
            return out[0..count];
        }
    }
};

// ---------------------------------------------------------------------------
// epoll backend (Linux)
// ---------------------------------------------------------------------------

const EpollReactor = struct {
    epfd: sys.fd_t,
    interest: std.AutoHashMap(sys.fd_t, TokenInterest),
    allocator: std.mem.Allocator,

    const TokenInterest = struct { token: usize, interest: Interest };
    // std.c exposes the epoll_* libc functions and epoll_event, but not the EPOLL
    // constants struct; take those from std.os.linux (the functions stay libc).
    const EPOLL = std.os.linux.EPOLL;

    pub fn init(allocator: std.mem.Allocator) !EpollReactor {
        const epfd = c.epoll_create1(EPOLL.CLOEXEC);
        if (epfd < 0) return error.ReactorInitFailed;
        return .{
            .epfd = epfd,
            .interest = std.AutoHashMap(sys.fd_t, TokenInterest).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *EpollReactor) void {
        sys.close(self.epfd);
        self.interest.deinit();
    }

    fn maskOf(interest: Interest) u32 {
        if (interest.isEmpty()) return 0; // no interest -> no RDHUP, matching kqueue
        var m: u32 = EPOLL.RDHUP;
        if (interest.read) m |= EPOLL.IN;
        if (interest.write) m |= EPOLL.OUT;
        return m;
    }

    pub fn register(self: *EpollReactor, fd: sys.fd_t, token: usize, interest: Interest) !void {
        if (self.interest.contains(fd)) return error.AlreadyRegistered;
        var ev = c.epoll_event{ .events = maskOf(interest), .data = .{ .u64 = token } };
        if (c.epoll_ctl(self.epfd, EPOLL.CTL_ADD, fd, &ev) != 0) return error.RegisterFailed;
        try self.interest.put(fd, .{ .token = token, .interest = interest });
    }

    pub fn modify(self: *EpollReactor, fd: sys.fd_t, token: usize, interest: Interest) !void {
        const entry = self.interest.getEntry(fd) orelse return error.NotRegistered;
        var ev = c.epoll_event{ .events = maskOf(interest), .data = .{ .u64 = token } };
        if (c.epoll_ctl(self.epfd, EPOLL.CTL_MOD, fd, &ev) != 0) return error.ModifyFailed;
        entry.value_ptr.* = .{ .token = token, .interest = interest };
    }

    pub fn unregister(self: *EpollReactor, fd: sys.fd_t) !void {
        if (!self.interest.remove(fd)) return error.NotRegistered;
        _ = c.epoll_ctl(self.epfd, EPOLL.CTL_DEL, fd, null); // ignore: fd may be closed
    }

    pub fn poll(self: *EpollReactor, out: []Event, timeout_ns: ?u64) ![]Event {
        var events: [256]c.epoll_event = undefined;
        const max = @min(out.len, events.len);
        // Round sub-millisecond waits UP to 1ms (epoll's granularity) so a tiny
        // timer doesn't degrade into a zero-timeout busy-poll; asyncio does the
        // same. A literal 0ns stays 0 (intentional poll).
        const timeout_ms: i32 = if (timeout_ns) |ns| blk: {
            if (ns == 0) break :blk 0;
            const ms = (ns + std.time.ns_per_ms - 1) / std.time.ns_per_ms;
            break :blk @intCast(@min(ms, std.math.maxInt(i32)));
        } else -1;

        const n = c.epoll_wait(self.epfd, &events, @intCast(max), timeout_ms);
        if (n < 0) {
            const e: c.E = @enumFromInt(c._errno().*);
            if (e == .INTR) return out[0..0];
            return error.PollFailed;
        }
        var count: usize = 0;
        for (events[0..@intCast(n)]) |ev| {
            const e = ev.events;
            const hup = (e & (EPOLL.HUP | EPOLL.ERR | EPOLL.RDHUP)) != 0;
            out[count] = .{
                .token = ev.data.u64,
                .readable = (e & EPOLL.IN) != 0 or hup,
                .writable = (e & EPOLL.OUT) != 0,
                .hup = hup,
            };
            count += 1;
        }
        return out[0..count];
    }
};

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

const testing = std.testing;

test "register, poll readable on a pipe" {
    var r = try Reactor.init(testing.allocator);
    defer r.deinit();

    const fds = try sys.pipe();
    defer sys.close(fds[0]);
    defer sys.close(fds[1]);

    try r.register(fds[0], 0xABCD, .{ .read = true });

    var buf: [8]Event = undefined;
    var ready = try r.poll(&buf, 0);
    try testing.expectEqual(@as(usize, 0), ready.len);

    _ = try sys.write(fds[1], "hi");
    ready = try r.poll(&buf, std.time.ns_per_s);
    try testing.expectEqual(@as(usize, 1), ready.len);
    try testing.expectEqual(@as(usize, 0xABCD), ready[0].token);
    try testing.expect(ready[0].readable);
}

test "writable readiness and modify to drop interest" {
    var r = try Reactor.init(testing.allocator);
    defer r.deinit();

    const fds = try sys.socketPair();
    defer sys.close(fds[0]);
    defer sys.close(fds[1]);

    try r.register(fds[0], 7, .{ .write = true });
    var buf: [8]Event = undefined;
    var ready = try r.poll(&buf, std.time.ns_per_s);
    try testing.expect(ready.len >= 1);
    try testing.expect(ready[0].writable);

    try r.modify(fds[0], 7, .{});
    ready = try r.poll(&buf, 0);
    try testing.expectEqual(@as(usize, 0), ready.len);
}

test "unregister removes interest" {
    var r = try Reactor.init(testing.allocator);
    defer r.deinit();
    const fds = try sys.pipe();
    defer sys.close(fds[0]);
    defer sys.close(fds[1]);

    try r.register(fds[0], 1, .{ .read = true });
    try r.unregister(fds[0]);
    try testing.expectError(error.NotRegistered, r.unregister(fds[0]));

    _ = try sys.write(fds[1], "x");
    var buf: [8]Event = undefined;
    const ready = try r.poll(&buf, 0);
    try testing.expectEqual(@as(usize, 0), ready.len);
}

test "double register is rejected" {
    var r = try Reactor.init(testing.allocator);
    defer r.deinit();
    const fds = try sys.pipe();
    defer sys.close(fds[0]);
    defer sys.close(fds[1]);
    try r.register(fds[0], 1, .{ .read = true });
    try testing.expectError(error.AlreadyRegistered, r.register(fds[0], 2, .{ .read = true }));
}
