//! UringPollReactor: an io_uring backend that implements the same readiness
//! contract as the kqueue/epoll Reactor (register/modify/unregister/poll ->
//! Event{token, readable, writable, hup}) using multishot IORING_OP_POLL_ADD.
//!
//! This is Phase 1 of the completion-model roadmap: io_uring's POLL_ADD is a
//! readiness op *inside* the completion model, so it produces exactly what
//! epoll_wait produces while the loop and transports keep doing their own
//! read/write syscalls. It is opt-in (ZLOOP_IO_URING=1); epoll stays the default
//! until this is proven faster.

const std = @import("std");
const c = std.c;
const linux = std.os.linux;
const IoUring = linux.IoUring;
const sys = @import("sys.zig");
const reactor = @import("reactor.zig");

/// True when ZLOOP_IO_URING is set to "1"/"true". Uses libc getenv (the module
/// links libc) so it works without a std.process allocator.
pub fn enabled() bool {
    const raw = c.getenv("ZLOOP_IO_URING") orelse return false;
    const v = std.mem.sliceTo(raw, 0);
    return std.mem.eql(u8, v, "1") or std.mem.eql(u8, v, "true");
}

const Interest = reactor.Interest;
const Event = reactor.Event;

// user_data carries the fd in the low bits; a tag bit distinguishes a poll
// arm-completion from a poll-remove acknowledgement (whose CQE we ignore).
const REMOVE_TAG: u64 = 1 << 40;

// POLLRDHUP (peer half-close) is not in std's POLL struct; same value as on epoll.
// It must be explicitly requested, and lets a clean half-close surface as a
// readable hangup so the pending read runs and observes EOF - matching the epoll
// backend, which sets EPOLLRDHUP for the same reason.
const POLLRDHUP: u32 = 0x2000;

fn pollMask(interest: Interest) u32 {
    if (interest.isEmpty()) return 0;
    var m: u32 = POLLRDHUP;
    if (interest.read) m |= linux.POLL.IN;
    if (interest.write) m |= linux.POLL.OUT;
    return m;
}

pub const UringPollReactor = struct {
    ring: IoUring,
    interest: std.AutoHashMap(sys.fd_t, Entry),
    allocator: std.mem.Allocator,

    const Entry = struct { token: usize, interest: Interest };
    const ENTRIES: u16 = 256;

    pub fn init(allocator: std.mem.Allocator) !UringPollReactor {
        // SINGLE_ISSUER/DEFER_TASKRUN fit a single-threaded loop; fall back to a
        // plain ring on kernels that reject the flags.
        const flags = linux.IORING_SETUP_SINGLE_ISSUER | linux.IORING_SETUP_DEFER_TASKRUN;
        const ring = IoUring.init(ENTRIES, flags) catch
            IoUring.init(ENTRIES, 0) catch return error.ReactorInitFailed;
        return .{
            .ring = ring,
            .interest = std.AutoHashMap(sys.fd_t, Entry).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *UringPollReactor) void {
        self.ring.deinit();
        self.interest.deinit();
    }

    fn arm(self: *UringPollReactor, fd: sys.fd_t, interest: Interest) !void {
        const sqe = self.ring.poll_add(@intCast(fd), fd, pollMask(interest)) catch
            return error.RegisterFailed;
        sqe.len |= linux.IORING_POLL_ADD_MULTI; // multishot: re-fires until removed
        _ = self.ring.submit() catch return error.RegisterFailed;
    }

    fn disarm(self: *UringPollReactor, fd: sys.fd_t) void {
        const sqe = self.ring.poll_remove(REMOVE_TAG | @as(u64, @intCast(fd)), @intCast(fd)) catch return;
        _ = sqe;
        _ = self.ring.submit() catch {};
    }

    pub fn register(self: *UringPollReactor, fd: sys.fd_t, token: usize, interest: Interest) !void {
        if (self.interest.contains(fd)) return error.AlreadyRegistered;
        if (!interest.isEmpty()) try self.arm(fd, interest);
        try self.interest.put(fd, .{ .token = token, .interest = interest });
    }

    pub fn modify(self: *UringPollReactor, fd: sys.fd_t, token: usize, interest: Interest) !void {
        const entry = self.interest.getEntry(fd) orelse return error.NotRegistered;
        const old = entry.value_ptr.interest;
        if (!old.isEmpty()) self.disarm(fd);
        if (!interest.isEmpty()) try self.arm(fd, interest);
        entry.value_ptr.* = .{ .token = token, .interest = interest };
    }

    pub fn unregister(self: *UringPollReactor, fd: sys.fd_t) !void {
        const entry = self.interest.fetchRemove(fd) orelse return error.NotRegistered;
        if (!entry.value.interest.isEmpty()) self.disarm(fd);
    }

    pub fn poll(self: *UringPollReactor, out: []Event, timeout_ns: ?u64) ![]Event {
        // A timeout is expressed by submitting a timeout op alongside the wait.
        // 0ns means "don't block"; null means "block forever".
        var ts: linux.kernel_timespec = undefined;
        const wait_nr: u32 = if (timeout_ns) |ns| blk: {
            if (ns == 0) break :blk 0;
            ts = .{ .sec = @intCast(ns / std.time.ns_per_s), .nsec = @intCast(ns % std.time.ns_per_s) };
            _ = self.ring.timeout(REMOVE_TAG, &ts, 0, 0) catch {};
            break :blk 1;
        } else 1;

        _ = self.ring.submit_and_wait(wait_nr) catch |err| switch (err) {
            error.SignalInterrupt => return out[0..0],
            else => return error.PollFailed,
        };

        var cqes: [256]linux.io_uring_cqe = undefined;
        const max = @min(out.len, cqes.len);
        const n = self.ring.copy_cqes(cqes[0..max], 0) catch return error.PollFailed;

        var count: usize = 0;
        for (cqes[0..n]) |cqe| {
            const ud = cqe.user_data;
            if (ud == REMOVE_TAG) continue; // our timeout op
            if (ud & REMOVE_TAG != 0) continue; // a poll-remove acknowledgement
            const fd: sys.fd_t = @intCast(ud);
            const entry = self.interest.get(fd) orelse continue; // stale CQE for a gone fd
            if (cqe.res < 0) continue; // -ECANCELED etc.

            const e: u32 = @intCast(cqe.res);
            const hup = (e & (linux.POLL.HUP | linux.POLL.ERR | POLLRDHUP)) != 0;
            out[count] = .{
                .token = entry.token,
                .readable = (e & linux.POLL.IN) != 0 or hup,
                .writable = (e & linux.POLL.OUT) != 0,
                .hup = hup,
            };
            count += 1;

            // Multishot keeps firing while F_MORE is set; re-arm if it stopped
            // and we still want this fd.
            if (cqe.flags & linux.IORING_CQE_F_MORE == 0 and !entry.interest.isEmpty()) {
                self.arm(fd, entry.interest) catch {};
            }
        }
        return out[0..count];
    }
};

// ---------------------------------------------------------------------------
// tests (Linux only; the same contract as the epoll/kqueue reactor tests)
// ---------------------------------------------------------------------------

const testing = std.testing;

test "uring: register, poll readable on a pipe" {
    var r = UringPollReactor.init(testing.allocator) catch return; // skip if io_uring unavailable
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

test "uring: writable readiness and modify to drop interest" {
    var r = UringPollReactor.init(testing.allocator) catch return;
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

test "uring: unregister removes interest" {
    var r = UringPollReactor.init(testing.allocator) catch return;
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
