//! Completor: a true io_uring *completion* (Proactor) port — Phase 2.
//!
//! Where the Reactor answers "which fds are ready?" and the caller then does the
//! recv/send, the Completor submits the I/O itself and reaps results: the kernel
//! moves the bytes and posts a Completion carrying the byte count (or -errno).
//!
//! Reads use multishot recv drawing from a kernel-registered provided-buffer ring
//! (std's IoUring.BufferGroup): the kernel picks a buffer per arrival and returns
//! its id in the CQE; we build the PyBytes from it (with the GIL held) and recycle
//! the buffer. Writes are submitted as SEND and reaped on completion.
//!
//! This is a *sibling* of the Reactor, not a wider Reactor — readiness and
//! completion are different contracts. It is Linux-io_uring-only and opt-in via
//! ZLOOP_IO_URING=completion; epoll/kqueue stay the default.

const std = @import("std");
const c = std.c;
const linux = std.os.linux;
const IoUring = linux.IoUring;
const sys = @import("sys.zig");

/// True when ZLOOP_IO_URING=completion. Uses libc getenv (the module links libc).
pub fn enabled() bool {
    const raw = c.getenv("ZLOOP_IO_URING") orelse return false;
    const v = std.mem.sliceTo(raw, 0);
    return std.mem.eql(u8, v, "completion");
}

/// What kind of op a Completion is for. Encoded in the high bits of user_data so
/// reap can route a CQE without a side table, and so a stale CQE for a reused fd
/// is filtered by its generation (see UserData).
pub const OpKind = enum(u3) { recv = 0, send = 1, cancel = 2, wake = 3, poll = 4 };

// POLLIN/POLLOUT/HUP/ERR for POLL_ADD ops (accept, connect, signals - the fds
// that aren't plain-socket data and so use readiness-in-completion, not recv).
const POLLIN: u32 = 0x001;
const POLLOUT: u32 = 0x004;
const POLLERR: u32 = 0x008;
const POLLHUP: u32 = 0x010;

/// user_data layout: [ gen:29 | kind:3 | fd:32 ]. The generation is bumped per
/// registration so a late CQE from a closed-then-reused fd is dropped.
pub const UserData = struct {
    pub fn make(op_kind: OpKind, op_fd: sys.fd_t, op_gen: u32) u64 {
        const ufd: u64 = @as(u32, @bitCast(op_fd));
        const ukind: u64 = @intFromEnum(op_kind);
        const ugen: u64 = op_gen & 0x1FFF_FFFF;
        return (ugen << 35) | (ukind << 32) | ufd;
    }
    pub fn fd(ud: u64) sys.fd_t {
        return @bitCast(@as(u32, @truncate(ud)));
    }
    pub fn kind(ud: u64) OpKind {
        return @enumFromInt(@as(u3, @truncate(ud >> 32)));
    }
    pub fn gen(ud: u64) u32 {
        return @truncate(ud >> 35);
    }
};

/// A reaped completion handed back to the loop. `result` >= 0 is bytes moved,
/// < 0 is -errno. For recv, `buf` is the kernel-filled slice (valid until the
/// caller calls `recycle`); for send it is empty.
pub const Completion = struct {
    user_data: u64,
    result: isize,
    flags: u32,
    buf: []const u8 = &.{},
    buf_id: ?u16 = null,
    // For poll (readiness) completions:
    readable: bool = false,
    writable: bool = false,
    hup: bool = false,
};

pub const WAKE_UD: u64 = std.math.maxInt(u64);
/// Re-export so the loop can test multishot continuation without importing linux.
pub const F_MORE: u32 = linux.IORING_CQE_F_MORE;

const RECV_GROUP: u16 = 1;
const RECV_BUF_SIZE: u32 = 64 * 1024;
const RECV_BUF_COUNT: u16 = 1024;
const QUEUE_DEPTH: u16 = 256;

pub const Completor = struct {
    ring: IoUring,
    bufs: IoUring.BufferGroup,
    allocator: std.mem.Allocator,

    /// Initialise `self` in place. The Completor is self-referential -
    /// BufferGroup stores `ring: *IoUring`, so the ring must live at its final
    /// address before the buffer group is built. Returning by value would copy
    /// the ring and leave bufs.ring dangling (a recv would then hang on a stale
    /// ring), so the caller must give us our final location.
    pub fn init(self: *Completor, allocator: std.mem.Allocator) !void {
        // Plain ring. (SINGLE_ISSUER/DEFER_TASKRUN would fit a single-threaded
        // loop, but DEFER_TASKRUN defers completion processing to GETEVENTS-only
        // and complicates the reap path; revisit as a perf tweak once correct.)
        self.ring = IoUring.init(QUEUE_DEPTH, 0) catch return error.CompletorInitFailed;
        errdefer self.ring.deinit();
        self.allocator = allocator;
        // Provided-buffer ring (needs kernel 5.19+) built against our OWN ring.
        // If unsupported, the caller falls back to the readiness Reactor.
        self.bufs = IoUring.BufferGroup.init(&self.ring, allocator, RECV_GROUP, RECV_BUF_SIZE, RECV_BUF_COUNT) catch
            return error.CompletorBufRingUnsupported;
    }

    pub fn deinit(self: *Completor) void {
        self.bufs.deinit(self.allocator);
        self.ring.deinit();
    }

    /// Ensure a free SQE is available, flushing the queue to the kernel first if
    /// it is full. Ops are otherwise queued and submitted in one batch by reap,
    /// so a busy loop turn costs a single io_uring_enter instead of one per op.
    fn ensureSqe(self: *Completor) !*linux.io_uring_sqe {
        return self.ring.get_sqe() catch {
            _ = self.ring.submit() catch return error.SubmitFailed;
            return self.ring.get_sqe() catch return error.SubmitFailed;
        };
    }

    /// Queue a single-shot recv for `fd` drawing from the provided-buffer ring.
    /// The transport re-submits after each completion (one recv in flight per fd).
    /// Single-shot (not multishot) for delivery predictability across kernels;
    /// multishot can be a later optimization. Not submitted here - reap flushes it.
    pub fn submitRecv(self: *Completor, fd: sys.fd_t, ud: u64) !void {
        const sqe = try self.ensureSqe();
        sqe.prep_rw(.RECV, fd, 0, 0, 0);
        sqe.rw_flags = 0;
        sqe.flags |= linux.IOSQE_BUFFER_SELECT;
        sqe.buf_index = self.bufs.group_id;
        sqe.user_data = ud;
    }

    /// Queue a multishot POLL_ADD for readiness (accept/connect/signals - fds
    /// that aren't plain-socket data). Re-arms until F_MORE clears.
    pub fn submitPoll(self: *Completor, fd: sys.fd_t, want_read: bool, want_write: bool, ud: u64) !void {
        var mask: u32 = 0;
        if (want_read) mask |= POLLIN;
        if (want_write) mask |= POLLOUT;
        const sqe = try self.ensureSqe();
        sqe.prep_poll_add(fd, mask);
        sqe.len |= linux.IORING_POLL_ADD_MULTI;
        sqe.user_data = ud;
    }

    /// Queue a send of `buf` (borrowed by the kernel until the send completes —
    /// the caller MUST keep it alive and unmodified until the matching reap).
    pub fn submitSend(self: *Completor, fd: sys.fd_t, buf: []const u8, ud: u64) !void {
        const sqe = try self.ensureSqe();
        sqe.prep_send(fd, buf, 0);
        sqe.user_data = ud;
    }

    /// Queue a cancel of every outstanding op tagged with `target_ud`. The
    /// cancelled ops complete with -ECANCELED, reaped like any other.
    pub fn cancel(self: *Completor, target_ud: u64, ud: u64) void {
        const sqe = self.ensureSqe() catch return;
        sqe.prep_cancel(target_ud, 0);
        sqe.user_data = ud;
    }

    /// Recycle a recv buffer (by its completion) back to the kernel ring.
    pub fn recycle(self: *Completor, cqe_flags: u32, buf_id: u16, used: usize) void {
        const cqe = linux.io_uring_cqe{ .user_data = 0, .res = @intCast(used), .flags = cqe_flags | (@as(u32, buf_id) << linux.IORING_CQE_BUFFER_SHIFT) };
        self.bufs.put(cqe) catch {};
    }

    /// Block until at least one completion (or timeout), then drain ready CQEs
    /// into `out`. `timeout_ns`: null=block forever, 0=don't block, n=up to n ns.
    pub fn reap(self: *Completor, out: []Completion, timeout_ns: ?u64) ![]Completion {
        var ts: linux.kernel_timespec = undefined;
        const wait_nr: u32 = if (timeout_ns) |ns| blk: {
            if (ns == 0) break :blk 0;
            ts = .{ .sec = @intCast(ns / std.time.ns_per_s), .nsec = @intCast(ns % std.time.ns_per_s) };
            // Queue the wait-bounding timeout alongside whatever ops accumulated
            // this turn; submit_and_wait below flushes them all in one enter.
            if (self.ensureSqe()) |sqe| {
                sqe.prep_timeout(&ts, 0, 0);
                sqe.user_data = WAKE_UD;
            } else |_| {}
            break :blk 1;
        } else 1;

        _ = self.ring.submit_and_wait(wait_nr) catch |err| switch (err) {
            error.SignalInterrupt => return out[0..0],
            else => return error.ReapFailed,
        };

        var cqes: [256]linux.io_uring_cqe = undefined;
        const max = @min(out.len, cqes.len);
        const n = self.ring.copy_cqes(cqes[0..max], 0) catch return error.ReapFailed;

        var count: usize = 0;
        for (cqes[0..n]) |cqe| {
            if (cqe.user_data == WAKE_UD) continue; // our timeout op
            var comp = Completion{ .user_data = cqe.user_data, .result = cqe.res, .flags = cqe.flags };
            const knd = UserData.kind(cqe.user_data);
            if (knd == .recv and cqe.res > 0 and (cqe.flags & linux.IORING_CQE_F_BUFFER) != 0) {
                const bid = cqe.buffer_id() catch null;
                if (bid) |id| {
                    comp.buf = self.bufs.get_by_id(id)[0..@intCast(cqe.res)];
                    comp.buf_id = id;
                }
            } else if (knd == .poll and cqe.res >= 0) {
                const e: u32 = @intCast(cqe.res);
                comp.hup = (e & (POLLHUP | POLLERR)) != 0;
                comp.readable = (e & POLLIN) != 0 or comp.hup;
                comp.writable = (e & POLLOUT) != 0;
            }
            out[count] = comp;
            count += 1;
        }
        return out[0..count];
    }
};

// ---------------------------------------------------------------------------
// tests (Linux only; skip if io_uring / buffer rings unavailable on the kernel)
// ---------------------------------------------------------------------------

const testing = std.testing;

test "completor: buffer-ring recv delivers kernel-filled bytes" {
    var co: Completor = undefined;
    co.init(testing.allocator) catch return; // skip on old kernels
    defer co.deinit();

    const fds = try sys.socketPair();
    defer sys.close(fds[0]);
    defer sys.close(fds[1]);

    // Data first (matches the proven std BufferGroup ordering), then recv.
    _ = try sys.write(fds[1], "hello");

    const ud = UserData.make(.recv, fds[0], 1);
    try co.submitRecv(fds[0], ud);

    var out: [8]Completion = undefined;
    const comps = try co.reap(&out, null);
    try testing.expect(comps.len >= 1);
    try testing.expectEqual(ud, comps[0].user_data);
    try testing.expectEqual(@as(isize, 5), comps[0].result);
    try testing.expectEqualStrings("hello", comps[0].buf);
    if (comps[0].buf_id) |id| co.recycle(comps[0].flags, id, 5);
}

test "completor: send completes with the byte count" {
    var co: Completor = undefined;
    co.init(testing.allocator) catch return;
    defer co.deinit();

    const fds = try sys.socketPair();
    defer sys.close(fds[0]);
    defer sys.close(fds[1]);

    const ud = UserData.make(.send, fds[0], 1);
    try co.submitSend(fds[0], "world!", ud);

    var out: [8]Completion = undefined;
    const comps = try co.reap(&out, std.time.ns_per_s);
    try testing.expect(comps.len >= 1);
    try testing.expectEqual(ud, comps[0].user_data);
    try testing.expectEqual(@as(isize, 6), comps[0].result);

    var buf: [16]u8 = undefined;
    const n = try sys.read(fds[1], &buf);
    try testing.expectEqualStrings("world!", buf[0..n]);
}

test "UserData packs and unpacks kind/fd/gen" {
    const ud = UserData.make(.send, 4242, 7);
    try testing.expectEqual(OpKind.send, UserData.kind(ud));
    try testing.expectEqual(@as(sys.fd_t, 4242), UserData.fd(ud));
    try testing.expectEqual(@as(u32, 7), UserData.gen(ud));
}
