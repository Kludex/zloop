//! Thin, ergonomic wrappers over libc syscalls. This is the single place that
//! talks to the operating system; everything above it is OS-agnostic. Zig
//! 0.16's std.posix no longer wraps sockets/epoll/kqueue, so we bind libc
//! directly through std.c and translate errno into a flat Zig error set.

const std = @import("std");
const builtin = @import("builtin");
const c = std.c;

pub const fd_t = c.fd_t;
pub const socklen_t = c.socklen_t;
pub const sockaddr = c.sockaddr;

pub const SysError = error{
    WouldBlock,
    Interrupted,
    ConnectionReset,
    ConnectionRefused,
    ConnectionAborted,
    BrokenPipe,
    InProgress,
    AddressInUse,
    AddressNotAvailable,
    PermissionDenied,
    BadFileDescriptor,
    InvalidArgument,
    NoBufferSpace,
    NotConnected,
    TimedOut,
    NetworkUnreachable,
    HostUnreachable,
    TooManyOpenFiles,
    Again,
    Unexpected,
};

fn errnoToError(e: c.E) SysError {
    return switch (e) {
        .AGAIN => SysError.WouldBlock,
        .INTR => SysError.Interrupted,
        .CONNRESET => SysError.ConnectionReset,
        .CONNREFUSED => SysError.ConnectionRefused,
        .CONNABORTED => SysError.ConnectionAborted,
        .PIPE => SysError.BrokenPipe,
        .INPROGRESS => SysError.InProgress,
        .ADDRINUSE => SysError.AddressInUse,
        .ADDRNOTAVAIL => SysError.AddressNotAvailable,
        .ACCES, .PERM => SysError.PermissionDenied,
        .BADF => SysError.BadFileDescriptor,
        .INVAL => SysError.InvalidArgument,
        .NOBUFS, .NOMEM => SysError.NoBufferSpace,
        .NOTCONN => SysError.NotConnected,
        .TIMEDOUT => SysError.TimedOut,
        .NETUNREACH => SysError.NetworkUnreachable,
        .HOSTUNREACH => SysError.HostUnreachable,
        .MFILE, .NFILE => SysError.TooManyOpenFiles,
        else => SysError.Unexpected,
    };
}

fn lastErrno() c.E {
    return @enumFromInt(c._errno().*);
}

// ---------------------------------------------------------------------------
// constants re-exported for callers
// ---------------------------------------------------------------------------

pub const AF = c.AF;
pub const SOCK = c.SOCK;
pub const SOL = c.SOL;
pub const SO = c.SO;
pub const IPPROTO = c.IPPROTO;

// ---------------------------------------------------------------------------
// file descriptors
// ---------------------------------------------------------------------------

pub fn close(fd: fd_t) void {
    _ = c.close(fd);
}

pub fn read(fd: fd_t, buf: []u8) SysError!usize {
    while (true) {
        const n = c.read(fd, buf.ptr, buf.len);
        if (n < 0) {
            const e = lastErrno();
            if (e == .INTR) continue;
            return errnoToError(e);
        }
        return @intCast(n);
    }
}

pub fn write(fd: fd_t, buf: []const u8) SysError!usize {
    while (true) {
        const n = c.write(fd, buf.ptr, buf.len);
        if (n < 0) {
            const e = lastErrno();
            if (e == .INTR) continue;
            return errnoToError(e);
        }
        return @intCast(n);
    }
}

pub fn pipe() SysError![2]fd_t {
    var fds: [2]fd_t = undefined;
    if (c.pipe(&fds) != 0) return errnoToError(lastErrno());
    return fds;
}

extern "c" fn socketpair(domain: c_int, sock_type: c_int, protocol: c_int, fds: *[2]fd_t) c_int;

pub fn socketPair() SysError![2]fd_t {
    var fds: [2]fd_t = undefined;
    if (socketpair(AF.UNIX, SOCK.STREAM, 0, &fds) != 0) return errnoToError(lastErrno());
    return fds;
}

pub fn setNonBlocking(fd: fd_t) SysError!void {
    const flags = c.fcntl(fd, c.F.GETFL, @as(c_int, 0));
    if (flags < 0) return errnoToError(lastErrno());
    const nonblock: u32 = @bitCast(@as(c.O, .{ .NONBLOCK = true }));
    if (c.fcntl(fd, c.F.SETFL, flags | @as(c_int, @bitCast(nonblock))) < 0) return errnoToError(lastErrno());
}

pub fn setCloexec(fd: fd_t) SysError!void {
    const flags = c.fcntl(fd, c.F.GETFD, @as(c_int, 0));
    if (flags < 0) return errnoToError(lastErrno());
    if (c.fcntl(fd, c.F.SETFD, flags | c.FD_CLOEXEC) < 0) return errnoToError(lastErrno());
}

// ---------------------------------------------------------------------------
// sockets
// ---------------------------------------------------------------------------

pub fn socket(domain: u32, sock_type: u32, protocol: u32) SysError!fd_t {
    const fd = c.socket(@intCast(domain), @intCast(sock_type), @intCast(protocol));
    if (fd < 0) return errnoToError(lastErrno());
    return fd;
}

pub fn setReuseAddr(fd: fd_t) SysError!void {
    const one: c_int = 1;
    if (c.setsockopt(fd, SOL.SOCKET, SO.REUSEADDR, &one, @sizeOf(c_int)) != 0)
        return errnoToError(lastErrno());
}

pub fn bind(fd: fd_t, addr: *const sockaddr, len: socklen_t) SysError!void {
    if (c.bind(fd, addr, len) != 0) return errnoToError(lastErrno());
}

pub fn listen(fd: fd_t, backlog: u31) SysError!void {
    if (c.listen(fd, backlog) != 0) return errnoToError(lastErrno());
}

pub fn accept(fd: fd_t, addr: ?*sockaddr, len: ?*socklen_t) SysError!fd_t {
    while (true) {
        const conn = c.accept(fd, addr, len);
        if (conn < 0) {
            const e = lastErrno();
            if (e == .INTR) continue;
            return errnoToError(e);
        }
        return conn;
    }
}

pub fn getsockname(fd: fd_t, addr: *sockaddr, len: *socklen_t) SysError!void {
    if (c.getsockname(fd, addr, len) != 0) return errnoToError(lastErrno());
}

pub fn shutdown(fd: fd_t, how: c_int) void {
    _ = c.shutdown(fd, how);
}

// ---------------------------------------------------------------------------
// monotonic clock (matches Python's time.monotonic / CLOCK_MONOTONIC)
// ---------------------------------------------------------------------------

pub fn monotonicNs() u64 {
    var ts: c.timespec = undefined;
    _ = c.clock_gettime(c.CLOCK.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

const testing = std.testing;

test "pipe read/write roundtrip" {
    const fds = try pipe();
    defer close(fds[0]);
    defer close(fds[1]);
    const n = try write(fds[1], "hello");
    try testing.expectEqual(@as(usize, 5), n);
    var buf: [16]u8 = undefined;
    const r = try read(fds[0], buf[0..]);
    try testing.expectEqualStrings("hello", buf[0..r]);
}

test "nonblocking read yields WouldBlock" {
    const fds = try pipe();
    defer close(fds[0]);
    defer close(fds[1]);
    try setNonBlocking(fds[0]);
    var buf: [4]u8 = undefined;
    try testing.expectError(SysError.WouldBlock, read(fds[0], buf[0..]));
}

test "socketpair and monotonic clock" {
    const fds = try socketPair();
    defer close(fds[0]);
    defer close(fds[1]);
    const a = monotonicNs();
    const b = monotonicNs();
    try testing.expect(b >= a);
}
