//! Monotonic clock. asyncio's loop.time() is time.monotonic(); we read the same
//! CLOCK_MONOTONIC source so deadlines computed on the Python side and the Zig
//! side agree.

const std = @import("std");
const sys = @import("sys.zig");

/// Current monotonic time in nanoseconds.
pub fn nowNs() u64 {
    return sys.monotonicNs();
}

/// Current monotonic time in seconds as f64, matching time.monotonic().
pub fn nowSeconds() f64 {
    return @as(f64, @floatFromInt(nowNs())) / std.time.ns_per_s;
}

const testing = std.testing;

test "clock is monotonic non-decreasing" {
    const a = nowNs();
    const b = nowNs();
    try testing.expect(b >= a);
    try testing.expect(nowSeconds() > 0);
}
