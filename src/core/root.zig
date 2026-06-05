//! Pure-Zig core: the event loop engine, independent of CPython.

pub const sys = @import("sys.zig");
pub const reactor = @import("reactor.zig");
pub const timers = @import("timers.zig");
pub const queue = @import("queue.zig");
pub const clock = @import("clock.zig");
pub const loop = @import("loop.zig");

test {
    _ = sys;
    _ = reactor;
    _ = timers;
    _ = queue;
    _ = clock;
    _ = loop;
}
