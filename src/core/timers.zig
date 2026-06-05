//! A monotonic min-heap of timer deadlines. The loop uses it to find the next
//! deadline (to size the reactor poll timeout) and to pop all timers that are
//! due. Cancellation is lazy: a cancelled timer stays in the heap and is
//! skipped when popped, which keeps push/cancel O(log n)/O(1) without a
//! decrease-key.

const std = @import("std");

pub const Timer = struct {
    /// Absolute monotonic deadline in nanoseconds.
    when: u64,
    /// Monotonically increasing insertion order; breaks deadline ties so equal
    /// deadlines fire FIFO, matching asyncio's documented ordering.
    seq: u64,
    /// Opaque handle the loop maps back to a Python TimerHandle.
    token: usize,
    cancelled: bool = false,

    fn lessThan(_: void, a: Timer, b: Timer) std.math.Order {
        if (a.when != b.when) return std.math.order(a.when, b.when);
        return std.math.order(a.seq, b.seq);
    }
};

pub const TimerQueue = struct {
    heap: Heap,
    allocator: std.mem.Allocator,
    next_seq: u64 = 0,

    const Heap = std.PriorityQueue(Timer, void, Timer.lessThan);

    pub fn init(allocator: std.mem.Allocator) TimerQueue {
        return .{ .heap = Heap.initContext({}), .allocator = allocator };
    }

    pub fn deinit(self: *TimerQueue) void {
        self.heap.deinit(self.allocator);
    }

    /// Schedule `token` to fire at absolute monotonic time `when`. Returns the
    /// seq assigned, which together with the token identifies the timer for
    /// cancellation.
    pub fn push(self: *TimerQueue, when: u64, token: usize) !u64 {
        const seq = self.next_seq;
        self.next_seq += 1;
        try self.heap.push(self.allocator, .{ .when = when, .seq = seq, .token = token });
        return seq;
    }

    /// Mark the timer with this (token, seq) cancelled. Lazy: the entry is
    /// dropped when it reaches the top of the heap. Returns true if found.
    pub fn cancel(self: *TimerQueue, token: usize, seq: u64) bool {
        for (self.heap.items) |*t| {
            if (t.token == token and t.seq == seq) {
                t.cancelled = true;
                return true;
            }
        }
        return false;
    }

    /// The earliest live deadline, skipping cancelled entries at the top.
    pub fn peekDeadline(self: *TimerQueue) ?u64 {
        self.dropCancelledTop();
        const top = self.heap.peek() orelse return null;
        return top.when;
    }

    /// Pop the next due, non-cancelled timer if its deadline <= `now`.
    pub fn popDue(self: *TimerQueue, now: u64) ?Timer {
        self.dropCancelledTop();
        const top = self.heap.peek() orelse return null;
        if (top.when > now) return null;
        return self.heap.pop().?;
    }

    pub fn count(self: *const TimerQueue) usize {
        return self.heap.items.len;
    }

    fn dropCancelledTop(self: *TimerQueue) void {
        while (self.heap.peek()) |top| {
            if (!top.cancelled) break;
            _ = self.heap.pop().?;
        }
    }
};

const testing = std.testing;

test "timers fire in deadline then insertion order" {
    var q = TimerQueue.init(testing.allocator);
    defer q.deinit();

    _ = try q.push(100, 0xA);
    _ = try q.push(50, 0xB);
    const tie1 = try q.push(50, 0xC);
    _ = tie1;

    try testing.expectEqual(@as(?u64, 50), q.peekDeadline());

    // at now=50 the two 50ns timers are due, in insertion order B then C
    try testing.expectEqual(@as(usize, 0xB), q.popDue(50).?.token);
    try testing.expectEqual(@as(usize, 0xC), q.popDue(50).?.token);
    try testing.expectEqual(@as(?Timer, null), q.popDue(50)); // A not due yet
    try testing.expectEqual(@as(usize, 0xA), q.popDue(100).?.token);
    try testing.expectEqual(@as(?u64, null), q.peekDeadline());
}

test "cancel skips the timer" {
    var q = TimerQueue.init(testing.allocator);
    defer q.deinit();

    _ = try q.push(10, 1);
    const seq = try q.push(20, 2);
    _ = try q.push(30, 3);

    try testing.expect(q.cancel(2, seq));
    try testing.expect(!q.cancel(2, seq + 999)); // unknown seq

    try testing.expectEqual(@as(usize, 1), q.popDue(100).?.token);
    try testing.expectEqual(@as(usize, 3), q.popDue(100).?.token); // 2 skipped
    try testing.expectEqual(@as(?Timer, null), q.popDue(100));
}

test "cancelled top is skipped by peekDeadline" {
    var q = TimerQueue.init(testing.allocator);
    defer q.deinit();
    const seq = try q.push(10, 1);
    _ = try q.push(20, 2);
    try testing.expect(q.cancel(1, seq));
    try testing.expectEqual(@as(?u64, 20), q.peekDeadline());
}
