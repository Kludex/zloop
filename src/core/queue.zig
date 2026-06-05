//! A FIFO of ready callbacks, identified by opaque token. This is the loop's
//! `_ready` deque: callbacks scheduled with call_soon, plus I/O and timer
//! callbacks promoted each iteration. The loop drains a snapshot of its length
//! per iteration so callbacks scheduled during draining run on the next turn,
//! matching asyncio semantics.

const std = @import("std");

pub const ReadyQueue = struct {
    items: std.ArrayList(usize),
    head: usize = 0,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ReadyQueue {
        return .{ .items = .empty, .allocator = allocator };
    }

    pub fn deinit(self: *ReadyQueue) void {
        self.items.deinit(self.allocator);
    }

    pub fn push(self: *ReadyQueue, token: usize) !void {
        try self.items.append(self.allocator, token);
    }

    pub fn pop(self: *ReadyQueue) ?usize {
        if (self.head >= self.items.items.len) {
            self.head = 0;
            self.items.clearRetainingCapacity();
            return null;
        }
        const token = self.items.items[self.head];
        self.head += 1;
        return token;
    }

    /// Number of callbacks waiting to run.
    pub fn len(self: *const ReadyQueue) usize {
        return self.items.items.len - self.head;
    }

    pub fn isEmpty(self: *const ReadyQueue) bool {
        return self.len() == 0;
    }
};

const testing = std.testing;

test "fifo order and drain" {
    var q = ReadyQueue.init(testing.allocator);
    defer q.deinit();

    try testing.expect(q.isEmpty());
    try q.push(1);
    try q.push(2);
    try q.push(3);
    try testing.expectEqual(@as(usize, 3), q.len());

    try testing.expectEqual(@as(?usize, 1), q.pop());
    try testing.expectEqual(@as(?usize, 2), q.pop());
    // pushing during drain is observed by len but the snapshot pattern is the
    // caller's responsibility
    try q.push(4);
    try testing.expectEqual(@as(?usize, 3), q.pop());
    try testing.expectEqual(@as(?usize, 4), q.pop());
    try testing.expectEqual(@as(?usize, null), q.pop());
    try testing.expect(q.isEmpty());
}

test "reuse after drain resets storage" {
    var q = ReadyQueue.init(testing.allocator);
    defer q.deinit();
    try q.push(10);
    _ = q.pop();
    _ = q.pop(); // triggers reset
    try q.push(20);
    try testing.expectEqual(@as(?usize, 20), q.pop());
}
