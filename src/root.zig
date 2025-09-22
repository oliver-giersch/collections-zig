const std = @import("std");

const tt = std.testing;
const assert = std.debug.assert;

const array_list = @import("array_list.zig");
const hash_map = @import("hash_map.zig");

comptime {
    _ = array_list;
    _ = hash_map;
}

const runtime_safety = switch (@import("builtin").mode) {
    .Debug, .ReleaseSafe => true,
    else => false,
};
const Log2Int = std.math.Log2Int(usize);

pub const BoundedArrayList = array_list.BoundedArrayList;
pub const BoundedArrayListAligned = array_list.BoundedArrayListAligned;
pub const ArrayList = array_list.ArrayList;
pub const ArrayListAligned = array_list.ArrayListAligned;

pub const OOM = std.mem.Allocator.Error;
pub const oom: OOM = error.OutOfMemory;

pub const SafetyLock = struct {
    const Self = @This();

    pub const unlocked: Self = .{ .state = .unlocked };
    pub const locked: Self = .{ .state = .locked };

    const State = if (runtime_safety) enum { unlocked, locked } else enum {
        const unlocked = undefined;
        const locked = undefined;
    };

    state: State,

    pub fn assertLocked(self: *const Self) void {
        if (comptime !runtime_safety)
            return;
        assert(self.state == .locked);
    }

    pub fn assertUnlocked(self: *const Self) void {
        if (comptime !runtime_safety)
            return;
        assert(self.state == .unlocked);
    }

    pub fn lock(self: *Self) void {
        if (comptime !runtime_safety)
            return;
        self.assertUnlocked();
        self.state = .locked;
    }

    pub fn unlock(self: *Self) void {
        if (comptime !runtime_safety)
            return;
        self.assertLocked();
        self.state = .unlocked;
    }
};

test "safety lock (runtime safety on)" {
    var lock: SafetyLock = .unlocked;
    lock.lock();
    lock.unlock();

    lock = .locked;
    lock.unlock();
    lock.lock();
}

pub fn nextPow2(v: usize) OOM!usize {
    if (isPow2(v))
        return v;

    const log = log2(v);
    if (log == @bitSizeOf(usize) - 1)
        return oom;
    return @as(usize, 1) << (log + 1);
}

test "next power of 2" {
    try tt.expectEqual(1, try nextPow2(1));
    try tt.expectEqual(2, try nextPow2(2));
    try tt.expectEqual(4, try nextPow2(3));
    try tt.expectEqual(4, try nextPow2(4));
    try tt.expectEqual(8, try nextPow2(5));
    try tt.expectEqual(8, try nextPow2(7));
    try tt.expectEqual(8, try nextPow2(8));
    try tt.expectEqual(32, try nextPow2(17));
    try tt.expectEqual(32, try nextPow2(31));
    try tt.expectEqual(32, try nextPow2(32));
    try tt.expectEqual(64, try nextPow2(63));
    try tt.expectEqual(0x8000000000000000, try nextPow2(0x7FFFFFFFFFFFFFFF));
    try tt.expectError(oom, nextPow2(~@as(usize, 0)));
}

pub fn isPow2(v: usize) bool {
    assert(v != 0);
    return (v & (v - 1) == 0);
}

test "is power of 2" {
    try tt.expect(isPow2(1));
    try tt.expect(isPow2(2));
    try tt.expect(isPow2(4));
    try tt.expect(isPow2(8));
    try tt.expect(isPow2(16));
    try tt.expect(isPow2(32));
    try tt.expect(isPow2(64));
    try tt.expect(isPow2(4096));

    try tt.expect(!isPow2(3));
    try tt.expect(!isPow2(5));
    try tt.expect(!isPow2(6));
    try tt.expect(!isPow2(12));
    try tt.expect(!isPow2(17));
    try tt.expect(!isPow2(33));
    try tt.expect(!isPow2(55));
    try tt.expect(!isPow2(9000));
}

fn log2(v: usize) Log2Int {
    const bits = @bitSizeOf(usize);
    return @intCast(bits - 1 - @clz(v - 1));
}
