const std = @import("std");

const tt = std.testing;
const assert = std.debug.assert;

const Log2Int = std.math.Log2Int(usize);

const array_list = @import("array_list.zig");
comptime {
    _ = array_list;
}

pub const BoundedArrayListAligned = array_list.BoundedArrayListAligned;

pub const OOM = std.mem.Allocator.Error;
pub const oom: OOM = error.OutOfMemory;

pub fn nextPow2(v: usize) OOM!usize {
    if (isPow2(v))
        return v;

    const log = log2(v);
    if (log == @bitSizeOf(usize) - 1)
        return oom;
    return @as(usize, 1) << (log + 1);
}

pub fn isPow2(v: usize) bool {
    assert(v != 0);
    return (v & (v - 1) == 0);
}

fn log2(v: usize) Log2Int {
    const bits = @bitSizeOf(usize);
    return @intCast(bits - 1 - @clz(v - 1));
}

test "next power of 2" {
    try tt.expectEqual(1, try nextPow2(1));
    try tt.expectEqual(2, try nextPow2(2));
    try tt.expectEqual(4, try nextPow2(3));
    try tt.expectEqual(4, try nextPow2(4));
    try tt.expectEqual(8, try nextPow2(5));
    try tt.expectEqual(8, try nextPow2(7));
    try tt.expectEqual(8, try nextPow2(8));
    try tt.expectEqual(64, try nextPow2(63));
    try tt.expectEqual(0x8000000000000000, try nextPow2(0x7FFFFFFFFFFFFFFF));
    try tt.expectError(oom, nextPow2(0xFFFFFFFFFFFFFFFF));
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
