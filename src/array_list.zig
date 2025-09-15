pub fn BoundedArrayList(comptime T: type) type {
    return BoundedArrayListAligned(T, null);
}

pub fn BoundedArrayListAligned(
    comptime T: type,
    comptime A: ?Alignment,
) type {
    if (A) |alignment| {
        if (alignment == Alignment.of(T))
            return BoundedArrayListAligned(T, null);
    }

    return struct {
        const Self = @This();

        /// An empty list initializer.
        pub const empty: Self = .{ .items = &.{} };

        pub const Item = T;
        pub const item_alignment = if (A) |alignment|
            alignment.toByteUnits()
        else
            @alignOf(Item);

        pub const OOM = collections.OOM;

        /// The slice of initialized items.
        items: []align(item_alignment) Item,
        /// The maximum capacity of the backing memory.
        capacity: usize = 0,

        /// Initializes an empty list using the given slice as backing memory.
        pub fn init(items: []align(item_alignment) Item) Self {
            return .{ .items = items[0..0], .capacity = items.len };
        }

        /// Copies all items from the other array over.
        pub fn copy(self: *Self, other: *const Self) OOM!void {
            if (self.capacity < other.items.len)
                return error.OutOfMemory;

            self.items.len = other.items.len;
            @memcpy(self.items, other.items);
        }

        /// Returns the remaining capacity of the list.
        pub fn remainingCapacity(self: *const Self) usize {
            return self.capacity - self.items.len;
        }

        /// Returns a pointer to the last item in the list or null, if the list
        /// is empty.
        pub fn getLast(self: *Self) ?*Item {
            return @constCast(self.getConstLast());
        }

        /// Returns a const pointer to the last item in the list or null, if the
        /// list is empty.
        pub fn getConstLast(self: *const Self) ?*const Item {
            return if (self.items.len == 0)
                null
            else
                &self.items[self.items.len - 1];
        }

        /// Clears all items from the array list.
        pub fn clear(self: *Self) void {
            @memset(self.items, undefined);
            self.items.len = 0;
        }

        /// Returns the backing slice containing all items and uninitialized
        /// capacity.
        pub fn backingSlice(self: *Self) []align(item_alignment) Item {
            var backing = self.items;
            backing.len = self.capacity;
            return backing;
        }

        /// Returns the slice of the uninitialized capacity.
        pub fn undefinedSlice(self: *Self) []Item {
            const slice = self.backingSlice();
            return slice[self.items.len..];
        }

        /// Reserves an uninitialized slot at the given index and returns a
        /// pointer to it.
        ///
        /// Any initialized items after the given index are shifted forwards.
        ///
        /// Asserts that the index is within bounds or at most one after the
        /// consecutive slice of initialized items.
        pub fn appendAt(self: *Self, idx: usize) OOM!*Item {
            assert(idx <= self.items.len);
            if (self.items.len == self.capacity)
                return error.OutOfMemory;

            self.shiftForward(idx, 1);
            return &self.items[idx];
        }

        /// Reserves an uninitialized slot at the end of the item slice and
        /// returns a pointer to it.
        pub fn append(self: *Self) OOM!*Item {
            return self.appendAt(self.items.len);
        }

        /// Reserves a number of uninitialized slots at the given index and
        /// returns the corresponding slice.
        ///
        /// Any initialized items after the given index are shifted forwards.
        ///
        /// Asserts that the index is within bounds or at most one after the
        /// consecutive slice of initialized items.
        pub fn appendSliceAt(self: *Self, idx: usize, len: usize) OOM![]Item {
            if (self.items.len + len <= self.capacity) {
                self.shiftForward(idx, len);
                return self.items[idx..][0..len];
            }

            return error.OutOfMemory;
        }

        /// Reserves a number of uninitialized slots at the end of the item
        /// slice and returns the corresponding slice.
        pub fn appendSlice(self: *Self, len: usize) OOM![]Item {
            return self.appendSliceAt(self.items.len, len);
        }

        /// Inserts the given item at the given index.
        ///
        /// Any initialized items after the given index are shifted forwards.
        ///
        /// Asserts that the index is within bounds or at most one after the
        /// consecutive slice of initialized items.
        pub fn pushAt(self: *Self, idx: usize, item: Item) OOM!void {
            const ptr = try self.appendAt(idx);
            ptr.* = item;
        }

        /// Inserts the given item at the end of the item slice.
        pub fn push(self: *Self, item: Item) OOM!void {
            const ptr = try self.append();
            ptr.* = item;
        }

        pub fn pushSliceAt(self: *Self, idx: usize, items: []const Item) OOM!void {
            const slice = try self.appendSliceAt(idx, items.len);
            @memcpy(slice, items);
        }

        pub fn pushSlice(self: *Self, items: []const Item) OOM!void {
            const slice = try self.appendSlice(items.len);
            @memcpy(slice, items);
        }

        pub fn pop(self: *Self) ?Item {
            if (self.items.len == 0)
                return null;

            const idx = self.items.len - 1;
            const item = self.items[idx];
            self.items[idx] = undefined;
            self.items.len = idx;

            return item;
        }

        pub fn remove(self: *Self, idx: usize) Item {
            assert(idx < self.items.len);
            const item = self.items[idx];
            self.shiftBackward(idx);
            return item;
        }

        pub fn swapRemove(self: *Self, idx: usize) Item {
            const last_item = self.pop() orelse unreachable;
            const removed_item = self.items[idx];
            self.items[idx] = last_item;
            return removed_item;
        }

        fn shiftForward(self: *Self, idx: usize, len: usize) void {
            assert(idx <= self.items.len);
            const trailing_len = self.items.len - idx;
            self.items.len += len;

            const source = self.items[idx + len ..][0..trailing_len];
            const dest = self.items[idx..][0..trailing_len];
            @memmove(source, dest);
            @memset(self.items[idx..][0..len], undefined);
        }

        fn shiftBackward(self: *Self, idx: usize) void {
            assert(idx < self.items.len);
            const trailing_len = self.items.len - idx - 1;
            const trailing_items = self.items[idx + 1 ..][0..trailing_len];
            @memmove(self.items[idx..][0..trailing_len], trailing_items);
            self.items[idx] = undefined;
            self.items.len -= 1;
        }
    };
}

fn capacityFor(capacity: usize) collections.OOM!usize {
    const min_capacity = 8;

    if (capacity <= min_capacity) {
        return min_capacity;
    }

    return collections.nextPow2(capacity);
}

const std = @import("std");
const tt = std.testing;
const assert = std.debug.assert;

const Alignment = std.mem.Alignment;
const Allocator = std.mem.Allocator;

const collections = @import("root.zig");

test "bounded empty" {
    var list: BoundedArrayList(i32) = .empty;
    try tt.expectEqual(0, list.items.len);
    try tt.expectEqual(0, list.capacity);
    try tt.expectEqual(null, list.pop());
    try tt.expectEqual(null, list.getLast());
    try tt.expectEqual(null, list.getConstLast());
    try tt.expectError(collections.oom, list.push(1));
}

test "append at" {
    var items: [8]i32 = undefined;
    var list: BoundedArrayList(i32) = .init(&items);
    try tt.expectEqual(items.len, list.capacity);

    var ptr = try list.appendAt(0);
    ptr.* = 2;
    try tt.expectEqual(1, list.items.len);
    ptr = try list.appendAt(0);
    ptr.* = 1;
    try tt.expectEqual(2, list.items.len);
    ptr = try list.appendAt(2);
    ptr.* = 3;
    try tt.expectEqual(3, list.items.len);

    try tt.expectEqualSlices(i32, &.{ 1, 2, 3 }, list.items);
    try tt.expectEqual(5, list.remainingCapacity());
}
