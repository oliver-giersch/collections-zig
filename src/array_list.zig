const min_array_list_capacity = 8;
const max_array_list_capacity = 1 << (@bitSizeOf(usize) - 1);

pub fn ArrayList(comptime T: type) type {
    return ArrayListAligned(T, null);
}

pub fn ArrayListAligned(comptime T: type, comptime A: ?Alignment) type {
    if (A) |alignment| {
        if (alignment == Alignment.of(T))
            return ArrayListAligned(T, null);
    }

    return struct {
        const Self = @This();

        /// An empty array list initializer.
        pub const empty: Self = .{ .bounded = .empty };

        /// The type of item stored in the list.
        pub const Item = Bounded.Item;
        pub const item_alignment = Bounded.item_alignment;

        pub const OOM = collections.OOM;

        const Bounded = BoundedArrayListAligned(T, A);

        /// The currently allocated (if non-zero capacity) bounded array list.
        bounded: Bounded,

        /// Initializes an empty list with sufficient capacity for
        /// `min_capacity` items.
        pub fn init(allocator: Allocator, min_capacity: usize) OOM!Self {
            const capacity = try capacityFor(min_capacity);
            if (capacity == 0)
                return .empty;

            const items = try allocator.alignedAlloc(Item, A, capacity);
            return .{ .bounded = .init(items) };
        }

        /// Frees the backing bounded array list allocation, if any.
        pub fn deinit(self: *Self, allocator: Allocator) void {
            if (self.bounded.capacity == 0)
                return;
            allocator.free(self.bounded.backingSlice());
        }

        /// Returns true if the list is empty.
        pub fn isEmpty(self: *const Self) bool {
            return self.bounded.isEmpty();
        }

        pub fn clone(self: *const Self, allocator: Allocator) OOM!Self {
            if (self.bounded.capacity == 0)
                return .empty;

            const items = try allocator.alignedAlloc(Item, A, self.bounded.capacity);
            var bounded: Bounded = .init(&items);
            bounded.copy(&self.bounded) catch unreachable;

            return .{ .bounded = bounded };
        }

        /// Returns the remaining capacity of the list.
        pub fn remainingCapacity(self: *const Self) usize {
            return self.bounded.remainingCapacity();
        }

        /// Returns a pointer to the last item.
        pub fn getLast(self: *Self) ?*Item {
            return self.bounded.getLast();
        }

        /// Returns a const pointer to the last item.
        pub fn getConstLast(self: *const Self) ?*const Item {
            return self.bounded.getConstLast();
        }

        /// Clears the list but keeps any allocated capacity.
        pub fn clear(self: *Self) void {
            self.bounded.clear();
        }

        /// Reserves additional space for at least `capacity` additional items.
        pub fn reserve(self: *Self, allocator: Allocator, capacity: usize) OOM!void {
            if (capacity >= max_array_list_capacity)
                return error.OutOfMemory;

            self.bounded.reserve(capacity) catch {
                // No overflow possible, since capacity is less than `max_array_list_capacity`
                // and the bounded array length is as well by definition.
                const new_capacity = try capacityFor(self.bounded.items.len + capacity);
                return self.resizeTo(allocator, new_capacity);
            };
        }

        pub fn shrink(self: *Self, allocator: Allocator) OOM!void {
            if (self.bounded.capacity == 0)
                return;

            const new_capacity = capacityFor(self.bounded.items.len) catch unreachable;
            if (self.bounded.capacity == new_capacity)
                return;

            return self.resizeTo(allocator, new_capacity);
        }

        pub fn ensureCapacity(self: *Self, allocator: Allocator, capacity: usize) OOM!void {
            if (capacity > self.bounded.capacity)
                try self.reserve(allocator, capacity - self.bounded.capacity);
        }

        pub fn appendAt(self: *Self, allocator: Allocator, idx: usize) OOM!*Item {
            return self.bounded.appendAt(idx) catch blk: {
                try self.grow(allocator);
                break :blk self.bounded.appendAt(idx) catch unreachable;
            };
        }

        pub fn append(self: *Self, allocator: Allocator) OOM!*Item {
            return self.appendAt(allocator, self.bounded.items.len);
        }

        pub fn appendSliceAt(
            self: *Self,
            allocator: Allocator,
            len: usize,
            idx: usize,
        ) OOM![]Item {
            return self.bounded.appendSliceAt(len, idx) catch blk: {
                try self.reserve(allocator, len);
                break :blk self.bounded.appendSliceAt(len, idx) catch unreachable;
            };
        }

        pub fn appendSlice(
            self: *Self,
            allocator: Allocator,
            len: usize,
        ) OOM![]Item {
            return self.appendSliceAt(allocator, len, self.bounded.items.len);
        }

        pub fn pushAt(
            self: *Self,
            allocator: Allocator,
            idx: usize,
            item: Item,
        ) OOM!void {
            const ptr = try self.appendAt(allocator, idx);
            ptr.* = item;
        }

        pub fn push(
            self: *Self,
            allocator: Allocator,
            item: Item,
        ) OOM!void {
            const ptr = try self.append(allocator);
            ptr.* = item;
        }

        pub fn pushSliceAt(
            self: *Self,
            allocator: Allocator,
            items: []const Item,
            idx: usize,
        ) OOM!void {
            const slice = try self.appendSliceAt(allocator, items.len, idx);
            @memcpy(slice, items);
        }

        pub fn pushSlice(
            self: *Self,
            allocator: Allocator,
            items: []const Item,
        ) OOM!void {
            const slice = try self.appendSlice(allocator, items.len);
            @memcpy(slice, items);
        }

        pub fn pop(self: *Self) ?Item {
            return self.bounded.pop();
        }

        pub fn remove(self: *Self, idx: usize) Item {
            return self.bounded.remove(idx);
        }

        pub fn swapRemove(self: *Self, idx: usize) Item {
            return self.bounded.swapRemove(idx);
        }

        fn grow(self: *Self, allocator: Allocator) OOM!void {
            const capacity = self.bounded.capacity;
            const new_capacity = try capacityFor(capacity + 1);
            return self.resizeTo(allocator, new_capacity);
        }

        fn resizeTo(self: *Self, allocator: Allocator, new_capacity: usize) OOM!void {
            assert(collections.isPow2(new_capacity));
            const items = try allocator.realloc(self.bounded.items, new_capacity);
            self.bounded.items.ptr = items.ptr;
            self.bounded.capacity = new_capacity;
        }

        fn alloc(allocator: Allocator, capacity: usize) OOM![]align(item_alignment) Item {
            assert(isPow2(capacity));
            return allocator.alignedAlloc(Item, A, capacity);
        }
    };
}

fn capacityFor(new_capacity: usize) collections.OOM!usize {
    if (new_capacity == 0)
        return 0;
    if (new_capacity <= min_array_list_capacity)
        return min_array_list_capacity;

    return nextPow2(new_capacity);
}

test "max capacity" {
    const max_capacity = try capacityFor(max_array_list_capacity);
    const no_overflow = max_capacity + (max_capacity - 1);
    try tt.expectError(collections.oom, capacityFor(no_overflow));
}

test "deinit" {
    var list: ArrayList(i32) = .empty;
    list.deinit(tt.allocator);
    list = try .init(tt.allocator, 1024);
    list.deinit(tt.allocator);
}

test "reserve" {
    const allocator = tt.allocator;

    var list: ArrayList(i32) = .empty;
    defer list.deinit(tt.allocator);

    try list.reserve(allocator, 0);
    try tt.expectEqual(0, list.bounded.capacity);

    try list.reserve(allocator, 4);
    try tt.expectEqual(8, list.bounded.capacity);
    list.bounded.pushSlice(&.{ 1, 2, 3, 4, 5, 6, 7, 8 }) catch unreachable;
    try list.reserve(allocator, 64);
    try tt.expectEqual(128, list.bounded.capacity);
    try tt.expectEqualSlices(i32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, list.bounded.items);
}

test "append" {
    var list: ArrayList(i32) = .empty;
    defer list.deinit(tt.allocator);

    var ptr = try list.append(tt.allocator);
    ptr.* = 1;
    ptr = try list.append(tt.allocator);
    ptr.* = 2;
    ptr = try list.append(tt.allocator);
    ptr.* = 3;

    try tt.expectEqualSlices(i32, &.{ 1, 2, 3 }, list.bounded.items);
    try tt.expectEqual(3, list.bounded.items.len);
    try tt.expectEqual(5, list.remainingCapacity());
}

test "append slice" {
    var list: ArrayList(i32) = .empty;
    defer list.deinit(tt.allocator);

    var slice = try list.appendSlice(tt.allocator, 8);
    @memcpy(slice, &[_]i32{ 1, 2, 3, 4, 5, 6, 7, 8 });
    slice = try list.appendSlice(tt.allocator, 8);
    @memcpy(slice, &[_]i32{ 9, 10, 11, 12, 13, 14, 15, 16 });

    try tt.expectEqualSlices(i32, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }, list.bounded.items);
    try tt.expectEqual(16, list.bounded.items.len);
    try tt.expectEqual(0, list.remainingCapacity());
}

test "append slice as array" {
    var list: ArrayList(i32) = .empty;
    defer list.deinit(tt.allocator);

    const slice = try list.appendSlice(tt.allocator, 4);
    const array1: *[4]i32 = slice[0..4];
    @memcpy(array1, &[_]i32{ 1, 2, 3, 4 });
    try tt.expectEqualSlices(i32, &.{ 1, 2, 3, 4 }, list.bounded.items);

    const array2: *[4]i32 = (try list.appendSlice(tt.allocator, 4))[0..4];
    @memcpy(array2, &[_]i32{ 5, 6, 7, 8 });
    try tt.expectEqualSlices(i32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, list.bounded.items);
}

test "append slice at as array" {
    var list: ArrayList(i32) = .empty;
    defer list.deinit(tt.allocator);

    var array: *[4]i32 = (try list.appendSlice(tt.allocator, 4))[0..4];
    @memcpy(array, &[_]i32{ 5, 6, 7, 8 });
    array = (try list.appendSliceAt(tt.allocator, 4, 0))[0..4];
    @memcpy(array, &[_]i32{ 1, 2, 3, 4 });

    try tt.expectEqualSlices(i32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, list.bounded.items);
}

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

        /// An empty array list initializer.
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

        /// Returns true if the list is empty.
        pub fn isEmpty(self: *const Self) bool {
            return self.items.len == 0;
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

        /// Ensures the required capacity is available.
        ///
        /// This is effectively a no-op. It just checks, if sufficient capacity
        /// is available and returns an error otherwise.
        pub fn reserve(self: *const Self, capacity: usize) OOM!void {
            if (self.remainingCapacity() < capacity) {
                return error.OutOfMemory;
            }
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
        pub fn appendSliceAt(self: *Self, len: usize, idx: usize) OOM![]Item {
            if (self.items.len + len <= self.capacity) {
                self.shiftForward(idx, len);
                return self.items[idx..][0..len];
            }

            return error.OutOfMemory;
        }

        /// Reserves a number of uninitialized slots at the end of the item
        /// slice and returns the corresponding slice.
        pub fn appendSlice(self: *Self, len: usize) OOM![]Item {
            return self.appendSliceAt(len, self.items.len);
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

        /// Inserts the given slice at the given index.
        ///
        /// Any initialized items after the given index are shifted forwards.
        ///
        /// Asserts that the index is within bounds or at most one after the
        /// consecutive slice of initialized items.
        pub fn pushSliceAt(self: *Self, idx: usize, items: []const Item) OOM!void {
            const slice = try self.appendSliceAt(items.len, idx);
            @memcpy(slice, items);
        }

        /// Inserts the given slice at the end of the item slice.
        pub fn pushSlice(self: *Self, items: []const Item) OOM!void {
            const slice = try self.appendSlice(items.len);
            @memcpy(slice, items);
        }

        /// Removes and returns the last item in the list or returns null, if
        /// the list is empty.
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
            self.items[self.items.len - 1] = undefined;
            self.items.len -= 1;
        }
    };
}

const std = @import("std");
const tt = std.testing;
const assert = std.debug.assert;

const Alignment = std.mem.Alignment;
const Allocator = std.mem.Allocator;

const collections = @import("root.zig");
const isPow2 = collections.isPow2;
const nextPow2 = collections.nextPow2;

test "bounded empty" {
    var list: BoundedArrayList(i32) = .empty;
    try tt.expectEqual(0, list.items.len);
    try tt.expectEqual(0, list.capacity);
    try tt.expectEqual(null, list.pop());
    try tt.expectEqual(null, list.getLast());
    try tt.expectEqual(null, list.getConstLast());
    try tt.expectError(collections.oom, list.push(1));

    var items: [8]i32 = undefined;
    list = .init(&items);
    try tt.expectEqual(0, list.items.len);
    try tt.expectEqual(items.len, list.capacity);
    try tt.expectEqual(null, list.pop());
    try tt.expectEqual(null, list.getLast());
    try tt.expectEqual(null, list.getConstLast());
}

test "bounded append" {
    var items: [8]i32 = undefined;
    var list: BoundedArrayList(i32) = .init(&items);

    var ptr = try list.append();
    ptr.* = 1;
    ptr = try list.append();
    ptr.* = 2;
    ptr = try list.append();
    ptr.* = 3;

    try tt.expectEqualSlices(i32, &.{ 1, 2, 3 }, list.items);
    try tt.expectEqual(3, list.items.len);
    try tt.expectEqual(5, list.remainingCapacity());
}

test "bounded append slice" {
    var items: [8]i32 = undefined;
    var list: BoundedArrayList(i32) = .init(&items);

    var slice = try list.appendSlice(4);
    @memcpy(slice, &[_]i32{ 1, 2, 3, 4 });
    slice = try list.appendSlice(4);
    @memcpy(slice, &[_]i32{ 5, 6, 7, 8 });

    try tt.expectEqualSlices(i32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, list.items);
    try tt.expectEqual(8, list.items.len);
    try tt.expectEqual(0, list.remainingCapacity());
    try tt.expectError(collections.oom, list.append());
}

test "bounded append at" {
    var items: [8]i32 = undefined;
    var list: BoundedArrayList(i32) = .init(&items);

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

test "bounded append slice at" {
    var items: [8]i32 = undefined;
    var list: BoundedArrayList(i32) = .init(&items);

    var slice = try list.appendSlice(4);
    @memcpy(slice, &[_]i32{ 5, 6, 7, 8 });
    slice = try list.appendSliceAt(4, 0);
    @memcpy(slice, &[_]i32{ 1, 2, 3, 4 });

    try tt.expectEqualSlices(i32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, list.items);
    try tt.expectEqual(8, list.items.len);
    try tt.expectEqual(0, list.remainingCapacity());
    try tt.expectError(collections.oom, list.append());
}

test "pop" {
    var items: [8]i32 = undefined;
    var list: BoundedArrayList(i32) = .init(&items);
    try list.pushSlice(&.{ 1, 2, 3, 4, 5, 6, 7, 8 });
    try tt.expectEqual(8, list.items.len);
    try tt.expectEqual(0, list.remainingCapacity());

    for (0..items.len) |i| {
        const s: i32 = @intCast(i);
        try tt.expectEqual(@as(i32, items.len) - s, list.pop());
        try tt.expectEqual(items.len - i - 1, list.items.len);
        try tt.expectEqual(i + 1, list.remainingCapacity());
    }

    try tt.expectEqual(null, list.pop());
    try tt.expectEqual(0, list.items.len);
    try tt.expectEqual(8, list.remainingCapacity());
}

test "remove" {
    var items: [8]i32 = undefined;
    var list: BoundedArrayList(i32) = .init(&items);
    try list.pushSlice(&.{ 1, 2, 3, 4, 5, 6, 7, 8 });

    try tt.expectEqual(1, list.remove(0));
    try tt.expectEqualSlices(i32, &.{ 2, 3, 4, 5, 6, 7, 8 }, list.items);
    try tt.expectEqual(2, list.remove(0));
    try tt.expectEqualSlices(i32, &.{ 3, 4, 5, 6, 7, 8 }, list.items);
    try tt.expectEqual(4, list.remove(1));
    try tt.expectEqualSlices(i32, &.{ 3, 5, 6, 7, 8 }, list.items);
    try tt.expectEqual(7, list.remove(3));
    try tt.expectEqualSlices(i32, &.{ 3, 5, 6, 8 }, list.items);

    try tt.expectEqual(4, list.items.len);
    try tt.expectEqual(4, list.remainingCapacity());
}

test "swap remove" {
    var items: [8]i32 = undefined;
    var list: BoundedArrayList(i32) = .init(&items);
    try list.pushSlice(&.{ 1, 2, 3, 4, 5, 6, 7, 8 });

    try tt.expectEqual(1, list.swapRemove(0));
    try tt.expectEqualSlices(i32, &.{ 8, 2, 3, 4, 5, 6, 7 }, list.items);
    try tt.expectEqual(2, list.swapRemove(1));
    try tt.expectEqualSlices(i32, &.{ 8, 7, 3, 4, 5, 6 }, list.items);
    try tt.expectEqual(4, list.swapRemove(3));
    try tt.expectEqualSlices(i32, &.{ 8, 7, 3, 6, 5 }, list.items);
    try tt.expectEqual(7, list.swapRemove(1));
    try tt.expectEqualSlices(i32, &.{ 8, 5, 3, 6 }, list.items);

    try tt.expectEqual(4, list.items.len);
    try tt.expectEqual(4, list.remainingCapacity());
}

test "reserve and push" {
    var items: [8]i32 = undefined;
    var list: BoundedArrayList(i32) = .init(&items);

    try list.reserve(4);
    list.push(1) catch unreachable;
    list.push(2) catch unreachable;
    list.push(3) catch unreachable;
    list.push(4) catch unreachable;

    try list.reserve(4);
    list.pushSlice(&.{ 5, 6, 7, 8 }) catch unreachable;

    try tt.expectEqualSlices(i32, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, list.items);
}

test "clear" {
    var items: [8]i32 = undefined;
    var list: BoundedArrayList(i32) = .init(&items);
    list.pushSlice(&.{ 1, 2, 3, 4, 5, 6, 7, 8 }) catch unreachable;
    list.clear();

    try tt.expectEqual(0, list.items.len);
}
