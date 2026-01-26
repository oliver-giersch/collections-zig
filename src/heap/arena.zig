const ArenaAllocator = @This();

const std = @import("std");

const Alignment = std.mem.Alignment;
const Allocator = std.mem.Allocator;

const SList = @import("../root.zig").SList;

pub const ResetMode = union(enum) {
    retain_with_limit: usize,
    retain_all: void,
    free_all: void,
};

const BufferNode = extern struct {
    const node_size = @sizeOf(BufferNode);
    const node_alignment: Alignment = .of(BufferNode);

    next: SList.Link,
    len: usize,
    buf: [0]u8,

    fn alloc(
        allocator: Allocator,
        n: usize,
        alignment: Alignment,
    ) Allocator.Error!*BufferNode {
        const len = n + alignment.toByteUnits();
        const ptr = allocator.rawAlloc(
            node_size + len,
            node_alignment,
            @returnAddress(),
        ) orelse return error.OutOfMemory;

        const node: *BufferNode = @ptrCast(@alignCast(ptr));
        node.* = .{ .next = undefined, .len = len, .buf = undefined };
        return node;
    }

    fn free(self: *BufferNode, allocator: Allocator) void {
        const mem = self.memory();
        allocator.rawFree(mem, node_alignment, @returnAddress());
    }

    fn resize(self: *BufferNode, allocator: Allocator, n: usize, alignment: Alignment) bool {
        const new_len = n + alignment.toByteUnits();
        const mem = self.memory();
        return allocator.rawResize(mem, alignment, new_len, @returnAddress());
    }

    fn placeAllocation(
        self: *BufferNode,
        alignment: Alignment,
        idx: usize,
    ) struct { [*]u8, usize } {
        const buf = self.data();
        const addr = @intFromPtr(buf.ptr) + idx;
        const aligned_addr = alignment.forward(addr);
        const diff = aligned_addr - addr;

        return .{ @ptrCast(aligned_addr), diff };
    }

    fn data(self: *BufferNode) []u8 {
        const ptr: [*]u8 = @ptrCast(self.buf);
        return ptr[0..self.len];
    }

    fn memory(self: *BufferNode) []u8 {
        const ptr: [*]u8 = @ptrCast(self);
        return ptr[0 .. node_size + self.len];
    }

    fn from(link: *SList.Link) *BufferNode {
        return @fieldParentPtr("next", link);
    }
};

parent_allocator: Allocator,
list: SList = .empty,
end_idx: usize = 0,

pub fn init(parent_allocator: Allocator) ArenaAllocator {
    return .{ .parent_allocator = parent_allocator };
}

pub fn deinit(self: *ArenaAllocator) void {
    var it = self.list.iter();
    while (it.next()) |link| {
        const node: *BufferNode = @fieldParentPtr("next", link);
        node.free(self.parent_allocator);
    }
}

pub fn reset(self: *ArenaAllocator, mode: ResetMode) bool {
    const retain_capacity = switch (mode) {
        .retain_with_limit => |limit| @min(limit, self.calculateCapacity()),
        .retain_all => self.calculateCapacity(),
        .free_all => 0,
    };

    if (retain_capacity == 0) {
        self.deinit();
        self.* = .{ .parent_allocator = self.parent_allocator };
        return true;
    }

    var it = self.list.iter();
    const last_node: *BufferNode = while (it.next()) |link| {
        const node = BufferNode.from(link);
        if (it.peekNext() == null)
            break node;

        node.free(self.parent_allocator);
    } else return true;

    self.list.head = &last_node.next;
    self.end_idx = 0;

    // Nothing to do if the node already has the desired capacity.
    if (last_node.len == retain_capacity)
        return true;

    // Try to resize the node in place.
    if (last_node.resize(self.parent_allocator, retain_capacity, .@"1")) {
        last_node.len = retain_capacity;
        return true;
    }

    // Allocate a new node with the correct capacity and free the old one.
    const new_node = BufferNode.alloc(
        self.parent_allocator,
        retain_capacity,
        .@"1",
    ) catch return false;
    last_node.free(self.parent_allocator);
    new_node.len = retain_capacity;
    self.list.head = &new_node.next;

    return true;
}

pub fn calculateCapacity(self: *const ArenaAllocator) usize {
    var capacity: usize = 0;
    var it = self.list.constIter();
    while (it.next()) |link| {
        const node: *BufferNode = @fieldParentPtr("next", link);
        capacity += node.len;
    }

    return capacity;
}

pub fn alloc(
    self: *ArenaAllocator,
    n: usize,
    alignment: Alignment,
) Allocator.Error![]u8 {
    const ptr = self.rawAlloc(n, alignment) orelse
        return error.OutOfMemory;
    return ptr[0..n];
}

pub fn rawAlloc(self: *ArenaAllocator, n: usize, alignment: Alignment) ?[*]u8 {
    // Check if the allocation fits into the head node.
    if (self.allocHead(n, alignment)) |ptr|
        return ptr;

    // If there are no buffers or resizing the current buffer failed, try
    // to allocate a new one.
    const node = BufferNode.alloc(self.parent_allocator, n, alignment) catch
        return null;
    const ptr, const diff = node.placeAllocation(alignment, 0);
    self.end_idx = diff + n;

    return ptr;
}

fn allocHead(self: *ArenaAllocator, n: usize, alignment: Alignment) ?[*]u8 {
    const link = self.list.head orelse return null;
    const node: *BufferNode = @fieldParentPtr("next", link);
    const ptr, const diff = node.placeAllocation(alignment, self.end_idx);
    const new_end_idx = self.end_idx + diff + n;

    // Check if the allocation fits into the node as-is.
    if (new_end_idx <= node.len)
        return ptr;

    // Otherwise, try resizing the node in place.
    if (node.resize(self.parent_allocator, new_end_idx, alignment))
        return ptr;

    return null;
}

const vtable: Allocator.VTable = .{
    .alloc = arena_alloc,
    .resize = arena_resize,
    .remap = arena_remap,
    .free = arena_free,
};

fn arena_alloc(
    ptr: *anyopaque,
    len: usize,
    alignment: Alignment,
    _: usize,
) ?[*]u8 {
    const arena_allocator: *ArenaAllocator = @ptrCast(@alignCast(ptr));
    return arena_allocator.rawAlloc(len, alignment);
}

fn arena_resize(
    ptr: *anyopaque,
    memory: []u8,
    _: Alignment,
    new_len: usize,
    _: usize,
) bool {
    const arena_allocator: *ArenaAllocator = @ptrCast(@alignCast(ptr));
    arena_allocator.resize(memory, new_len);
}

fn arena_remap(
    ptr: *anyopaque,
    memory: []u8,
    alignment: Alignment,
    new_len: usize,
    _: usize,
) ?[*]u8 {
    const arena_allocator: *ArenaAllocator = @ptrCast(@alignCast(ptr));
    return arena_allocator.remap(memory, alignment, new_len);
}

fn arena_free(ptr: *anyopaque, memory: []u8, _: Alignment, _: usize) void {
    const arena_allocator: *ArenaAllocator = @ptrCast(@alignCast(ptr));
    arena_allocator.free(memory);
}
