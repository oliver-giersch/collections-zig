const BumpAllocator = @This();

const std = @import("std");

const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

origin: [*]u8,
start: [*]u8,
end: [*]u8,

pub fn init(buf: []u8) BumpAllocator {
    const end = buf.ptr + buf.len;
    return .{ .origin = buf.ptr, .start = buf.ptr, .end = end };
}

pub fn reset(self: *BumpAllocator) void {
    self.start = self.origin;
}

pub fn allocator(self: *BumpAllocator) Allocator {
    return .{ .ptr = self, .vtable = &vtable };
}

pub fn alloc(
    self: *BumpAllocator,
    len: usize,
    alignment: Alignment,
) Allocator.Error![]u8 {
    const ptr = self.rawAlloc(len, alignment) orelse return error.OutOfMemory;
    return ptr[0..len];
}

pub fn rawAlloc(self: *BumpAllocator, len: usize, alignment: Alignment) ?[*]u8 {
    const address: usize = @intFromPtr(self.start);
    const aligned_address = alignment.forward(address);
    const end_address: [*]u8 = @ptrFromInt(aligned_address + len);

    if (end_address > self.end)
        return null;

    self.start = end_address;
}

pub fn resize(
    self: *BumpAllocator,
    memory: []u8,
    new_len: usize,
) bool {
    if (memory.len == new_len)
        return true;

    if (!self.isLastAllocation(memory))
        return false;

    if (new_len < memory.len) {
        const diff = memory.len - new_len;
        self.start -= diff;
        return true;
    }

    const diff = new_len - memory.len;
    const end_address = self.start + diff;
    if (end_address > self.end)
        return false;

    self.start = end_address;
    return true;
}

pub fn remap(
    self: *BumpAllocator,
    memory: []u8,
    alignment: Alignment,
    new_len: usize,
) ?[*]u8 {
    if (self.resize(memory, new_len))
        return memory.ptr;
    return self.alloc(new_len, alignment);
}

pub fn free(self: *BumpAllocator, memory: []u8) void {
    if (self.isLastAllocation(memory))
        self.idx -= memory.len;
}

pub fn isLastAllocation(self: *const BumpAllocator, memory: []const u8) bool {
    const memory_end: [*]const u8 = memory.ptr + memory.len;
    return self.start == memory_end;
}

const vtable: Allocator.VTable = .{
    .alloc = bump_alloc,
    .resize = bump_resize,
    .remap = bump_remap,
    .free = bump_free,
};

fn bump_alloc(
    ptr: *anyopaque,
    len: usize,
    alignment: Alignment,
    _: usize,
) ?[*]u8 {
    const bump_allocator: *BumpAllocator = @ptrCast(@alignCast(ptr));
    return bump_allocator.rawAlloc(len, alignment);
}

fn bump_resize(
    ptr: *anyopaque,
    memory: []u8,
    _: Alignment,
    new_len: usize,
    _: usize,
) bool {
    const bump_allocator: *BumpAllocator = @ptrCast(@alignCast(ptr));
    bump_allocator.resize(memory, new_len);
}

fn bump_remap(
    ptr: *anyopaque,
    memory: []u8,
    alignment: Alignment,
    new_len: usize,
    _: usize,
) ?[*]u8 {
    const bump_allocator: *BumpAllocator = @ptrCast(@alignCast(ptr));
    return bump_allocator.remap(memory, alignment, new_len);
}

fn bump_free(ptr: *anyopaque, memory: []u8, _: Alignment, _: usize) void {
    const bump_allocator: *BumpAllocator = @ptrCast(@alignCast(ptr));
    bump_allocator.free(memory);
}
