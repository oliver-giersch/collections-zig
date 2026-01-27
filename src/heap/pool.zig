const std = @import("std");

const math = std.math;
const mem = std.mem;
const assert = std.debug.assert;

const Allocator = mem.Allocator;
const Alignment = mem.Alignment;

const collections = @import("../root.zig");

const DList = collections.DList;
const DQueue = collections.DQueue;

pub const PoolOptions = struct {
    max_cached_items: usize = 128,
};

pub fn ItemPool(comptime T: type) type {
    return ItemPoolAligned(T, null);
}

pub fn ItemPoolAligned(comptime T: type, comptime A: ?Alignment) type {
    if (A) |alignment| {
        if (alignment == Alignment.of(T))
            return ItemPoolAligned(T, null);
    }

    return struct {
        const Self = @This();

        const OOM = collections.OOM;
        const oom = collections.oom;

        const Item = T;
        const item_alignment = @max(@alignOf(Item), A);

        const AlignedItem = struct {
            aligned: Item align(item_alignment),
        };

        const Node = struct {
            const item_count = @bitSizeOf(usize);
            const node_alignment = math.ceilPowerOfTwo(
                usize,
                @max(item_alignment, @sizeOf(Node)),
            );

            const Index = math.Log2Int(usize);

            const all_free: usize = ~@as(usize, 0);

            free_mask: usize,
            next: DQueue.Link,
            items: [item_count]AlignedItem,

            fn alloc(self: *Node) *align(item_alignment) Item {
                // Trailing zeroes corresponds to the first 0x1 (free) item bit.
                assert(self.free_mask != 0);
                const tz = @ctz(self.free_mask);
                const idx: Index = @intCast(tz);
                self.free_mask &= ~Node.mask(idx);

                return &self.items[idx].aligned;
            }

            fn allocFirst(self: *Node) *align(item_alignment) Item {
                self.free_mask = ~@as(usize, 0x1);
                return &self.items[0].aligned;
            }

            inline fn mask(bit: Index) usize {
                return @as(usize, 1) << bit;
            }
        };

        allocator: Allocator,
        empty_count: usize,
        max_empty_nodes: usize,
        /// The list of empty nodes.
        empty: DList,
        /// The list of full nodes.
        full: DList,
        /// The list of partial nodes.
        partial: DList,

        pub fn init(allocator: Allocator) Self {
            return .initOptions(allocator, .{});
        }

        pub fn initPreheat(allocator: Allocator, items: usize) OOM!Self {
            var pool: Self = .initOptions(allocator, .{ .max_cached_items = items });
            errdefer pool.deinit();

            for (0..pool.max_empty_nodes) |_| {
                const node = try allocator.create(Node);
                node.free_mask = Node.all_free;
                pool.empty_count += 1;
                pool.empty.insertHead(&node.next);
            }

            return pool;
        }

        pub fn initOptions(allocator: Allocator, opts: PoolOptions) Self {
            const max_empty_nodes = (opts.max_cached_items + Node.item_count - 1) / Node.item_count;
            return .{
                .allocator = allocator,
                .empty_count = 0,
                .max_empty_nodes = max_empty_nodes,
                .empty = .empty,
                .full = .empty,
                .partial = .empty,
            };
        }

        pub fn deinit(self: *Self) void {
            self.deinitList(&self.empty);
            self.deinitList(&self.full);
            self.deinitList(&self.partial);
        }

        pub fn alloc(self: *Self) OOM!*align(item_alignment) Item {
            // Try allocating from a partial list node.
            if (self.partial.head) |link| {
                const node: *Node = @fieldParentPtr("next", link);
                const item = node.alloc();

                if (node.isFull()) {
                    @branchHint(.unlikely);
                    _ = self.partial.removeHead();
                    self.full.insertHead(link);
                }

                return item;
            }

            const node = self.popEmpty() orelse try self.allocator.create(Node);
            self.partial.insertHead(&node.next);
            return node.allocFirst();
        }

        pub fn free(self: *Self, item: *align(item_alignment) Item) void {
            const node_addr = @intFromPtr(item) & (Node.node_alignment - 1);
            const node: *Node = @ptrCast(node_addr);
            const idx = node.getIndex(item);

            // If the node was full, move it from the full to the partial queue.
            // We insert at the tail of the partial queue in order to .. ?
            const was_full = node.free(idx);
            if (was_full) {
                self.full.remove(&node.next);
                // TODO: which is better, insertHead or insertTail?
                // insertTail: may not be used for new allocations
                self.partial.insertHead(&node.next);
                return;
            }

            // If the node has become empty, remove it from the partial list
            // and either free it or move it to the empty list.
            const free_count = node.freeCount();
            if (free_count == 0) {
                self.partial.remove(&node.next);
                if (self.empty_count == self.max_empty_nodes)
                    self.allocator.destroy(node)
                else {
                    self.empty.insertHead(&node.next);
                    self.empty_count += 1;
                }

                return;
            }
        }

        fn popEmpty(self: *Self) ?*Node {
            const link = self.empty.removeHead() orelse return null;
            assert(self.empty_count != 0);
            self.empty_count -= 1;
            return @fieldParentPtr("next", link);
        }

        fn deinitList(self: *Self, list: *DList) void {
            var it = list.iter();
            while (it.next()) |link| {
                const node: *Node = @fieldParentPtr("next", link);
                self.allocator.destroy(node);
            }
        }
    };
}
