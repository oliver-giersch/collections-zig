pub fn HashMap(comptime K: type, V: type, comptime O: Options) type {
    return ContextHashMap(K, V, std.hash_map.AutoContext(K), O);
}

pub fn StringHashMap(comptime V: type, comptime O: Options) type {
    return ContextHashMap([]const u8, V, StringContext, O);
}

pub const StringContext = struct {
    const Self = @This();

    pub fn eql(_: Self, a: []const u8, b: []const u8) bool {
        return mem.eql(u8, a, b);
    }

    pub fn hash(_: Self, str: []const u8) Hash {
        return Wyhash.hash(0, str);
    }
};

/// A global (static) placeholder metadata vector used as default buffer pointer
/// for allocation-less hash maps.
var empty_vector: Metadata.Block = Metadata.repeat(.empty);

/// The configuration options for the HashMap type.
pub const Options = struct {
    pub const default: Options = .{};

    /// The data layout for keys and values within the hash map.
    ///
    /// Keys and values can either be stored together (`.array`) or separately
    /// (`.multi_array`). The latter variant can be result in greater space and
    /// cache efficiency, if different size or alignment requirements of keys
    /// and values would otherwise cause padding bytes to be inserted between
    /// them.
    pub const Layout = enum {
        auto,
        array,
        multi_array,
    };

    pub const ProbingStrategy = enum {
        linear,
        triangular,
        cache_line,
    };

    /// The memory layout for the key-value pairs.
    layout: Options.Layout = .auto,
    /// The probing strategy used for resolving hash conflicts.
    probing_strategy: ProbingStrategy = .triangular,
    /// The maximum load percentage before the map is resized.
    max_load_percentage: u8 = 88, // round-up from 87.5, i.e. 1/8th
};

pub const Hash = u64;

pub fn ContextHashMap(
    comptime K: type,
    comptime V: type,
    comptime C: type,
    comptime O: Options,
) type {
    if (O.max_load_percentage < 50)
        @compileError("invalid max load <50%");
    return struct {
        const Self = @This();

        /// The initializer value for an empty hash map.
        pub const empty: Self = .{
            .len = 0,
            .remaining_capacity = 0,
            .entry_mask = 0,
            .metadata = @ptrCast(&empty_vector),
        };

        /// The key type.
        pub const Key = K;
        /// The value type.
        pub const Value = V;
        /// The hashing and equality context.
        pub const Context = C;
        /// The comptime configuration options.
        pub const options = O;

        /// The key-value iterator.
        pub const ConstIterator = struct {
            pub const Entry = struct {
                key: *const Key,
                value: *const Value,
            };

            // TODO: iterate blocks, not the block copy (?) current block pointer/index
            map: *const Self,
            remaining_len: usize,
            entry_idx: usize = 0,
            current_block: Metadata.BitMask = 0,

            pub fn next(self: *ConstIterator) ?ConstIterator.Entry {
                if (self.remaining_len == 0) {
                    return null;
                }

                while (true) {
                    if (self.current_block == 0) {
                        const block = self.map.getAlignedMetadaBlock(self.entry_idx / Metadata.block_size);
                        self.current_block = @bitCast(Metadata.findUsed(block));
                        self.entry_idx = (self.entry_idx + Metadata.block_size) & Metadata.block_mask; // FIXME:
                        continue;
                    }

                    const ctz = @ctz(self.current_block);
                    self.entry_idx += ctz;
                    self.remaining_len -= 1;
                    self.current_block >>= ctz;

                    const idx = self.entry_idx;
                    return .{
                        .key = self.map.getConstKey(idx),
                        .value = self.map.getConstValue(idx),
                    };
                }
            }
        };

        pub const KeyValue = struct {
            key: Key,
            value: Value,
        };

        /// The result of a `getOrInsert` method call.
        pub const GetOrInsert = union(enum) {
            /// The initialized value found for the given key.
            found: *Value,
            /// The uninitialized value for the inserted key.
            inserted: *Value,
        };

        /// This struct describes the memory layout for the allocation backing
        /// the hashmap.
        /// The buffer stores either two or three (depending on configuration)
        /// variable length arrays containing the metadata slots followed
        /// immediately by the key-value pairs, which are either stored next
        /// to each other of spread out across two separate variable length
        /// arrays.
        const Buffer = extern struct {
            /// The alignment of the metadata slot array.
            const metadata_alignment = if (options.probing_strategy == .cache_line)
                @max(cache_line.len, @alignOf(Metadata.Block))
            else
                @alignOf(Metadata.Block);

            /// The alignment of the buffer struct.
            const buffer_alignment = @max(@alignOf(Buffer), metadata_alignment, if (multi_array)
                @max(key_align, value_align)
            else
                @alignOf(KeyValue));

            const alignment = Alignment.fromByteUnits(buffer_alignment);

            /// The header contains pointers to the variable length array(s)
            /// containing the key-value pairs.
            /// The array(s) contain exactly items as the current capacity of
            /// the hashmap, including the unusable capacity required for
            /// enforcing max load limits.
            header: if (multi_array) extern struct {
                keys: [*]Key,
                values: [*]Value,
            } else extern struct {
                kvs: [*]KeyValue,
            },
            /// The start of the metadata array, aligned to the natural
            /// alignment of a SIMD-aligned block/vector of metadata slots.
            ///
            /// The array contains `entries` individual slots followed by a
            /// single block of "mirror" slots, which mirror the state of the
            /// first block at all times.
            ///
            /// There can never be less than `Metadata.block_size` entries in an
            /// allocated buffer.
            metadata: [0]Metadata.Block,

            /// Calculates the size in bytes for a buffer with the given number
            /// of entries.
            fn calculateSize(entries: usize) usize {
                var size: usize = @sizeOf(Buffer);
                size += metadataSize(entries);
                if (comptime multi_array) {
                    size += @alignOf(Key) + @sizeOf(Key) * entries;
                    size += @alignOf(Value) + @sizeOf(Value) * entries;
                } else size += @alignOf(KeyValue) + @sizeOf(KeyValue) * entries;
                return size;
            }

            /// Allocates and initializes an empty buffer with sufficient
            /// capacity for the given number of entries.
            ///
            /// The number of entries must be a power of 2.
            fn alloc(allocator: Allocator, entries: usize) OOM!*Buffer {
                assert(isPow2(entries));
                const n = Buffer.calculateSize(entries);
                const buf = try allocator.alignedAlloc(u8, Buffer.alignment, n);
                const start = @intFromPtr(buf.ptr);

                const buffer: *Buffer = @ptrCast(buf.ptr);
                const metadata: [*]Metadata.Block = &buffer.metadata;
                @memset(metadata[0..entries], Metadata.repeat(.empty));

                var address = @intFromPtr(metadata);
                address += metadataSize(entries);

                if (comptime multi_array) {
                    address = Alignment.of(Key).forward(address);
                    const keys: [*]Key = @ptrFromInt(address);
                    @memset(keys[0..entries], undefined);
                    address += @sizeOf(Key) * entries;

                    address = Alignment.of(Value).forward(address);
                    const values: [*]Value = @ptrFromInt(address);
                    @memset(values[0..entries], undefined);
                    address += @sizeOf(Value) * entries;
                    buffer.header = .{ .keys = keys, .values = values };
                } else {
                    address = Alignment.of(KeyValue).forward(address);
                    const kvs: [*]KeyValue = @ptrFromInt(address);
                    @memset(kvs[0..entries], undefined);
                    address += @sizeOf(KeyValue) * entries;
                    buffer.header = .{ .kvs = kvs };
                }

                assert(start + buf.len >= address);
                return buffer;
            }

            /// Frees the given buffer allocation with the given amount of
            /// entries.
            fn free(self: *Buffer, allocator: Allocator, entries: usize) void {
                const n = Buffer.calculateSize(entries);
                const ptr: [*]align(buffer_alignment) u8 = @ptrCast(@alignCast(self));
                allocator.free(ptr[0..n]);
            }

            /// Returns the byte-size of the metadata array for the given number
            /// of entries.
            fn metadataSize(entries: usize) usize {
                return @sizeOf(Metadata) * (entries + Metadata.block_size);
            }
        };

        /// An abstraction for the implemented probing sequence.
        const Probe = switch (options.probing_strategy) {
            .linear => LinearProbe,
            .triangular => TriangularProbe,
            .cache_line => CacheLineProbe,
        };

        const load_factor_nths = nths(options.max_load_percentage);
        const load_min_capacity = @max((Metadata.block_size * (load_factor_nths - 1)) / load_factor_nths, 1);

        const block_align = @alignOf(Metadata.Block);
        const key_align = @alignOf(Key);
        const value_align = @alignOf(Value);
        const multi_array = switch (options.layout) {
            .auto => key_align != value_align,
            .array => false,
            .multi_array => true,
        };

        const OOM = Allocator.Error;

        /// The number of inserted key-value pairs.
        len: usize,
        /// The remaining capacity for further key-value pair insertion before
        /// rehashing or resizing is required.
        remaining_capacity: usize,
        /// The bit mask for mapping a key hash to a buffer entry index.
        entry_mask: usize,
        /// The pointer to the (block-aligned) array of metadata slots.
        ///
        /// This is a pointer into the allocated `Buffer` structure, with the
        /// buffer header followed by the metadata array and the key-value
        /// array(s).
        metadata: [*]align(block_align) Metadata,

        /// Returns an initialized hash map with sufficient capacity for at
        /// least the given number of entries.
        pub fn initCapacity(allocator: Allocator, capacity: usize) OOM!Self {
            if (capacity == 0)
                return .empty;

            const entries = try entriesForCapacity(capacity);
            const entry_mask = entries - 1;
            const remaining_capacity = applyLoadLimit(entry_mask);
            const buffer = try Buffer.alloc(allocator, entries);

            return .{
                .len = 0,
                .remaining_capacity = remaining_capacity,
                .entry_mask = entry_mask,
                .metadata = @ptrCast(&buffer.metadata),
            };
        }

        /// Deinitializes the map and deallocates its allocated buffer,
        /// if any.
        pub fn deinit(self: *Self, allocator: Allocator) void {
            if (self.getBuffer()) |buffer| {
                buffer.free(allocator, self.getEntries());
            }
        }

        /// Clears all map entries but keeps any allocated capacity.
        pub fn clear(self: *Self) void {
            if (self.noAlloc()) {
                @branchHint(.unlikely);
                assert(self.remaining_capacity == 0 and self.len == 0);
                return;
            }

            const metadata = self.getMetadataBlocks();
            @memset(metadata, Metadata.repeat(.empty));
            self.remaining_capacity = self.getUsableCapacity();
            self.len = 0;
        }

        pub fn getByPtr(self: *Self, key: *const Key) *Value {
            return @constCast(self.getConstByPtr(key));
        }

        pub fn getConstByPtr(self: *const Self, key: *const Key) *const Value {
            const buffer = self.getBuffer() orelse unreachable;
            const entry_idx: usize = if (comptime multi_array)
                @as([*]const Key, key) - buffer.header.keys
            else blk: {
                const kv: [*]const KeyValue = @fieldParentPtr("key", key);
                break :blk kv - buffer.header.kvs;
            };

            return self.getValue(entry_idx);
        }

        /// Returns the total capacity for the configured maximum
        /// load factor.
        pub fn getCapacity(self: *const Self) usize {
            return self.len + self.remaining_capacity;
        }

        pub fn cloneContext(
            self: *const Self,
            allocator: Allocator,
            ctx: Context,
        ) OOM!Self {
            const old_buffer = self.getConstBuffer() orelse return .empty;

            const entries = self.getEntries();
            const buffer = try Buffer.alloc(allocator, entries);
            var cloned = Self{
                .len = self.len,
                .remaining_capacity = undefined,
                .entry_mask = self.entry_mask,
                .metadata = @ptrCast(&buffer.metadata),
            };

            const capacity = self.getUsableCapacity() - self.len;
            if (self.remaining_capacity <= capacity / 2) {
                cloned.batchInsert(self, ctx);
                cloned.remaining_capacity = capacity;
            } else {
                @memcpy(cloned.getMetadataBlocks(), self.getMetadataBlocks());
                if (comptime multi_array) {
                    @memcpy(buffer.header.keys, old_buffer.header.keys);
                    @memcpy(buffer.header.values, old_buffer.header.values);
                } else {
                    @memcpy(buffer.header.kvs, old_buffer.header.kvs);
                }
                cloned.remaining_capacity = self.remaining_capacity;
            }

            return cloned;
        }

        const clone = if (is_zst_ctx)
            zst_ctx.clone
        else {};

        /// Reserves at least enough capacity for the given number of additional
        /// entries.
        pub fn reserveContext(
            self: *Self,
            allocator: Allocator,
            count: usize,
            ctx: Context,
        ) OOM!void {
            if (count <= self.remaining_capacity)
                return;
            try self.grow(allocator, count, ctx);
        }

        pub const reserve = if (is_zst_ctx)
            zst_ctx.reserve
        else {};

        pub fn rehashContext(self: *Self, ctx: Context) void {
            self.rehashInPlace(ctx);
            self.remaining_capacity = self.getUsableCapacity() - self.len;
        }

        pub const rehash = if (is_zst_ctx)
            zst_ctx.rehash
        else {};

        /// Returns true, if the map contains the given key.
        pub fn containsContext(
            self: *const Self,
            key: Key,
            ctx: Context,
        ) bool {
            const hash, const hint = hashKey(key, ctx);
            var probe = self.probeHash(hash);
            return self.probeGetIdx(&probe, key, hint, ctx) != null;
        }

        pub const contains = if (is_zst_ctx)
            zst_ctx.contains
        else {};

        /// Returns a pointer to the value for the given key.
        pub fn getConstPtrContext(
            self: *const Self,
            key: Key,
            ctx: Context,
        ) ?*const Value {
            const hash, const hint = hashKey(key, ctx);
            var probe = self.probeHash(hash);
            const entry_idx = self.probeGetIdx(&probe, key, hint, ctx) orelse
                return null;
            return self.getConstValue(entry_idx);
        }

        pub const getConstPtr = if (is_zst_ctx)
            zst_ctx.getConstPtr
        else {};

        /// Returns a pointer to the value for the given key.
        pub fn getPtrContext(
            self: *Self,
            key: Key,
            ctx: Context,
        ) ?*Value {
            return @constCast(self.getConstPtrContext(key, ctx));
        }

        pub const getPtr = if (is_zst_ctx)
            zst_ctx.getPtr
        else {};

        pub fn getContext(self: *const Self, key: Key, ctx: Context) ?Value {
            const ptr = self.getConstPtrContext(key, ctx) orelse return null;
            return ptr.*;
        }

        pub const get = if (is_zst_ctx)
            zst_ctx.get
        else {};

        /// Inserts the given key-value pair, silently overwriting the previous
        /// value associated with the key, if any.
        pub fn insertContext(
            self: *Self,
            allocator: Allocator,
            key: Key,
            value: Value,
            ctx: Context,
        ) OOM!void {
            _ = try self.insertFetchContext(allocator, key, value, ctx);
        }

        pub const insert = if (is_zst_ctx)
            zst_ctx.insert
        else {};

        /// Inserts the given key-value pair, silently overwriting the previous
        /// value associated with the key, if any.
        ///
        /// Asserts that there is available capacity.
        pub fn insertUncheckedContext(
            self: *Self,
            key: Key,
            value: Value,
            ctx: Context,
        ) void {
            _ = self.insertFetchUncheckedContext(key, value, ctx);
        }

        pub const insertUnchecked = if (is_zst_ctx)
            zst_ctx.insertUnchecked
        else {};

        pub fn insertFetchContext(
            self: *Self,
            allocator: Allocator,
            key: Key,
            value: Value,
            ctx: Context,
        ) OOM!?Value {
            const res = try self.getOrInsertKeyContext(allocator, key, ctx);
            switch (res) {
                .found => |ptr| {
                    const fetch = ptr.*;
                    ptr.* = value;
                    return fetch;
                },
                .inserted => |ptr| {
                    ptr.* = value;
                    return null;
                },
            }
        }

        pub const insertFetch = if (is_zst_ctx)
            zst_ctx.insertFetch
        else {};

        /// Inserts the given key-value pair and returns a copy of the previous
        /// value, if any.
        ///
        /// Asserts, that there is capacity available.
        pub fn insertFetchUncheckedContext(
            self: *Self,
            key: Key,
            value: Value,
            ctx: Context,
        ) ?Value {
            assert(self.remaining_capacity != 0);
            const res = self.insertFetchContext(undefined, key, value, ctx) catch unreachable;
            return res;
        }

        pub const insertFetchUnchecked = if (is_zst_ctx)
            zst_ctx.insertFetchUnchecked
        else {};

        /// Inserts the given key-value pair.
        ///
        /// Asserts that the key does not yet exist.
        pub fn insertUniqueContext(
            self: *Self,
            allocator: Allocator,
            key: Key,
            value: Value,
            ctx: Context,
        ) OOM!void {
            const hash, const hint = hashKey(key, ctx);
            var entry_idx = self.findInsertIdx(hash);

            if (self.metadata[entry_idx].isEmpty()) {
                if (self.remaining_capacity == 0) {
                    @branchHint(.unlikely);
                    try self.grow(allocator, 1, ctx);
                    entry_idx = self.findInsertIdx(hash);
                }

                self.remaining_capacity -= 1;
            }

            self.insertKey(entry_idx, key, hint);
            self.getValue(entry_idx).* = value;
        }

        pub const insertUnique = if (is_zst_ctx)
            zst_ctx.insertUnique
        else {};

        /// Inserts the given key-value pair.
        ///
        /// Asserts that there is available capacity and the key does
        /// not yet exist.
        pub fn insertUniqueUncheckedContext(
            self: *Self,
            key: Key,
            value: Value,
            ctx: Context,
        ) void {
            assert(self.remaining_capacity != 0);
            self.insertUniqueContext(undefined, key, value, ctx) catch unreachable;
        }

        pub const insertUniqueUnchecked = if (is_zst_ctx)
            zst_ctx.insertUniqueUnchecked
        else {};

        /// Returns a pointer to the value for the given key or inserts the key
        /// and returns a pointer to the uninitialized value.
        pub fn getOrInsertKeyContext(
            self: *Self,
            allocator: Allocator,
            key: Key,
            ctx: Context,
        ) OOM!GetOrInsert {
            const hash, const hint = hashKey(key, ctx);
            var probe = self.probeHash(hash);

            var entry_idx, const found = self.probeGetOrInsertIdx(&probe, key, hint, ctx);
            if (found) {
                return .{ .found = self.getValue(entry_idx) };
            }

            if (self.metadata[entry_idx].isEmpty()) {
                if (self.remaining_capacity == 0) {
                    @branchHint(.unlikely);
                    try self.grow(allocator, 1, ctx);
                    entry_idx = self.findInsertIdx(hash);
                }

                self.remaining_capacity -= 1;
            }

            self.insertKey(entry_idx, key, hint);
            return .{ .inserted = self.getValue(entry_idx) };
        }

        pub const getOrInsertKey = if (is_zst_ctx)
            zst_ctx.getOrInsertKey
        else {};

        /// Returns a pointer to the value of the key if it exists or inserts
        /// the key and returns a pointer to the uninitialized value.
        // FIXME: what is unchecked here? The capacity! is this a useful API?
        pub fn getOrInsertKeyUncheckedContext(
            self: *Self,
            key: Key,
            ctx: Context,
        ) GetOrInsert {
            assert(self.remaining_capacity != 0);
            const res = self.getOrInsertKeyContext(undefined, key, ctx) catch unreachable;
            return res;
        }

        pub const getOrInsertKeyUnchecked = if (is_zst_ctx)
            zst_ctx.getOrInsertKeyUnchecked
        else {};

        pub fn getOrInsertContext(
            self: *Self,
            allocator: Allocator,
            key: Key,
            value: Value,
            ctx: Context,
        ) OOM!?Value {
            const res = try self.getOrInsertKeyContext(allocator, key, ctx);
            switch (res) {
                .found => |ptr| return ptr.*,
                .inserted => |ptr| {
                    ptr.* = value;
                    return null;
                },
            }
        }

        pub const getOrInsert = if (is_zst_ctx)
            zst_ctx.getOrInsert
        else {};

        pub fn getOrInsertUncheckedContext(
            self: *Self,
            key: Key,
            value: Value,
            ctx: Context,
        ) ?Value {
            assert(self.remaining_capacity != 0);
            return self.getOrInsertContext(undefined, key, value, ctx) catch unreachable;
        }

        pub const getOrInsertUnchecked = if (is_zst_ctx)
            zst_ctx.getOrInsertUnchecked
        else {};

        /// Removes the given key and its associated value and returns true, if
        /// a key-value pair was actually removed.
        pub fn removeContext(self: *Self, key: Key, ctx: Context) bool {
            return self.removeFetchContext(key, ctx) != null;
        }

        pub const remove = if (is_zst_ctx)
            zst_ctx.remove
        else {};

        /// Removes the given key and its associated value and returns the
        /// removed value, if any.
        pub fn removeFetchContext(self: *Self, key: Key, ctx: Context) ?Value {
            const hash, const hint = hashKey(key, ctx);
            var probe = self.probeHash(hash);

            const entry_idx = self.probeGetIdx(&probe, key, hint, ctx) orelse return null;
            if (self.isLastInSequence(&probe, entry_idx)) {
                self.remaining_capacity += 1;
                self.insertMetadata(entry_idx, .empty);
            } else self.insertMetadata(entry_idx, .deleted);
            self.len -= 1;

            return self.getValue(entry_idx).*;
        }

        pub const removeFetch = if (is_zst_ctx)
            zst_ctx.removeFetch
        else {};

        fn isLastInSequence(self: *const Self, probe: *Probe, entry_idx: usize) bool {
            const relative_idx = self.getRelativeIdx(probe.pos, entry_idx);
            assert(relative_idx < Metadata.block_size);
            if (relative_idx < Metadata.block_size - 1) {
                // Check, if there is a subsequent empty slot after the entry
                // index in same block relative to the last probing position.
                return self.metadata[entry_idx + 1].isEmpty();
            } else {
                // Otherwise, check if the next slot in the probing sequence
                // is empty.
                const next = probe.next(self.entry_mask);
                return self.metadata[next].isEmpty();
            }
        }

        fn grow(
            self: *Self,
            allocator: Allocator,
            count: usize,
            ctx: Context,
        ) OOM!void {
            @branchHint(.cold);
            assert(self.remaining_capacity < count);
            const old_cap = self.getUsableCapacity();
            const new_cap = try overflowingAdd(self.len, count);

            // If the  required capacity would comfortably fit into the current
            // buffer allocation, rehash all entries to free additional capacity
            // from deleted entries.
            if (new_cap < old_cap / 2) {
                self.rehashInPlace(ctx);
                self.remaining_capacity = old_cap - self.len;
                return;
            }

            // Otherwise, at least double the allocation.
            const new_entries = try entriesForCapacity(@max(old_cap + 1, new_cap));
            const entry_mask = new_entries - 1;
            const buffer = try Buffer.alloc(allocator, new_entries);

            var old_table = self.*;
            self.* = .{
                .len = old_table.len,
                .remaining_capacity = applyLoadLimit(entry_mask) - old_table.len,
                .entry_mask = entry_mask,
                .metadata = @ptrCast(&buffer.metadata),
            };

            self.batchInsert(&old_table, ctx);
            if (old_table.getBuffer()) |old_buffer|
                old_buffer.free(allocator, old_table.getEntries());
        }

        fn rehashInPlace(self: *Self, ctx: Context) void {
            // Prepare all metadata slots for rehashing.
            // Converts all slots from:
            //     - empty   -> empty
            //     - deleted -> empty
            //     - used    -> deleted
            //
            // Empty slots can be freely reused, deleted slots indicate entries
            // that need to be rehashed and reinserted.
            const vectors = self.getMetadataBlocks();
            for (vectors[0 .. vectors.len - 1]) |*vector|
                vector.* = Metadata.prepareRehash(vector.*);

            outer: for (0..self.getEntries()) |i| {
                if (self.metadata[i] != Metadata.deleted)
                    continue;

                const key = self.getKey(i);
                inner: while (true) {
                    const hash, const hint = hashKey(key.*, ctx);
                    var probe = self.probeHash(hash);

                    // If the new insert index is located in the same block as
                    // the old index, allow the entry to remain in its previous
                    // place.
                    const entry_idx = self.probeInsertIdx(&probe);
                    if (self.getBlockIdx(probe.pos, i) == self.getBlockIdx(probe.pos, entry_idx)) {
                        @branchHint(.likely);
                        self.metadata[i] = hint;
                        continue :outer;
                    }

                    const value = self.getValue(i);
                    const insert_key = self.getKey(entry_idx);
                    const insert_value = self.getValue(entry_idx);

                    const metadata = self.metadata[entry_idx];
                    self.metadata[entry_idx] = hint;

                    if (metadata == Metadata.empty) {
                        insert_key.* = key.*;
                        insert_value.* = value.*;
                        continue :outer;
                    } else {
                        assert(metadata == Metadata.deleted);
                        mem.swap(Key, key, insert_key);
                        mem.swap(Value, value, insert_value);
                        continue :inner;
                    }
                }
            }

            // Ensure the final metadata block mirrors the first block.
            vectors[vectors.len - 1] = vectors[0];
        }

        fn batchInsert(self: *Self, other: *const Self, ctx: Context) void {
            const other_vectors = other.getConstMetadataBlocks();
            for (other_vectors[0 .. other_vectors.len - 1], 0..) |vector, v| {
                const block_idx = v * Metadata.block_size;
                var used: Metadata.BitMask = @bitCast(Metadata.findUsed(vector));
                while (nextBit(&used)) |idx| {
                    const entry_idx = block_idx + idx;
                    const key = other.getConstKey(entry_idx);
                    const value = other.getConstValue(entry_idx);

                    const hash, const hint = hashKey(key.*, ctx);
                    const insert_idx = self.findInsertIdx(hash);
                    self.metadata[insert_idx] = hint;
                    const new_key = self.getKey(insert_idx);
                    const new_value = self.getValue(insert_idx);
                    new_key.* = key.*;
                    new_value.* = value.*;
                }
            }

            // Ensure the final metadata block mirrors the first block.
            const vectors = self.getMetadataBlocks();
            vectors[vectors.len - 1] = vectors[0];
        }

        fn insertKey(self: *Self, entry_idx: usize, key: Key, hint: Metadata) void {
            self.insertMetadata(entry_idx, hint);
            self.getKey(entry_idx).* = key;
            self.len += 1;
        }

        fn insertMetadata(self: *Self, entry_idx: usize, metadata: Metadata) void {
            const mirror_idx = self.getMirrorIdx(entry_idx);
            if (entry_idx != mirror_idx) {
                @branchHint(.unlikely);
                self.metadata[mirror_idx] = metadata;
            }

            self.metadata[entry_idx] = metadata;
        }

        fn probeGetOrInsertIdx(
            self: *const Self,
            probe: *Probe,
            key: Key,
            hint: Metadata,
            ctx: Context,
        ) struct { usize, bool } {
            var insert_idx: ?usize = null;

            while (true) {
                const block = self.getMetadataBlock(probe.pos);
                if (Metadata.findHint(block, hint)) |relative_idx| {
                    const metadata_idx = metadataIdx(probe, relative_idx);
                    if (self.eqlKey(key, hint, metadata_idx, ctx)) |entry_idx|
                        return .{ entry_idx, true };
                }

                if (insert_idx == null) {
                    if (Metadata.findUnused(block)) |relative_idx|
                        insert_idx = metadataIdx(probe, relative_idx) & self.entry_mask;
                }

                if (insert_idx) |idx| {
                    if (Metadata.findEmpty(block)) |_|
                        return .{ idx, false };
                }

                _ = probe.next(self.entry_mask);
            }
        }

        fn probeGetIdx(
            self: *const Self,
            probe: *Probe,
            key: Key,
            hint: Metadata,
            ctx: Context,
        ) ?usize {
            while (true) {
                // Search for a metadata block containing the correct hash hint
                // for the queried key in accordance with the probing sequence.
                const block = self.getMetadataBlock(probe.pos);
                if (Metadata.findHint(block, hint)) |relative_idx| {
                    const metadata_idx = metadataIdx(probe, relative_idx);
                    if (self.eqlKey(key, hint, metadata_idx, ctx)) |entry_idx|
                        return entry_idx;
                }

                // Upon encountering an empty slot within a probed block stop
                // searchin further.
                if (Metadata.findEmpty(block)) |_|
                    return null;

                // Try the next vector in the probe sequence.
                _ = probe.next(self.entry_mask);
            }
        }

        // Finds and returns the "natural" insertion index for the given hash as
        // well as the first possible insertion index, without checking if the
        // key already exists.
        fn findInsertIdx(
            self: *const Self,
            hash: Hash,
        ) usize {
            var probe = self.probeHash(hash);
            return self.probeInsertIdx(&probe);
        }

        fn probeInsertIdx(self: *const Self, probe: *Probe) usize {
            while (true) {
                const block = self.getMetadataBlock(probe.pos); // IDEA: return a bool indicating if we need to wrap?
                if (Metadata.findUnused(block)) |relative_idx| {
                    const entry_idx = metadataIdx(probe, relative_idx) & self.entry_mask;
                    return entry_idx;
                }
                _ = probe.next(self.entry_mask);
            }
        }

        fn probeHash(self: *const Self, hash: Hash) Probe {
            const entry_idx = self.getEntry(hash);
            return .start(entry_idx);
        }

        /// Checks the hash hint for the given metadata slot index (may be a
        /// mirror slot) and if it's equal checks the actual key for equality.
        /// If both equality checks suceed, returns the index of the matching
        /// key-value pair.
        fn eqlKey(
            self: *const Self,
            key: Key,
            hint: Metadata,
            metadata_idx: usize,
            ctx: Context,
        ) ?usize {
            // Determining a hash hint mismatch early allows us to avoid having
            // to check for key equality in most cases.
            if (hint.hash_hint != self.metadata[metadata_idx].hash_hint)
                return null;

            // Due to the mirror metadata slot we can use the given idx directly
            // for the metadata lookup, but not for looking up the key entry.
            const entry_idx = metadata_idx & self.entry_mask;
            return if (ctx.eql(key, self.getConstKey(entry_idx).*))
                entry_idx
            else
                null;
        }

        /// Returs a pointer to the current buffer allocation, if any.
        fn getBuffer(self: *Self) ?*Buffer {
            return @constCast(self.getConstBuffer());
        }

        /// Returs a const pointer to the current buffer allocation, if any.
        fn getConstBuffer(self: *const Self) ?*const Buffer {
            if (self.noAlloc())
                return null;

            const metadata: *const [0]Metadata.Block = @ptrCast(self.metadata);
            const buffer: *const Buffer = @fieldParentPtr("metadata", metadata);
            return buffer;
        }

        /// Reads and returns the (possibly unaligned) block of metadata slots
        /// starting at the given index.
        ///
        /// This is always valid for any entry index, because the metadata slot
        /// array must at all times contain an additional mirror block at the
        /// end.
        ///
        /// It is even valid with an empty map with a pointer to the static
        /// "empty" buffer, since the entry mask 0 will ensure that all hashes
        /// map to index 0.
        fn getMetadataBlock(self: *const Self, entry_idx: usize) Metadata.Block {
            if (comptime options.probing_strategy == .cache_line) {
                // Check, if the metadata block would cross a cache-line
                // boundary. If yes, wrap around at the end of the cache-line.
                const end = (entry_idx + cache_line.len) & cache_line.mask;
                const len = end - entry_idx;
                if (len < Metadata.block_size) {
                    // Calculate the number of metadata slots that must be read
                    // from the start of the cache.line.
                    const remaining_len = Metadata.block_size - len;
                    const start = entry_idx & cache_line.mask;

                    var block: [Metadata.block_size]Metadata = undefined;
                    @memcpy(block[0..len], self.metadata[entry_idx..][0..len]);
                    @memcpy(block[len..], self.metadata[start..][0..remaining_len]);
                    return @bitCast(block);
                }
            }

            return @bitCast(self.metadata[entry_idx..][0..Metadata.block_size].*);
        }

        fn getAlignedMetadaBlock(self: *Self, block_idx: usize) *Metadata.Block {
            const vectors = self.getMetadataBlocks();
            return &vectors[block_idx];
        }

        /// Returns a slice of all metadata blocks including the mirror slot
        /// block.
        fn getMetadataBlocks(self: *Self) []Metadata.Block {
            return @constCast(self.getConstMetadataBlocks());
        }

        /// Returns a slice of all metadata blocks including the mirror slot
        /// block.
        fn getConstMetadataBlocks(self: *const Self) []const Metadata.Block {
            const ptr: [*]const Metadata.Block = @ptrCast(self.metadata);
            const len = (self.entry_mask + 1 + Metadata.block_size) / Metadata.block_size;
            return ptr[0..len];
        }

        fn getKey(self: *Self, entry_idx: usize) *Key {
            return @constCast(self.getConstKey(entry_idx));
        }

        fn getConstKey(self: *const Self, entry_idx: usize) *const Key {
            const buffer = self.getConstBuffer() orelse unreachable;
            return if (comptime multi_array)
                &buffer.header.keys[entry_idx]
            else
                &buffer.header.kvs[entry_idx].key;
        }

        fn getValue(self: *Self, entry_idx: usize) *Value {
            return @constCast(self.getConstValue(entry_idx));
        }

        fn getConstValue(self: *const Self, entry_idx: usize) *const Value {
            const buffer = self.getConstBuffer() orelse unreachable;
            return if (comptime multi_array)
                &buffer.header.values[entry_idx]
            else
                &buffer.header.kvs[entry_idx].value;
        }

        /// Returns the current number of entries in the buffer, including the
        /// unusuable capacity reserved for enforcing load limits.
        fn getEntries(self: *const Self) usize {
            return self.entry_mask + 1;
        }

        /// Returns the metadata and entry index for the given hash.
        fn getEntry(self: *const Self, hash: Hash) usize {
            return @truncate(hash & self.entry_mask);
        }

        fn getBlockIdx(self: *const Self, base_idx: usize, entry_idx: usize) usize {
            return self.getRelativeIdx(base_idx, entry_idx) / Metadata.block_size;
        }

        fn getRelativeIdx(self: *const Self, base_idx: usize, entry_idx: usize) usize {
            return if (comptime options.probing_strategy == .cache_line)
                (entry_idx -% base_idx) & (cache_line.len - 1) & self.entry_mask
            else
                (entry_idx -% base_idx) & self.entry_mask;
        }

        fn getMirrorIdx(self: *const Self, entry_idx: usize) usize {
            return ((entry_idx -% Metadata.block_size) & self.entry_mask) + Metadata.block_size;
        }

        fn getUsableCapacity(self: *const Self) usize {
            return if (self.entry_mask == 0) 0 else applyLoadLimit(self.entry_mask);
        }

        fn noAlloc(self: *const Self) bool {
            const ptr: [*]const Metadata = @ptrCast(&empty_vector);
            return self.metadata == ptr;
        }

        fn containsKeyPtr(self: *const Self, key: *const Key) bool {
            const buffer = self.getConstBuffer() orelse unreachable;
            const entries = self.getEntries();
            const ptr: [*]const Key = @ptrCast(key);
            if (comptime multi_array) {
                const keys = buffer.header.keys[0..entries];
                return keys.ptr <= ptr and ptr <= keys.ptr + keys.len;
            } else {
                const kvs = buffer.header.kvs[0..entries];
                return kvs.ptr <= ptr and ptr <= kvs.ptr + kvs.len;
            }
        }

        fn metadataIdx(probe: *const Probe, relative_idx: usize) usize {
            return if (comptime options.probing_strategy == .cache_line)
                (probe.pos & cache_line.mask) + ((probe.pos + relative_idx) & (cache_line.len - 1))
            else
                probe.pos + relative_idx;
        }

        fn hashKey(key: Key, ctx: Context) struct { Hash, Metadata } {
            const hash: Hash = ctx.hash(key);
            return .{ hash, .hashHint(hash) };
        }

        // at least 1 free entry at all times!
        // load factor configurable at comptime!
        // always a power of 2!
        fn entriesForCapacity(min_capacity: usize) OOM!usize {
            assert(min_capacity != 0);
            if (min_capacity < load_min_capacity) {
                @branchHint(.unlikely);
                return Metadata.block_size;
            }

            const adjusted_cap = if (comptime load_factor_nths == 100)
                min_capacity + 1
            else blk: {
                const prod = try overflowingMul(min_capacity, load_factor_nths);
                break :blk prod / (load_factor_nths - 1);
            };

            return nextPow2(adjusted_cap);
        }

        /// Returns the total usable capacity of the current buffer.
        fn applyLoadLimit(entry_mask: usize) usize {
            const entry_len = entry_mask + 1;
            if (entry_len <= Metadata.block_size)
                return load_min_capacity;

            return if (comptime load_factor_nths == 100)
                entry_len - 1
            else
                entry_len * (load_factor_nths - 1) / load_factor_nths;
        }

        const is_zst_ctx = @sizeOf(Context) == 0;
        const zst_ctx = struct {
            /// Reserves at least enough capacity for the given number of additional
            /// entries.
            pub fn reserve(
                self: *Self,
                allocator: Allocator,
                count: usize,
            ) OOM!void {
                return self.reserveContext(allocator, count, undefined);
            }

            pub fn rehash(self: *Self) void {
                return self.rehashContext(undefined);
            }

            pub fn cloneContext(
                self: *const Self,
                allocator: Allocator,
            ) OOM!Self {
                return self.cloneContext(allocator, undefined);
            }

            /// Returns true, if the map contains the given key.
            pub fn contains(self: *const Self, key: Key) bool {
                return self.containsContext(key, undefined);
            }

            /// Returns a pointer to the value for the given key.
            pub fn getConstPtr(self: *const Self, key: Key) ?*const Value {
                return self.getConstPtrContext(key, undefined);
            }

            /// Returns a pointer to the value for the given key.
            pub fn getPtr(self: *Self, key: Key) ?*Value {
                return self.getPtrContext(key, undefined);
            }

            /// Returns a copy of the value for the given key.
            pub fn get(self: *Self, key: Key) ?Value {
                return self.getContext(key, undefined);
            }

            /// Inserts the given key-value pair, overwriting the previous
            /// value associated to that key, if any.
            pub fn insert(
                self: *Self,
                allocator: Allocator,
                key: Key,
                value: Value,
            ) OOM!void {
                return self.insertContext(allocator, key, value, undefined);
            }

            /// Inserts the given key-value pair, overwriting the previous
            /// value associated to that key, if any.
            ///
            /// Asserts that there is available capacity.
            pub fn insertUnchecked(self: *Self, key: Key, value: Value) void {
                return self.insertUncheckedContext(key, value, undefined);
            }

            /// Inserts the given key-value pair and returns a copy of the
            /// previous value for that key, if any.
            pub fn insertFetch(
                self: *Self,
                allocator: Allocator,
                key: Key,
                value: Value,
            ) OOM!?Value {
                return self.insertFetchContext(allocator, key, value, undefined);
            }

            /// Inserts the given key-value pair and returns a copy of the
            /// previous value for that key, if any.
            ///
            /// Asserts, that there is capacity available.
            pub fn insertFetchUnchecked(
                self: *Self,
                key: Key,
                value: Value,
            ) ?Value {
                return self.insertFetchUncheckedContext(key, value, undefined);
            }

            pub fn insertUnique(
                self: *Self,
                allocator: Allocator,
                key: Key,
                value: Value,
            ) OOM!void {
                return self.insertUniqueContext(allocator, key, value, undefined);
            }

            /// Inserts the given key-value pair.
            ///
            /// Asserts that there is available capacity and the key does
            /// not yet exist.
            pub fn insertUniqueUnchecked(self: *Self, key: Key, value: Value) void {
                return self.insertUniqueUncheckedContext(key, value, undefined);
            }

            /// Returns a pointer to the value for the given key or inserts the
            /// key and returns a pointer to the uninitialized value.
            pub fn getOrInsertKey(
                self: *Self,
                allocator: Allocator,
                key: Key,
            ) OOM!GetOrInsert {
                return self.getOrInsertKeyContext(allocator, key, undefined);
            }

            pub fn getOrInsertKeyUnchecked(self: *Self, key: Key) GetOrInsert {
                return self.getOrInsertKeyUncheckedContext(key, undefined);
            }

            pub fn getOrInsert(
                self: *Self,
                allocator: Allocator,
                key: Key,
                value: Value,
            ) OOM!?Value {
                return self.getOrInsertContext(allocator, key, value, undefined);
            }

            pub fn getOrInsertUnchecked(
                self: *Self,
                key: K,
                value: V,
                ctx: Context,
            ) GetOrInsert {
                return self.getOrInsertUncheckedContext(key, value, ctx);
            }

            pub fn remove(self: *Self, key: Key) bool {
                return self.removeContext(key, undefined);
            }

            pub fn removeFetch(self: *Self, key: Key) ?Value {
                return self.removeFetchContext(key, undefined);
            }
        };
    };
}

const Metadata = packed struct(u8) {
    const HashHint = u7;

    const Block = @Vector(block_size, u8);
    const BitVector = @Vector(block_size, bool);
    const BitMask = std.meta.Int(.unsigned, block_size);

    const block_size = 16;
    const block_mask = block_size - 1;
    const vector_indices: Block(block_size, u8) = blk: {
        var arr: [block_size]u8 = undefined;
        for (&arr, 0..) |*idx, i|
            idx.* = @intCast(i);
        break :blk arr;
    };

    const empty: Metadata = .{ .hash_hint = ~@as(HashHint, 0) };
    const deleted: Metadata = .{ .hash_hint = 0 };

    hash_hint: HashHint,
    free: bool = true,

    /// Returns the index of the slot within the block that equals the given
    /// hint or null.
    fn findHint(block: Metadata.Block, hint: Metadata) ?usize {
        const mask = Metadata.repeat(hint);
        const found = block == mask;
        const bits: BitMask = @bitCast(found);
        return if (bits == 0) null else @ctz(bits);
    }

    /// Returns the index of the first slot within the block that is either
    /// empty or deleted or null.
    fn findUnused(block: Metadata.Block) ?usize {
        const pred = block >= Metadata.repeat(.deleted);
        const bits: BitMask = @bitCast(pred);
        return if (bits == 0) null else @ctz(bits);
    }

    /// Returns the index of the first slot within the block that is empty or
    /// null.
    fn findEmpty(vector: Metadata.Block) ?usize {
        return Metadata.findHint(vector, .empty);
    }

    fn findUsed(block: Metadata.Block) BitVector {
        return block < Metadata.repeat(.deleted);
    }

    fn prepareRehash(block: Metadata.Block) Metadata.Block {
        // All slots with the MSB set (empty and deleted).
        const pred = block >= Metadata.repeat(.deleted);
        //const pred = vector & 0x80 != 0;
        // Mark all populated entries as deleted and all others as empty.
        return @select(u8, pred, Metadata.repeat(.empty), Metadata.repeat(.deleted));
    }

    fn repeat(self: Metadata) Metadata.Block {
        const bits: u8 = @bitCast(self);
        return @splat(bits);
    }

    fn hashHint(hash: Hash) Metadata {
        const hint = extractHashHint(hash);
        return .{ .hash_hint = hint, .free = false };
    }

    fn insert(self: *Metadata, hash: Hash) void {
        assert(self.free == true);
        const hint = extractHashHint(hash);
        self.* = .{
            .hash_hint = hint,
            .free = false,
        };
    }

    fn isEmpty(self: Metadata) bool {
        return self == Metadata.empty;
    }

    fn extractHashHint(hash: Hash) HashHint {
        const shift = @typeInfo(Hash).int.bits - @typeInfo(HashHint).int.bits;
        return @truncate(hash >> shift);
    }
};

const LinearProbe = struct {
    pos: usize,

    fn start(entry_idx: usize) LinearProbe {
        return .{ .pos = entry_idx };
    }

    fn next(self: *LinearProbe, entry_mask: usize) usize {
        self.pos = (self.pos + Metadata.block_size) & entry_mask;
        return self.pos;
    }
};

const TriangularProbe = struct {
    pos: usize,
    stride: usize = Metadata.block_size,

    fn start(entry_idx: usize) TriangularProbe {
        return .{ .pos = entry_idx, .stride = Metadata.block_size };
    }

    fn next(self: *TriangularProbe, entry_mask: usize) usize {
        const pos = (self.pos + self.stride) & entry_mask;
        self.pos = pos;
        self.stride += Metadata.block_size;
        return pos;
    }
};

const CacheLineProbe = struct {
    pos: usize,
    origin: usize,

    fn start(entry_idx: usize) CacheLineProbe {
        return .{ .pos = entry_idx, .origin = entry_idx };
    }

    fn next(self: *CacheLineProbe, entry_mask: usize) usize {
        const tentative_next = self.pos + Metadata.block_size;
        if (self.origin != tentative_next) {
            @branchHint(.likely);
            const cache_line_start = self.origin & cache_line.mask;
            self.pos = (cache_line_start + (tentative_next & (cache_line.len - 1))) & entry_mask;
            return self.pos;
        }

        self.pos = (self.origin + cache_line.len) & entry_mask;
        self.origin = self.pos;
        return self.pos;
    }
};

const cache_line = struct {
    const len: usize = std.atomic.cache_line;
    const mask: usize = ~@as(usize, cache_line.len - 1);
};

test "cache line probe" {
    // FIXME: this test is broken if cache line != 64
    var entry_mask: usize = 256 - 1;
    var probe = CacheLineProbe(64).start(52);
    try tt.expectEqual(4, probe.next(entry_mask));
    try tt.expectEqual(20, probe.next(entry_mask));
    try tt.expectEqual(36, probe.next(entry_mask));
    // jump to 2nd cacheline
    try tt.expectEqual(116, probe.next(entry_mask));
    try tt.expectEqual(68, probe.next(entry_mask));
    try tt.expectEqual(84, probe.next(entry_mask));
    try tt.expectEqual(100, probe.next(entry_mask));
    // jump to 3rd cache line
    try tt.expectEqual(180, probe.next(entry_mask));
    try tt.expectEqual(132, probe.next(entry_mask));
    try tt.expectEqual(148, probe.next(entry_mask));
    try tt.expectEqual(164, probe.next(entry_mask));
    // jump to 4th cache line
    try tt.expectEqual(244, probe.next(entry_mask));
    try tt.expectEqual(196, probe.next(entry_mask));
    try tt.expectEqual(212, probe.next(entry_mask));
    try tt.expectEqual(228, probe.next(entry_mask));
    // jump back to 1st cache line
    try tt.expectEqual(52, probe.next(entry_mask));
    try tt.expectEqual(4, probe.next(entry_mask));

    entry_mask = 32 - 1;
    probe = CacheLineProbe(64).start(28);
    try tt.expectEqual(12, probe.next(entry_mask));
    try tt.expectEqual(28, probe.next(entry_mask));
    try tt.expectEqual(12, probe.next(entry_mask));
    try tt.expectEqual(28, probe.next(entry_mask));
}

test "metadata find hint" {
    const hash1: Metadata = .{ .hash_hint = 0b1101101, .free = false };
    const hash2: Metadata = .{ .hash_hint = 0b0011010, .free = false };

    var block = Metadata.repeat(.empty);
    block[11] = @bitCast(hash1);
    try tt.expectEqual(11, Metadata.findHint(block, hash1));

    block = Metadata.repeat(hash2);
    block[0] = @bitCast(Metadata.empty);
    block[4] = @bitCast(Metadata.empty);
    block[8] = @bitCast(Metadata.empty);
    block[12] = @bitCast(Metadata.deleted);

    try tt.expectEqual(null, Metadata.findHint(block, hash1));
    block[11] = @bitCast(hash1);
    try tt.expectEqual(11, Metadata.findHint(block, hash1));
}

test "metadata find unused" {
    const random_hash: Metadata = .{ .hash_hint = 0b1101101, .free = false };

    var block = Metadata.repeat(random_hash);
    try tt.expectEqual(null, Metadata.findUnused(block));

    block = Metadata.repeat(random_hash);
    block[5] = @bitCast(Metadata.empty);
    try tt.expectEqual(5, Metadata.findUnused(block));

    block = Metadata.repeat(random_hash);
    block[9] = @bitCast(Metadata.deleted);
    try tt.expectEqual(9, Metadata.findUnused(block));
}

test "metadata prepare rehash" {
    const random_hash: Metadata = @bitCast(@as(u8, 0b0101_1001));
    try tt.expectEqual(false, random_hash.free);
    try tt.expectEqual(0b1011001, random_hash.hash_hint);

    var vec = Metadata.repeat(random_hash);
    vec = Metadata.prepareRehash(vec);
    try tt.expectEqual(Metadata.repeat(.deleted), vec);

    vec = Metadata.repeat(.empty);
    vec = Metadata.prepareRehash(vec);
    try tt.expectEqual(Metadata.repeat(.empty), vec);

    vec = Metadata.repeat(.deleted);
    vec = Metadata.prepareRehash(vec);
    try tt.expectEqual(Metadata.repeat(.empty), vec);
}

fn nextBit(mask: *u16) ?usize {
    const curr = mask.*;
    if (curr == 0)
        return null;

    const lsb = curr & ~(curr - 1);
    const idx: usize = @ctz(curr);
    mask.* = curr ^ lsb;
    return idx;
}

test "nextBit" {
    var mask: u16 = 0b1001_0111;
    try tt.expectEqual(0, nextBit(&mask));
    try tt.expectEqual(1, nextBit(&mask));
    try tt.expectEqual(2, nextBit(&mask));
    try tt.expectEqual(4, nextBit(&mask));
    try tt.expectEqual(7, nextBit(&mask));
    try tt.expectEqual(null, nextBit(&mask));
}

fn overflowingAdd(a: usize, b: usize) Allocator.Error!usize {
    const res, const overflow = @addWithOverflow(a, b);
    if (overflow != 0)
        return error.OutOfMemory;
    return res;
}

fn overflowingMul(a: usize, b: usize) Allocator.Error!usize {
    const res, const overflow = @mulWithOverflow(a, b);
    if (overflow != 0)
        return error.OutOfMemory;
    return res;
}

fn isPow2(v: usize) bool {
    assert(v != 0);
    return (v & (v - 1) == 0);
}

fn nextPow2(v: usize) Allocator.Error!usize {
    if (v & (v - 1) == 0)
        return v;

    const log = log2(v);
    if (log == @bitSizeOf(usize) - 1)
        return error.OutOfMemory;
    return @as(usize, 1) << (log + 1);
}

test "next power of 2" {
    const testing = std.testing;
    try testing.expectEqual(32, try nextPow2(17));
    try testing.expectEqual(32, try nextPow2(31));
    try testing.expectEqual(32, try nextPow2(32));
    try testing.expectEqual(128, try nextPow2(128));
    try testing.expectError(error.OutOfMemory, nextPow2(~@as(usize, 0)));
}

const Log2Int = math.Log2Int(usize);

fn log2(v: usize) Log2Int {
    const bits = @bitSizeOf(usize);
    return @intCast(bits - 1 - @clz(v - 1));
}

fn nths(percent: u8) usize {
    assert(percent >= 50 and percent <= 100);
    return if (percent == 100) 100 else 100 / (100 - percent);
}

const std = @import("std");
const math = std.math;
const mem = std.mem;
const tt = std.testing;

const assert = std.debug.assert;

const Alignment = std.mem.Alignment;
const Allocator = std.mem.Allocator;
const Wyhash = std.hash.Wyhash;

const Map = ContextHashMap(i32, i32, OneToOne, .default);
const OneToOne = struct {
    pub fn hash(_: OneToOne, key: i32) u64 {
        const unsigned: u64 = @bitCast(-@as(i64, key));
        return unsigned;
    }

    pub fn eql(_: OneToOne, a: i32, b: i32) bool {
        return a == b;
    }
};

test "nths" {
    try tt.expectEqual(100, nths(100));
    try tt.expectEqual(100, nths(99));
    try tt.expectEqual(50, nths(98));
    try tt.expectEqual(33, nths(97));
    try tt.expectEqual(25, nths(96));
    try tt.expectEqual(20, nths(95));
    try tt.expectEqual(12, nths(92));
    try tt.expectEqual(11, nths(91));
    try tt.expectEqual(10, nths(90));
    try tt.expectEqual(9, nths(89));
    try tt.expectEqual(8, nths(88));
    try tt.expectEqual(7, nths(87));
    try tt.expectEqual(7, nths(86));
    try tt.expectEqual(6, nths(85));
    try tt.expectEqual(6, nths(84));
    try tt.expectEqual(5, nths(83));
    try tt.expectEqual(5, nths(80));
    try tt.expectEqual(5, nths(80));
    try tt.expectEqual(4, nths(79));
    try tt.expectEqual(4, nths(75));
    try tt.expectEqual(3, nths(74));
    try tt.expectEqual(3, nths(67));
    try tt.expectEqual(2, nths(66));
    try tt.expectEqual(2, nths(50));
}

test "entries for capacity" {
    // default max load is 87.5%
    try tt.expectEqual(8, Map.load_factor_nths);
    try tt.expectEqual(16, Map.entriesForCapacity(1));
    try tt.expectEqual(16, Map.entriesForCapacity(2));
    try tt.expectEqual(16, Map.entriesForCapacity(12));
    try tt.expectEqual(16, Map.entriesForCapacity(13));
    try tt.expectEqual(16, Map.entriesForCapacity(14));
    try tt.expectEqual(32, Map.entriesForCapacity(15));
    try tt.expectEqual(32, Map.entriesForCapacity(16));
    try tt.expectEqual(64, Map.entriesForCapacity(51));
    try tt.expectEqual(64, Map.entriesForCapacity(52));

    const MapLoad88 = ContextHashMap(i32, i32, struct {}, .{ .max_load_percentage = 88 });
    try tt.expectEqual(8, MapLoad88.load_factor_nths);
    try tt.expectEqual(128, MapLoad88.entriesForCapacity(112));
    try tt.expectEqual(256, MapLoad88.entriesForCapacity(113));
    try tt.expectEqual(112, MapLoad88.applyLoadLimit(128 - 1));

    // max load 100% must still reserve at least one additional entry
    const MapLoad100 = ContextHashMap(i32, i32, struct {}, .{ .max_load_percentage = 100 });
    try tt.expectEqual(100, MapLoad100.load_factor_nths);
    try tt.expectEqual(16, MapLoad100.entriesForCapacity(1));
    try tt.expectEqual(16, MapLoad100.entriesForCapacity(15));
    try tt.expectEqual(32, MapLoad100.entriesForCapacity(31));
    try tt.expectEqual(256, MapLoad100.entriesForCapacity(255));
    try tt.expectEqual(127, MapLoad100.applyLoadLimit(128 - 1));
}

test "empty map" {
    var map: Map = .empty;
    defer map.deinit(tt.allocator);
    try tt.expectEqual(0, map.len);
    try tt.expectEqual(0, map.remaining_capacity);
    try tt.expectEqual(0, map.entry_mask);

    try tt.expect(map.getPtrContext(0, .{}) == null);
    try tt.expect(map.get(1) == null);
}

test "empty map get" {
    var map: Map = .empty;
    const value = map.get(1);
    try tt.expectEqual(0, map.getEntry(OneToOne.hash(.{}, 1)));
    try tt.expectEqual(null, value);
}

test "reserve" {
    var map: Map = .empty;
    defer map.deinit(tt.allocator);

    try map.reserve(tt.allocator, 1);
    try tt.expect(map.noAlloc() == false);
    try tt.expectEqual(0, map.len);
    try tt.expectEqual(14, map.remaining_capacity);
}

test "tiny map" {
    try tt.expectEqual(0xffffffffffffffff, (OneToOne{}).hash(1));
    try tt.expectEqual(Metadata.hashHint(0xffffffffffffffff), Metadata{ .hash_hint = 0x7f, .free = false });

    var map: Map = .empty;
    defer map.deinit(tt.allocator);

    // This should grow the capacity from 0 to 14 (with 16 entries and 87.5% max load)
    try map.insert(tt.allocator, 0, 99);
    try tt.expectEqual(1, map.len);
    try tt.expectEqual(13, map.remaining_capacity);
    try map.insert(tt.allocator, 1, 100);
    try tt.expectEqual(2, map.len);
    try tt.expectEqual(12, map.remaining_capacity);

    // Inspect the metatadata array internals.
    const blocks = map.getMetadataBlocks();
    try tt.expectEqual(2, blocks.len);
    // Assert that the mirror block is indeed identical to first block.
    try tt.expectEqual(blocks[0], blocks[1]);
    // Assert that the appropriate metadata slots contain the appropriate values.
    try tt.expectEqual(0, map.getEntry(OneToOne.hash(.{}, 0)));
    try tt.expectEqual(Metadata{ .hash_hint = 0x00, .free = false }, map.metadata[0]);
    try tt.expectEqual(0, map.getKey(0).*);
    try tt.expectEqual(99, map.getValue(0).*);

    try tt.expectEqual(15, map.getEntry(OneToOne.hash(.{}, 1)));
    try tt.expectEqual(Metadata{ .hash_hint = 0x7f, .free = false }, map.metadata[15]);
    try tt.expectEqual(1, map.getKey(15).*);
    try tt.expectEqual(100, map.getValue(15).*);

    // Test the key retrieval
    try tt.expectEqual(99, map.get(0));
    try tt.expectEqual(100, map.get(1));

    // Test key removal
    try tt.expectEqual(99, map.removeFetch(0));
    try tt.expectEqual(100, map.removeFetch(1));

    // Check for tombstones
    try tt.expectEqual(14, map.remaining_capacity);
    try tt.expectEqual(0, map.len);
    try tt.expectEqual(Metadata.empty, map.metadata[0]);
    try tt.expectEqual(Metadata.empty, map.metadata[15]);
}

test "insert cache line probing" {
    const allocator = tt.allocator;

    var map: HashMap(i32, void, .{ .probing_strategy = .cache_line }) = .empty;
    defer map.deinit(allocator);

    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        const is: usize = @intCast(i);
        const found = try map.insertFetch(allocator, i, {});
        try tt.expectEqual(null, found);

        try tt.expectEqual(is + 1, map.len);
        try tt.expectEqual(map.getUsableCapacity() - (is + 1), map.remaining_capacity);
    }

    try tt.expectEqual(112, map.getUsableCapacity());
    try tt.expectEqual(100, map.len);
    try tt.expectEqual(12, map.remaining_capacity);
}

test "rehash" {
    const allocator = tt.allocator;

    var map: HashMap(i32, i32, .default) = .empty;
    defer map.deinit(allocator);

    // Populate a map with all integers from 0 to 100, then remove
    // every third number.
    var i: i32 = 0;
    while (i < 100) : (i += 1) {
        try map.insert(allocator, i, i);
    }

    try tt.expectEqual(100, map.len);
    try tt.expectEqual(12, map.remaining_capacity);

    i = 0;
    while (i < 100) : (i += 3) {
        try tt.expectEqual(i, map.removeFetch(i));
        try tt.expectEqual(null, map.get(i));

        var j: i32 = 0;
        while (j < 100) : (j += 1) {
            if (@mod(j, 3) == 0 and j <= i)
                try tt.expectEqual(null, map.get(j))
            else
                try tt.expectEqual(j, map.get(j));
        }
    }

    try tt.expectEqual(66, map.len);
    try tt.expectEqual(16, map.remaining_capacity);

    i = 0;
    while (i < 100) : (i += 1) {
        if (@mod(i, 3) == 0)
            try tt.expectEqual(null, map.get(i))
        else
            try tt.expectEqual(i, map.get(i));
    }

    map.rehash();

    try tt.expectEqual(66, map.len);
    try tt.expectEqual(46, map.remaining_capacity);

    i = 0;
    while (i < 100) : (i += 1) {
        if (@mod(i, 3) == 0)
            try tt.expectEqual(null, map.get(i))
        else
            try tt.expectEqual(i, map.get(i));
    }
}

test "repeat remove" {
    var map: HashMap(u64, void, .default) = .empty;
    defer map.deinit(tt.allocator);

    try map.reserve(tt.allocator, 4);
    map.insertUnchecked(0, {});
    map.insertUnchecked(1, {});
    map.insertUnchecked(2, {});
    map.insertUnchecked(3, {});

    var i: usize = 0;
    while (i < 10) : (i += 1) {
        try tt.expect(map.remove(3));
        map.insertUnchecked(3, {});
    }

    try tt.expect(map.get(0) != null);
    try tt.expect(map.get(1) != null);
    try tt.expect(map.get(2) != null);
    try tt.expect(map.get(3) != null);
}

test "get or insert u32" {
    const allocator = tt.allocator;

    var map: HashMap(u32, u32, .default) = .empty;
    defer map.deinit(tt.allocator);

    // First round of inserts, before first resizing.
    var i: u32 = 0;
    while (i < 14) : (i += 1) {
        const value = try map.getOrInsert(allocator, i, i);
        try tt.expectEqual(null, value);
    }

    i = 0;
    while (i < 14) : (i += 1) {
        const value = map.get(i) orelse return error.NotFound;
        try tt.expectEqual(i, value);
    }

    // Second round of inserts.
    while (i < 100) : (i += 1) {
        const value = try map.getOrInsert(allocator, i, i);
        try tt.expectEqual(null, value);
    }

    i = 0;
    while (i < 100) : (i += 1) {
        const value = map.get(i) orelse return error.NotFound;
        try tt.expectEqual(i, value);
    }
}

test "get or insert sum" {
    var map: HashMap(u32, u32, .default) = .empty;
    defer map.deinit(tt.allocator);

    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        _ = try map.insert(tt.allocator, i * 2, 2);
    }

    i = 0;
    while (i < 20) : (i += 1) {
        _ = try map.getOrInsert(tt.allocator, i, 1);
    }

    i = 0;
    var sum = i;
    while (i < 20) : (i += 1) {
        sum += map.get(i) orelse unreachable;
    }

    try tt.expectEqual(30, sum);
}

test "get or insert allocation failure" {
    var map: StringHashMap(void, .default) = .empty;
    try tt.expectError(error.OutOfMemory, map.getOrInsertKey(tt.failing_allocator, "hello"));
}
