const std = @import("std");
const mem = std.mem;
const sort = std.sort;
const testing = std.testing;

const hash_map = @import("hash_map.zig");

const ArrayList = @import("array_list.zig").ArrayList;
const HashMap = hash_map.HashMap;
const Options = hash_map.Options;

inline fn getConfigurations() []const Options {
    const layouts: []const Options.Layout = &.{ .array, .multi_array };
    const probing_strategies: []const Options.ProbingStrategy = &.{ .linear, .triangular, .cache_line };
    const load_percentages: []const u8 = &.{ 50, 66, 88, 100 };

    const len = layouts.len * probing_strategies.len * load_percentages.len;
    var configurations: [len]Options = undefined;

    var i: usize = 0;
    for (layouts) |layout| {
        for (probing_strategies) |probing_strategy| {
            for (load_percentages) |max_load_percentage| {
                configurations[i] = .{
                    .layout = layout,
                    .probing_strategy = probing_strategy,
                    .max_load_percentage = max_load_percentage,
                };
                i += 1;
            }
        }
    }

    const const_configurations = configurations;
    return &const_configurations;
}

const cfgs = getConfigurations();

fn testEmptyMap(comptime options: Options) !void {
    var map: HashMap(u32, u32, options) = .empty;
    defer map.deinit(testing.allocator);

    try testing.expectEqual(0, map.len);
    try testing.expectEqual(0, map.remaining_capacity);
    try testing.expectEqual(0, map.entry_mask);

    try testing.expect(map.getPtr(0) == null);
    try testing.expect(map.get(1) == null);
}

fn testInsertContains(comptime options: Options) !void {
    const allocator = testing.allocator;

    var map: HashMap(u32, void, options) = .empty;
    defer map.deinit(allocator);

    try map.insert(allocator, 1, {});
    try testing.expectEqual(1, map.len);
    try testing.expectEqual(true, map.contains(1));
    try testing.expectEqual(false, map.contains(2));
}

fn testInsertGet(comptime options: Options) !void {
    const allocator = testing.allocator;

    var map: HashMap(u32, u32, options) = .empty;
    defer map.deinit(allocator);

    try map.insert(allocator, 1, 2);
    try map.insert(allocator, 2, 4);
    try testing.expectEqual(2, map.len);
    try testing.expectEqual(2, map.get(1));
    try testing.expectEqual(4, map.get(2));
}

fn testAllocationFailure(comptime options: Options) !void {
    var map: HashMap(u32, u32, options) = .empty;
    defer map.deinit(testing.allocator);

    const oom = error.OutOfMemory;
    try testing.expectError(oom, map.insert(testing.failing_allocator, 1, 1));
    try testing.expectError(oom, map.insertFetch(testing.failing_allocator, 1, 1));
    try testing.expectError(oom, map.insertUnique(testing.failing_allocator, 1, 1));
    try testing.expectError(oom, map.getOrInsert(testing.failing_allocator, 1, 1));
    try testing.expectError(oom, map.getOrInsertKey(testing.failing_allocator, 1));
}

fn testInsertN(comptime options: Options, n: usize, pre_alloc: bool) !void {
    var map: HashMap(u32, u32, options) = if (pre_alloc)
        try .init(testing.allocator, n)
    else
        .empty;
    defer map.deinit(testing.allocator);

    var i: u32 = 0;
    while (i < n) : (i += 1) {
        const value = map.insertFetch(testing.allocator, i, i);
        try testing.expectEqual(null, value);
    }

    i = 0;
    while (i < n) : (i += 1) {
        const value = map.get(i);
        try testing.expectEqual(i, value);
    }

    try testing.expectEqual(n, map.len);
}

fn testGetOrInsertSum(comptime options: Options) !void {
    var map: HashMap(u32, u32, options) = .empty;
    defer map.deinit(testing.allocator);

    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        _ = try map.insert(testing.allocator, i * 2, 2);
    }

    i = 0;
    while (i < 20) : (i += 1) {
        _ = try map.getOrInsert(testing.allocator, i, 1);
    }

    i = 0;
    var sum = i;
    while (i < 20) : (i += 1) {
        sum += map.get(i) orelse unreachable;
    }

    try testing.expectEqual(30, sum);
}

fn testRehashN(comptime options: Options, n: usize) !void {
    var map: HashMap(u32, u32, options) = .empty;
    defer map.deinit(testing.allocator);

    // Populate with all numbers 0-N
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        try map.insert(testing.allocator, i, i);
    }

    try testing.expectEqual(n, map.len);

    // Remove every third value
    i = 0;
    while (i < n) : (i += 3) {
        try testing.expectEqual(i, map.removeFetch(i));
        try testing.expectEqual(null, map.get(i));
        try testing.expect(!map.contains(i));

        var j: u32 = 0;
        while (j < n) : (j += 1) {
            if (@mod(j, 3) == 0 and j <= i)
                try testing.expectEqual(null, map.get(j))
            else
                try testing.expectEqual(j, map.get(j));
        }
    }

    map.rehash();

    try testing.expectEqual((n / 3) * 2, map.len);

    i = 0;
    while (i < n) : (i += 1) {
        if (@mod(i, 3) == 0)
            try testing.expectEqual(null, map.get(i))
        else
            try testing.expectEqual(i, map.get(i));
    }
}

fn testKeyValueIteratorN(comptime options: Options, n: usize) !void {
    var map: HashMap(u32, u32, options) = .empty;
    defer map.deinit(testing.allocator);

    var i: u32 = 0;
    while (i < n) : (i += 1) {
        const value = map.insertFetch(testing.allocator, i, i * 2);
        try testing.expectEqual(null, value);
    }

    var keys: ArrayList(u32) = .empty;
    var values: ArrayList(u32) = .empty;

    var it = map.constIter();
    while (it.next()) |kv| {
        try testing.expectEqual(2 * kv.key.*, kv.value.*);
        try keys.push(testing.allocator, kv.key.*);
        try values.push(testing.allocator, kv.value.*);
    }

    mem.sort(u32, keys.bounded.items, {}, sort.asc(u32));
    mem.sort(u32, values.bounded.items, {}, sort.asc(u32));

    i = 0;
    while (i < n) : (i += 1) {
        try testing.expectEqual(i, keys.bounded.items[i]);
        try testing.expectEqual(i * 2, values.bounded.items[i]);
    }
}

fn testKeyIteratorN(comptime options: Options, n: usize) !void {
    var map: HashMap(u32, u32, options) = .empty;
    defer map.deinit(testing.allocator);

    var i: u32 = 0;
    while (i < n) : (i += 1) {
        try map.insert(testing.allocator, i, i);
    }

    var keys: ArrayList(u32) = .empty;
    var it = map.keyIter();
    while (it.next()) |key| {
        try keys.push(testing.allocator, key.*);
    }

    mem.sort(u32, keys.bounded.items, {}, sort.asc(u32));

    i = 0;
    while (i < n) : (i += 1) {
        try testing.expectEqual(i, keys.bounded.items[i]);
    }
}

fn testValueIteratorN(comptime options: Options, n: usize) !void {
    var map: HashMap(u32, u32, options) = .empty;
    defer map.deinit(testing.allocator);

    var i: u32 = 0;
    while (i < n) : (i += 1) {
        const prev = try map.insertFetch(testing.allocator, i, i * 2);
        try testing.expectEqual(null, prev);
    }

    var values: ArrayList(u32) = try .init(testing.allocator, n);
    defer values.deinit(testing.allocator);
    {
        var it = map.valueIter();
        while (it.next()) |value| {
            value.* *= 2;
        }
    }

    var it = map.constValueIter();
    while (it.next()) |value| {
        values.bounded.push(value.*) catch unreachable;
    }

    mem.sort(u32, values.bounded.items, {}, sort.asc(u32));

    i = 0;
    while (i < n) : (i += 1) {
        try testing.expectEqual(i * 4, values.bounded.items[i]);
    }
}

fn testRandomInsertRemoveN(
    comptime options: Options,
    n: usize,
    pre_alloc: bool,
) !void {
    var map: HashMap(u32, u32, options) = if (pre_alloc)
        try .init(testing.allocator, n)
    else
        .empty;
    defer map.deinit(testing.allocator);

    var keys: ArrayList(u32) = .empty;
    defer keys.deinit(testing.allocator);

    var i: u32 = 0;
    while (i < n) : (i += 1) {
        try keys.push(testing.allocator, i);
    }

    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();
    random.shuffle(u32, keys.bounded.items);

    for (keys.bounded.items) |key| {
        try map.insert(testing.allocator, key, key);
    }

    i = 0;
    while (i < n) : (i += 1) {
        try testing.expect(map.contains(i));
        const value = map.get(i);
        try testing.expectEqual(i, value);
    }

    random.shuffle(u32, keys.bounded.items);
    i = 0;

    while (i < n) : (i += 1) {
        const key = keys.bounded.items[i];
        const value = map.removeFetch(key);
        try testing.expectEqual(key, value);
    }
}

test "empty map" {
    inline for (cfgs) |options| {
        try testEmptyMap(options);
    }
}

test "insert and contains" {
    inline for (cfgs) |options| {
        try testInsertContains(options);
    }
}

test "insert and get" {
    inline for (cfgs) |options| {
        try testInsertGet(options);
    }
}

test "allocation failure" {
    inline for (cfgs) |options| {
        try testAllocationFailure(options);
    }
}

test "insert 1e2 key-value pairs" {
    inline for (cfgs) |options| {
        try testInsertN(options, 100, true);
        try testInsertN(options, 100, false);
    }
}

test "insert 1e3 key-value pairs" {
    inline for (cfgs) |options| {
        try testInsertN(options, 1_000, true);
        try testInsertN(options, 1_000, false);
    }
}

test "rehash 1e2 key-value pairs" {
    inline for (cfgs) |options| {
        try testRehashN(options, 100);
    }
}

test "rehash 1e3 key-value pairs" {
    inline for (cfgs) |options| {
        try testRehashN(options, 1_000);
    }
}

test "get or insert (sum)" {
    inline for (cfgs) |options| {
        try testGetOrInsertSum(options);
    }
}

test "value iterator 1e2" {
    inline for (cfgs) |options| {
        try testValueIteratorN(options, 100);
    }
}

test "value iterator 1e3" {
    inline for (cfgs) |options| {
        try testValueIteratorN(options, 1_000);
    }
}

test "random insert/remove 1e2" {
    inline for (cfgs) |options| {
        try testRandomInsertRemoveN(options, 100, true);
        try testRandomInsertRemoveN(options, 100, false);
    }
}

test "random insert/remove 1e3" {
    inline for (cfgs) |options| {
        try testRandomInsertRemoveN(options, 1_000, true);
        try testRandomInsertRemoveN(options, 1_000, false);
    }
}

test "random insert/remove 1e6" {
    inline for (cfgs) |options| {
        try testRandomInsertRemoveN(options, 1_000_000, true);
        try testRandomInsertRemoveN(options, 1_000_000, false);
    }
}

test "debug test" {
    const options: Options = .{ .layout = .array, .probing_strategy = .cache_line, .max_load_percentage = 100 };
    try testRandomInsertRemoveN(options, 1_000_000, true);
}
