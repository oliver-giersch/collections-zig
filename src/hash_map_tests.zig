const hash_map = @import("hash_map.zig");

inline fn getConfigurations() []const hash_map.Options {
    const layouts: []const hash_map.Options.Layout = &.{ .array, .multi_array };
    const probing_strategies: []const hash_map.Options.ProbingStrategy = &.{ .linear, .triangular, .cache_line };
    const load_percentages: []const u8 = &.{ 50, 66, 88, 100 };

    const len = load_percentages.len * 3 * 2;
    var configurations: [len]hash_map.Options = undefined;

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

fn testInsertN(
    comptime options: hash_map.Options,
    n: usize,
    pre_alloc: bool,
) !void {
    const HashMap = hash_map.HashMap(u32, u32, options);
    var map: HashMap = if (pre_alloc) try .init(testing.allocator, n) else .empty;
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

test "insert 1e2 key-value pairs" {
    inline for (cfgs) |options| {
        try testInsertN(options, 100, true);
        try testInsertN(options, 100, false);
    }
}

test "insert 1e3 key-value pairs" {
    if (true) return error.SkipZigTest;
    inline for (cfgs) |options| {
        try testInsertN(options, 1_000, true);
        try testInsertN(options, 1_000, false);
    }
}

const std = @import("std");
const testing = std.testing;
