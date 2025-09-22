const double = @This();

pub const List = extern struct {
    const Self = @This();

    pub const empty: Self = .{ .head = null };

    pub const Link = double.Link;

    pub const ConstIterator = GenericIterator(true);
    pub const Iterator = GenericIterator(false);

    fn GenericIterator(comptime is_const: bool) type {
        return struct {
            pub const Item = if (is_const) *const Self.Link else *Self.Link;
            const ItemPointer = if (is_const) *const ?*Self.Link else *?*Self.Link;

            ptr: ItemPointer,

            pub fn peekNext(self: *@This()) ?Item {
                return self.ptr.*;
            }

            pub fn next(self: *@This()) ?Item {
                const link = self.peekNext() orelse return null;
                self.ptr = &link.next;
                return link;
            }

            pub fn peekPrev(self: *@This(), head: *const ?*Self.Link) ?Item {
                if (self.ptr == head)
                    return null;

                const link: Item = @fieldParentPtr("next", self.ptr);
                return link;
            }

            pub fn prev(self: *@This(), head: *const ?*Self.Link) ?Item {
                const link = self.peekPrev(head) orelse return null;
                self.ptr = link.prev;
                return link;
            }
        };
    }

    head: ?*Self.Link,

    pub const isEmpty = Mixin(Self).isEmpty;
    pub const contains = Mixin(Self).contains;
    pub const len = Mixin(Self).len;

    pub fn constIter(self: *const Self) ConstIterator {
        return .{ .ptr = &self.head };
    }

    pub fn iter(self: *Self) Iterator {
        return .{ .ptr = &self.head };
    }

    pub fn insertHead(self: *Self, link: *Self.Link) void {
        assert(!self.contains(link));

        link.next = self.head;
        link.prev = &self.head;
        self.head = link;
        if (link.next) |old_head| {
            @branchHint(.likely);
            old_head.prev = &link.next;
        }
    }

    pub fn insertAfter(self: *Self, after: *Self.Link, link: *Self.Link) void {
        assert(self.contains(after));
        assert(!self.contains(link));

        link.next = after.next;
        link.prev = &after.next;
        after.next = link;
        if (link.next) |next| {
            @branchHint(.likely);
            next.prev = &link.next;
        }
    }

    pub fn insertBefore(self: *Self, before: *Self.Link, link: *Self.Link) void {
        assert(self.contains(before));
        assert(!self.contains(link));

        link.next = before;
        link.prev = before.prev;
        before.prev.* = link;
        before.prev = &link.next;
    }

    pub fn removeHead(self: *Self) ?*Self.Link {
        const link = self.head orelse return null;
        self.head = link.next;
        if (link.next) |next| {
            @branchHint(.likely);
            next.prev = &self.head;
        }

        link.* = undefined;
        return link;
    }

    pub fn remove(self: *Self, link: *Self.Link) void {
        assert(self.contains(link));

        link.prev.* = link.next;
        if (link.next) |next| {
            @branchHint(.likely);
            next.prev = link.prev;
        }

        link.* = undefined;
    }
};

pub const Queue = extern struct {
    const Self = @This();

    pub const Link = double.Link;

    head: ?*Self.Link,
    tail: *?*Self.Link,

    pub fn empty(self: *Self) void {
        self.head = null;
        self.tail = &self.head;
    }

    pub const isEmpty = Mixin(Self).isEmpty;
    pub const contains = Mixin(Self).contains;
    pub const len = Mixin(Self).len;
};

pub const Link = extern struct {
    next: ?*Link,
    prev: *?*Link,
};

fn Mixin(comptime Self: type) type {
    return struct {
        pub fn isEmpty(self: *const Self) bool {
            return self.head == null;
        }

        pub fn contains(self: *const Self, link: *const Link) bool {
            var it = self.constIter();
            while (it.next()) |curr| {
                if (curr == link)
                    return true;
            }

            return false;
        }

        pub fn len(self: *const Self) usize {
            var it = self.constIter();
            var count: usize = 0;
            while (it.next()) |_| {
                count += 1;
            }

            return count;
        }
    };
}

const std = @import("std");
const assert = std.debug.assert;
