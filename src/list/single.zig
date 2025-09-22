const single = @This();

pub const List = struct {
    const Self = @This();

    pub const empty: Self = .{ .head = null };

    pub const Link = single.Link;

    pub const ConstIterator = GenericIterator(true);
    pub const Iterator = GenericIterator(false);

    /// The head of the list.
    head: ?*Self.Link,

    pub const isEmpty = Mixin(Self).isEmpty;
    pub const contains = Mixin(Self).contains;
    pub const len = Mixin(Self).len;
    pub const constIter = Mixin(Self).constIter;
    pub const iter = Mixin(Self).iter;

    pub fn insertFront(self: *Self, link: *Self.Link) void {
        assert(!self.contains(link));

        link.next = self.head;
        self.head = link;
    }

    pub fn insertAfter(self: *Self, after: *Self.Link, link: *Self.Link) void {
        assert(self.contains(after));
        assert(!self.contains(link));

        link.next = after.next;
        after.next = link;
    }

    pub fn removeHead(self: *Self) ?*Self.Link {
        const link = self.head orelse return null;
        self.head = link.next;

        link.* = undefined;
        return link;
    }

    pub fn remove(self: *Self, link: *Self.Link) void {
        assert(self.contains(link));

        const prev = Mixin(Self).findPrev(self, link);
        prev.next = link.next;
        link.* = undefined;
    }
};

/// A singly-linked queue optimized for tail-insertion.
pub const Queue = struct {
    const Self = @This();

    pub const Link = single.Link;

    pub const ConstIterator = GenericIterator(true);
    pub const Iterator = GenericIterator(false);

    /// The head of the queue.
    head: ?*Self.Link,
    /// The pointer to the tail of the queue.
    tail: *?*Self.Link,

    pub fn empty(self: *Self) void {
        self.head = null;
        self.tail = &self.head;
    }

    pub const isEmpty = Mixin(Self).isEmpty;
    pub const contains = Mixin(Self).contains;
    pub const len = Mixin(Self).len;
    pub const constIter = Mixin(Self).constIter;
    pub const iter = Mixin(Self).iter;

    pub fn insertHead(self: *Self, link: *Self.Link) void {
        assert(!self.contains(link));

        link.next = self.head;
        self.head = link;
        if (link.next == null) {
            @branchHint(.unlikely);
            self.tail = &link.next;
        }
    }

    pub fn insertTail(self: *Self, link: *Self.Link) void {
        assert(!self.contains(link));

        link.next = null;
        self.tail.* = link;
        self.tail = &link.next;
    }

    pub fn insertAfter(self: *Self, after: *Self.Link, link: *Self.Link) void {
        assert(self.contains(after));
        assert(!self.contains(link));

        link.next = after.next;
        after.next = link;
        if (link.next == null) {
            @branchHint(.unlikely);
            self.tail = &link.next;
        }
    }

    pub fn removeHead(self: *Self) ?*Self.Link {
        const link = self.head orelse return null;
        self.head = link.next;
        if (link.next == null) {
            @branchHint(.unlikely);
            self.tail = &self.head;
        }

        link.* = undefined;
        return link;
    }

    pub fn remove(self: *Self, link: *Self.Link) void {
        assert(self.contains(link));

        const prev = Mixin(Self).findPrev(self, link);
        prev.* = link.next;
        if (link.next == null) {
            @branchHint(.unlikely);
            self.tail = prev;
        }
        link.* = undefined;
    }
};

/// A link in a singly-linked list or queue.
pub const Link = struct {
    /// The pointer to the next link.
    next: ?*Link,
};

fn GenericIterator(comptime is_const: bool) type {
    return struct {
        const Self = @This();

        pub const ItemPointer = if (is_const) *const ?*Link else *?*Link;
        pub const Item = if (is_const) *const Link else *Link;

        ptr: ItemPointer,

        pub fn peekNext(self: *const Self) ?Item {
            return self.ptr.*;
        }

        pub fn next(self: *Self) ?Item {
            const link = self.peekNext() orelse return null;
            self.ptr = &link.next;
            return link;
        }
    };
}

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

        pub fn constIter(self: *const Self) GenericIterator(true) {
            return .{ .ptr = &self.head };
        }

        pub fn iter(self: *Self) GenericIterator(false) {
            return .{ .ptr = &self.head };
        }

        fn findPrev(self: *Self, link: *const Link) *?*Link {
            var it = self.iter();
            while (it.peekNext()) |curr| {
                if (curr == link)
                    return it.ptr;
                _ = it.next();
            }

            unreachable;
        }
    };
}

const std = @import("std");
const assert = std.debug.assert;

const tt = std.testing;

test "slist insert head" {
    var list: List = .empty;
    try tt.expect(list.isEmpty());
    var links: [2]List.Link = undefined;

    list.insertFront(&links[0]);
    list.insertFront(&links[1]);

    try tt.expect(list.contains(&links[0]));
    try tt.expect(list.contains(&links[1]));
}

test "squeue remove" {
    var queue: Queue = undefined;
    queue.empty();
    var links: [2]Queue.Link = undefined;

    queue.insertTail(&links[0]);
    queue.insertTail(&links[1]);

    queue.remove(&links[1]);
    queue.remove(&links[0]);
    try tt.expect(queue.isEmpty());
}
