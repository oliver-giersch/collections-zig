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

            pub fn peekNext(self: *const @This()) ?Item {
                return self.ptr.*;
            }

            pub fn next(self: *@This()) ?Item {
                const link = self.peekNext() orelse return null;
                self.ptr = &link.next;
                return link;
            }

            pub fn peekPrev(self: *const @This(), head: *const ?*Self.Link) ?Item {
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

    /// Returns true if the queue is empty.
    ///
    /// This operation has O(1) complexity.
    pub fn isEmpty(self: *const Self) bool {
        return Mixin(Self).isEmpty(self);
    }

    /// Returns true if the queue contains the given link.
    ///
    /// This operation has O(n) complexity in the worst case.
    pub fn contains(self: *const Self, link: *const Self.Link) bool {
        return Mixin(Self).contains(self, link);
    }

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

/// An intrusive, doubly-linked queue optimized for tail insertion.
pub const Queue = extern struct {
    const Self = @This();

    /// The link type for connecting queue items.
    pub const Link = double.Link;

    pub const ConstForwardIterator = GenericIterator(.forward, true);
    pub const ForwardIterator = GenericIterator(.forward, false);
    pub const ConstReverseIterator = GenericIterator(.reverse, true);
    pub const ReverseIterator = GenericIterator(.reverse, false);

    pub const Cursor = struct {
        queue: *Queue,
        ptr: *?*Self.Link,

        pub fn peekNext(self: *Cursor) ?*Self.Link {
            return self.ptr.* orelse return null;
        }

        pub fn moveNext(self: *Cursor) ?*Self.Link {
            const link = self.peekNext() orelse return null;
            self.ptr = &link.next;
            return link;
        }

        test moveNext {
            var queue: Queue = undefined;
            queue.empty();

            var links: [4]Queue.Link = undefined;
            for (&links) |link|
                queue.insertTail(link);

            var cur = queue.cursor();
            try tt.expectEqual(&links[0], cur.moveNext());
            try tt.expectEqual(&links[1], cur.moveNext());
            try tt.expectEqual(&links[2], cur.moveNext());
            try tt.expectEqual(&links[3], cur.moveNext());
            try tt.expectEqual(null, cur.moveNext());
        }

        pub fn insertNext(self: *Cursor, link: *Self.Link) void {
            assert(!self.queue.contains(link));

            link.next = self.peekNext();
            link.prev = self.ptr;
            self.ptr.* = link;
            if (link.next == null) {
                @branchHint(.unlikely);
                self.queue.tail = &link.next;
            }
        }

        pub fn removeNext(self: *Cursor) ?*Self.Link {
            const link = self.peekNext() orelse return null;
            self.ptr.* = link.next;
            if (link.next) |next| {
                @branchHint(.likely);
                next.prev = self.ptr;
            } else {
                self.queue.tail = self.ptr;
            }

            link.* = undefined;
            return link;
        }

        pub fn peekPrev(self: *const Cursor) ?*Self.Link {
            return getLink(self.ptr) orelse return null;
        }

        pub fn movePrev(self: *Cursor) ?*Self.Link {
            const link = self.peekPrev() orelse return null;
            self.ptr = link.prev;
            return link;
        }
    };

    fn GenericIterator(
        comptime direction: enum { forward, reverse },
        comptime is_const: bool,
    ) type {
        return struct {
            pub const Item = if (is_const) *const Self.Link else *Self.Link;

            link: ?Item,

            pub fn peekNext(self: *const @This()) ?Item {
                return self.link;
            }

            pub fn next(self: *@This()) ?Item {
                const link = self.peekNext() orelse return null;
                self.link = if (comptime direction == .forward) link.next else getLink(link.prev);
                return link;
            }
        };
    }

    /// The head of the queue.
    head: ?*Self.Link,
    /// The tail of the queue.
    ///
    /// This is a pointer to the pointer to the tail link.
    /// Points at `&self.head`, if the queue is empty.
    ///
    /// Copying an empty queue invalidates this field.
    tail: *?*Self.Link,

    /// Initializes the queue as empty.
    pub fn empty(self: *Self) void {
        self.head = null;
        self.tail = &self.head;
    }

    /// Returns true if the queue is empty.
    ///
    /// This operation has O(1) complexity.
    pub fn isEmpty(self: *const Self) bool {
        return Mixin(Self).isEmpty(self);
    }

    /// Returns true if the queue contains the given link.
    ///
    /// This operation has O(n) complexity in the worst case.
    pub fn contains(self: *const Self, link: *const Self.Link) bool {
        return Mixin(Self).contains(self, link);
    }

    /// Returns the queue's length.
    ///
    /// Consider storing and maintaining the length separately, if it is
    /// needed frequently.
    ///
    /// This operation has O(n) complexity.
    pub fn len(self: *const Self) usize {
        return Mixin(Self).len(self);
    }

    pub fn constIter(self: *const Self) ConstForwardIterator {
        return .{ .link = self.head };
    }

    pub fn iter(self: *Self) ForwardIterator {
        return .{ .link = self.head };
    }

    pub fn constReverseIter(self: *const Self) ConstReverseIterator {
        return .{ .link = getLink(self.tail) };
    }

    pub fn reverseIter(self: *Self) ReverseIterator {
        return .{ .link = getLink(self.tail) };
    }

    pub fn cursor(self: *Self) Cursor {
        return .{ .ptr = &self.head, .queue = self };
    }

    pub fn insertTail(self: *Self, link: *Self.Link) void {
        assert(!self.contains(link));

        link.next = null;
        link.prev = self.tail;
        self.tail.* = link;
        self.tail = &link.next;
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

    pub fn removeAfter(self: *Self, after: *Self.Link) ?*Self.Link {
        assert(self.contains(after));

        const link = after.next orelse return null;
        link.prev.* = link.next;
        if (link.next) |next| {
            @branchHint(.likely);
            next.prev = link.prev;
        } else {
            self.tail = &after.next;
        }

        link.* = undefined;
        return link;
    }

    pub fn removeBefore(self: *Self, before: *Self.Link) ?*Self.Link {
        assert(self.contains(before));

        const link = getLink(before.prev) orelse return null;
        link.prev.* = before;
        before.prev = link.prev;
        link.* = undefined;

        return link;
    }

    pub fn remove(self: *Self, link: *Self.Link) void {
        assert(self.contains(link));

        link.prev.* = link.next;
        if (link.next) |next| {
            @branchHint(.likely);
            next.prev = link.prev;
        } else {
            self.tail = link.prev;
        }

        link.* = undefined;
    }

    fn getLink(prev: *?*Self.Link) ?*Self.Link {
        const maybe_link: *Self.Link = @fieldParentPtr("next", prev);
        if (maybe_link.prev.* == null) {
            @branchHint(.unlikely);
            return null;
        }

        return maybe_link;
    }
};

pub const Link = extern struct {
    next: ?*Link,
    prev: *?*Link,
};

fn Mixin(comptime Self: type) type {
    return struct {
        fn isEmpty(self: *const Self) bool {
            return self.head == null;
        }

        fn contains(self: *const Self, link: *const Link) bool {
            var it = self.constIter();
            while (it.next()) |curr| {
                if (curr == link)
                    return true;
            }

            return false;
        }

        fn len(self: *const Self) usize {
            var it = self.constIter();
            var count: usize = 0;
            while (it.next()) |_| {
                count += 1;
            }

            return count;
        }

        fn get(self: *Self, idx: usize) ?*Link {
            return @constCast(self.getConst(idx));
        }

        fn getConst(self: *const Self, idx: usize) ?*const Link {
            var it = self.constIter();
            for (0..idx) |_|
                _ = it.next() orelse return null;
            return it.next();
        }
    };
}

const std = @import("std");
const assert = std.debug.assert;

const tt = std.testing;

test "queue cursor" {
    var queue: Queue = undefined;
    var links: [4]Queue.Link = undefined;
    queue.empty();

    for (&links) |*link|
        queue.insertTail(link);

    var cur = queue.cursor();
    try tt.expectEqual(&links[0], cur.moveNext());
    try tt.expectEqual(&links[1], cur.moveNext());
    try tt.expectEqual(&links[2], cur.moveNext());
    try tt.expectEqual(&links[3], cur.moveNext());
    try tt.expectEqual(null, cur.moveNext());
    try tt.expectEqual(&links[3], cur.movePrev());
    try tt.expectEqual(&links[2], cur.movePrev());
    try tt.expectEqual(&links[1], cur.movePrev());
    try tt.expectEqual(&links[0], cur.movePrev());
    try tt.expectEqual(null, cur.movePrev());
    try tt.expectEqual(&links[0], cur.moveNext());
}
