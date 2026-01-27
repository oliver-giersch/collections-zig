const double = @This();

const std = @import("std");
const assert = std.debug.assert;

/// An intrusive, double-linked list.
///
/// This type of list requires the most memory but in return supports efficient
/// insertion and removal at arbitrary list positions (except for tail
/// insertions) as well as iteration in both forward and reverse direction.
///
/// All insertion methods assume uninitialized link arguments and will fully
/// initialize any link that is inserted.
/// All removal methods must be assumed to uninitialize the contents of the
/// removed links but not the containing structures.
///
/// The user is entirely responsible for managing the lifetime of list links.
/// Each link must live for at least as long as it is part of the list.
pub const List = extern struct {
    const Self = @This();

    /// The initializer value for an empty list.
    pub const empty: Self = .{ .head = null };

    /// The link type for connecting list items.
    pub const Link = double.Link;

    /// A forward const iterator over all links in the list.
    pub const ConstIterator = GenericIterator(true);
    /// A forward iterator over all links in the list.
    pub const Iterator = GenericIterator(false);

    fn GenericIterator(comptime is_const: bool) type {
        return struct {
            pub const Item = if (is_const) *const Self.Link else *Self.Link;

            const ItemPointer = if (is_const) *const ?*Self.Link else *?*Self.Link;

            ptr: ItemPointer,

            /// Returns the next link in the iterator's sequence without
            /// advancing it.
            pub fn peekNext(self: *const @This()) ?Item {
                return self.ptr.*;
            }

            /// Returns the next link and advances the iterator.
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

    /// The list's head link.
    head: ?*Self.Link,

    /// Returns true if the queue is empty.
    ///
    /// This operation has O(1) complexity.
    pub fn isEmpty(self: *const Self) bool {
        return Mixin(Self).isEmpty(self);
    }

    test isEmpty {
        var list: List = .empty;
        try testing.expect(list.isEmpty());
    }

    /// Returns true if the queue contains the given link.
    ///
    /// This operation has O(n) complexity in the worst case.
    pub fn contains(self: *const Self, link: *const Self.Link) bool {
        return Mixin(Self).contains(self, link);
    }

    test contains {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);
        try testing.expect(list.contains(&links[0]));
        try testing.expect(list.contains(&links[1]));
    }

    /// Returns the list's length.
    ///
    /// Consider storing and maintaining the length separately, if it is
    /// needed frequently.
    ///
    /// This operation has O(n) complexity.
    pub fn len(self: *const Self) usize {
        return Mixin(Self).len(self);
    }

    test len {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);
        try testing.expectEqual(2, list.len());
    }

    /// Returns the link at the given index.
    ///
    /// This operation has O(n) complexity in the worst case.
    pub fn get(self: *Self, idx: usize) ?*Self.Link {
        return Mixin(Self).get(self, idx);
    }

    test get {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        for (&links, 0..) |*link, i|
            try testing.expectEqual(link, list.get(i));
    }

    /// Returns the link at the given index.
    ///
    /// This operation has O(n) complexity in the worst case.
    pub fn getConst(self: *const Self, idx: usize) ?*const Self.Link {
        return Mixin(Self).getConst(self, idx);
    }

    test getConst {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        const cref: *const List = &list;
        for (&links, 0..) |*link, i|
            try testing.expectEqual(link, cref.getConst(i));
    }

    /// Returns an iterator over the items in the list.
    pub fn iter(self: *Self) Iterator {
        return .{ .ptr = &self.head };
    }

    test iter {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        var it = list.iter();
        for (&links) |*link|
            try testing.expectEqual(link, it.next());
        try testing.expectEqual(null, it.next());
    }

    /// Returns a const iterator over the items in the list.
    pub fn constIter(self: *const Self) ConstIterator {
        return .{ .ptr = &self.head };
    }

    test constIter {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        const cref: *const List = &list;
        var it = cref.constIter();
        for (&links) |*link|
            try testing.expectEqual(link, it.next());
        try testing.expectEqual(null, it.next());
    }

    /// Inserts the given link at the list's head.
    ///
    /// Asserts that the list does not yet contain the given link.
    /// The given link may be uninitialized.
    ///
    /// This operation has O(1) complexity.
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

    /// Inserts the given link after the given predecessor link.
    ///
    /// Asserts that the list contains the predecessor but not the link itself.
    /// The given link may be uninitialized.
    ///
    /// This operation has O(1) complexity.
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

    /// Inserts the given link before the given successor link.
    ///
    /// Asserts that the list contains the successor but not the link itself.
    /// The given link may be uninitialized.
    ///
    /// This operation has O(1) complexity.
    pub fn insertBefore(self: *Self, before: *Self.Link, link: *Self.Link) void {
        assert(self.contains(before));
        assert(!self.contains(link));

        link.next = before;
        link.prev = before.prev;
        before.prev.* = link;
        before.prev = &link.next;
    }

    test insertBefore {
        var list: List = .empty;
        var links: [4]List.Link = undefined;

        list.insertHead(&links[3]);
        list.insertBefore(&links[3], &links[2]);
        list.insertBefore(&links[2], &links[1]);
        list.insertBefore(&links[1], &links[0]);

        for (&links, 0..) |*link, i|
            try testing.expectEqual(link, list.get(i));
    }

    /// Replaces an existing link with another one.
    ///
    /// Asserts that the list contains the existing but not the replacement
    /// link.
    /// The given link may be uninitialized.
    ///
    /// This operation has O(1) complexity.
    pub fn replace(self: *Self, old: *Self.Link, new: *Self.Link) void {
        return Mixin(Self).replace(self, old, new);
    }

    test replace {
        var list: List = .empty;
        var old_link: List.Link = undefined;
        var links: [4]List.Link = undefined;

        list.insertHead(&old_link);
        list.insertAfter(&old_link, &links[1]);
        list.insertAfter(&links[1], &links[2]);
        list.insertAfter(&links[2], &links[3]);

        list.replace(&old_link, &links[0]);

        for (&links, 0..) |*link, i|
            try testing.expectEqual(link, list.get(i));
    }

    /// Removes and returns the list's head.
    ///
    /// This operation has O(1) complexity.
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

    test removeHead {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        try testing.expectEqual(&links[0], list.removeHead());
        try testing.expectEqual(&links[1], list.removeHead());
        try testing.expectEqual(null, list.removeHead());
    }

    /// Removes and returns the link after the given link.
    ///
    /// Asserts that the list contains the predecessor link.
    ///
    /// This operation has O(1) complexity.
    pub fn removeAfter(self: *Self, after: *Self.Link) ?*Self.Link {
        assert(self.contains(after));

        const link = after.next orelse return null;
        link.prev.* = link.next;
        if (link.next) |next| {
            @branchHint(.likely);
            next.prev = &after.next;
        }

        link.* = undefined;
        return link;
    }

    test removeAfter {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        try testing.expectEqual(&links[1], list.removeAfter(&links[0]));
    }

    /// Removes and returns the link before the given link.
    ///
    /// Asserts that the list contains the sucessor link.
    ///
    /// This operation has O(1) complexity.
    pub fn removeBefore(self: *Self, before: *Self.Link) ?*Self.Link {
        assert(self.contains(before));

        const link = self.getLink(before.prev) orelse return null;
        link.prev.* = before;
        before.prev = link.prev;

        link.* = undefined;
        return link;
    }

    test removeBefore {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        try testing.expectEqual(&links[0], list.removeBefore(&links[1]));
        try testing.expect(!list.contains(&links[0]));
        try testing.expect(list.contains(&links[1]));
    }

    /// Removes the given link from the queue.
    ///
    /// Asserts that the queue contains the given link.
    ///
    /// This operation has O(1) complexity.
    pub fn remove(self: *Self, link: *Self.Link) void {
        assert(self.contains(link));

        link.prev.* = link.next;
        if (link.next) |next| {
            @branchHint(.likely);
            next.prev = link.prev;
        }

        link.* = undefined;
    }

    test remove {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[0]);
        list.insertHead(&links[1]);

        list.remove(&links[0]);
        try testing.expect(!list.contains(&links[0]));
        try testing.expect(list.contains(&links[1]));
    }

    fn getLink(self: *Self, prev: *?*Self.Link) ?*Self.Link {
        if (prev == &self.head) {
            @branchHint(.unlikely);
            return null;
        }

        return @fieldParentPtr("next", prev);
    }
};

/// An intrusive, double-linked queue optimized for tail insertion.
pub const Queue = extern struct {
    const Self = @This();

    /// The link type for connecting queue items.
    pub const Link = double.Link;

    /// A forward const iterator over all links in the list.
    pub const ConstIterator = GenericIterator(.forward, true);
    /// A forward iterator over all links in the list.
    pub const Iterator = GenericIterator(.forward, false);
    /// A reverse const iterator over all links in the list.
    pub const ConstReverseIterator = GenericIterator(.reverse, true);
    /// A reverse iterator over all links in the list.
    pub const ReverseIterator = GenericIterator(.reverse, false);

    pub const Cursor = double.Cursor;

    fn GenericIterator(
        comptime direction: enum { forward, reverse },
        comptime is_const: bool,
    ) type {
        return struct {
            /// The type of link pointer returned by the iterator.
            pub const Item = if (is_const) *const Self.Link else *Self.Link;

            /// True if iterating in forward direction.
            pub const is_forward = direction == .forward;

            /// The pointer to the current link.
            link: ?Item,

            /// Returns the next link without advancing the iterator.
            pub fn peekNext(self: *const @This()) ?Item {
                return self.link;
            }

            /// Returns the next link and advances the iterator.
            pub fn next(self: *@This()) ?Item {
                const link = self.peekNext() orelse return null;
                self.link = if (comptime direction == .forward)
                    link.next
                else
                    getLink(link.prev);
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

    test isEmpty {
        var queue: Queue = undefined;
        queue.empty();
        try testing.expect(queue.isEmpty());
    }

    /// Returns true if the queue contains the given link.
    ///
    /// This operation has O(n) complexity in the worst case.
    pub fn contains(self: *const Self, link: *const Self.Link) bool {
        return Mixin(Self).contains(self, link);
    }

    test contains {
        var queue: Queue = undefined;
        var links: [2]Queue.Link = undefined;
        queue.empty();

        queue.insertTail(&links[0]);
        queue.insertTail(&links[1]);
        try testing.expect(queue.contains(&links[0]));
        try testing.expect(queue.contains(&links[1]));
    }

    /// Returns the queue's length.
    ///
    /// Consider storing and maintaining the length separately,
    /// if it is needed frequently.
    ///
    /// This operation has O(n) complexity.
    pub fn len(self: *const Self) usize {
        return Mixin(Self).len(self);
    }

    test len {
        var queue: Queue = undefined;
        var links: [2]Queue.Link = undefined;
        queue.empty();

        queue.insertTail(&links[0]);
        queue.insertTail(&links[1]);
        try testing.expectEqual(2, queue.len());
    }

    /// Returns a pointer to the first link the queue.
    pub fn first(self: *Self) ?*Self.Link {
        return self.head;
    }

    pub fn firstConst(self: *const Self) ?*const Self.Link {
        return self.head;
    }

    /// Returns a pointer to the last link in the queue.
    pub fn last(self: *Self) ?*Self.Link {
        return getLink(self.tail);
    }

    test last {
        var queue: Queue = undefined;
        var links: [4]Queue.Link = undefined;
        queue.empty();

        for (&links) |*link|
            queue.insertTail(link);

        try testing.expectEqual(&links[3], queue.last());
    }

    pub fn lastConst(self: *const Self) ?*const Self.Link {
        return getLink(self.tail);
    }

    test lastConst {
        var queue: Queue = undefined;
        var links: [4]Queue.Link = undefined;
        queue.empty();

        for (&links) |*link|
            queue.insertTail(link);

        const cref: *const Queue = &queue;
        try testing.expectEqual(&links[3], cref.lastConst());
    }

    /// Returns the link at the given index.
    ///
    /// This operation has O(n) complexity in the worst case.
    pub fn get(self: *Self, idx: usize) ?*Self.Link {
        return Mixin(Self).get(self, idx);
    }

    test get {
        var queue: Queue = undefined;
        var links: [2]Queue.Link = undefined;
        queue.empty();

        queue.insertTail(&links[0]);
        queue.insertTail(&links[1]);
        try testing.expectEqual(&links[0], queue.get(0));
        try testing.expectEqual(&links[1], queue.get(1));
    }

    /// Returns the link at the given index.
    ///
    /// This operation has O(n) complexity in the worst case.
    pub fn getConst(self: *const Self, idx: usize) ?*const Self.Link {
        return Mixin(Self).getConst(self, idx);
    }

    test getConst {
        var queue: Queue = undefined;
        var links: [2]Queue.Link = undefined;
        queue.empty();

        queue.insertTail(&links[0]);
        queue.insertTail(&links[1]);

        const cref: *const Queue = &queue;
        try testing.expectEqual(&links[0], cref.getConst(0));
        try testing.expectEqual(&links[1], cref.getConst(1));
    }

    /// Returns an iterator over the queue's links.
    pub fn iter(self: *Self) Iterator {
        return .{ .link = self.head };
    }

    test iter {
        var queue: Queue = undefined;
        queue.empty();
        var links: [2]List.Link = undefined;

        queue.insertTail(&links[0]);
        queue.insertTail(&links[1]);

        var it = queue.iter();
        for (&links) |*link|
            try testing.expectEqual(link, it.next());
        try testing.expectEqual(null, it.next());
    }

    /// Returns a const iterator over the queue's links.
    pub fn constIter(self: *const Self) ConstIterator {
        return .{ .link = self.head };
    }

    test constIter {
        var queue: Queue = undefined;
        queue.empty();
        var links: [2]List.Link = undefined;

        queue.insertTail(&links[0]);
        queue.insertTail(&links[1]);

        const cref: *const Queue = &queue;
        var it = cref.constIter();
        for (&links) |*link|
            try testing.expectEqual(link, it.next());
        try testing.expectEqual(null, it.next());
    }

    /// Returns a reverse iterator over the queue's links.
    pub fn reverseIter(self: *Self) ReverseIterator {
        return .{ .link = getLink(self.tail) };
    }

    test reverseIter {
        var queue: Queue = undefined;
        queue.empty();
        var links: [4]Queue.Link = undefined;

        queue.insertTail(&links[0]);
        queue.insertTail(&links[1]);
        queue.insertTail(&links[2]);
        queue.insertTail(&links[3]);

        var i: usize = 4;
        var it = queue.reverseIter();
        while (i > 0) : (i -= 1)
            try testing.expectEqual(&links[i - 1], it.next());
        try testing.expectEqual(null, it.next());
    }

    /// Returns a const reverse iterator over the queue's links.
    pub fn constReverseIter(self: *const Self) ConstReverseIterator {
        return .{ .link = getLink(self.tail) };
    }

    /// Returns a cursor for the queue.
    pub fn cursor(self: *Self) Self.Cursor {
        return .{ .ptr = &self.head, .queue = self };
    }

    /// Appends the given queue to this queue's tail.
    ///
    /// This operation has O(1) complexity.
    pub fn concat(self: *Self, other: *const Self) void {
        const other_head = other.head orelse {
            @branchHint(.unlikely);
            return;
        };

        self.tail.* = other.head;
        other_head.prev = self.tail;
        self.tail = other.tail;
    }

    test concat {
        var queue1: Queue = undefined;
        queue1.empty();
        var queue2: Queue = undefined;
        queue2.empty();
        var links: [4]Queue.Link = undefined;

        queue1.insertTail(&links[0]);
        queue1.insertTail(&links[1]);
        queue2.insertTail(&links[2]);
        queue2.insertTail(&links[3]);

        queue1.concat(&queue2);
        for (&links, 0..) |*link, i| {
            try testing.expectEqual(link, queue1.get(i));
        }
    }

    /// Inserts the given link at the queue's head.
    ///
    /// This operation has O(1) complexity.
    pub fn insertHead(self: *Self, link: *Self.Link) void {
        assert(!self.contains(link));

        link.next = self.head;
        link.prev = &self.head;
        self.head = link;

        if (link.next) |next| {
            @branchHint(.likely);
            next.prev = &link.next;
        } else {
            self.tail = &link.next;
        }
    }

    test insertHead {
        var queue: Queue = undefined;
        queue.empty();
        var links: [4]Queue.Link = undefined;

        queue.insertHead(&links[3]);
        queue.insertHead(&links[2]);
        queue.insertHead(&links[1]);
        queue.insertHead(&links[0]);

        try testing.expectEqual(4, queue.len());
        for (&links, 0..) |*link, i|
            try testing.expectEqual(link, queue.get(i));
    }

    /// Inserts the given link at the queue's tail.
    ///
    /// This operation has O(1) complexity.
    pub fn insertTail(self: *Self, link: *Self.Link) void {
        assert(!self.contains(link));

        link.next = null;
        link.prev = self.tail;
        self.tail.* = link;
        self.tail = &link.next;
    }

    test insertTail {
        var queue: Queue = undefined;
        queue.empty();
        var links: [4]Queue.Link = undefined;

        for (&links) |*link|
            queue.insertTail(link);

        try testing.expectEqual(4, queue.len());
        for (&links, 0..) |*link, i|
            try testing.expectEqual(link, queue.get(i));
    }

    /// Inserts the given link after the given predecessor link.
    ///
    /// Asserts that the list contains the predecessor but not the link itself.
    /// The given link may be uninitialized.
    ///
    /// This operation has O(1) complexity.
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

    test insertAfter {
        var queue: Queue = undefined;
        queue.empty();
        var links: [4]Queue.Link = undefined;

        queue.insertHead(&links[0]);
        queue.insertAfter(&links[0], &links[2]);
        queue.insertAfter(&links[0], &links[1]);
        queue.insertAfter(&links[2], &links[3]);

        try testing.expectEqual(4, queue.len());
        for (&links, 0..) |*link, i|
            try testing.expectEqual(link, queue.get(i));
    }

    /// Inserts the given link before the given successor link.
    ///
    /// Asserts that the queue contains the successor but not the link itself.
    /// The given link may be uninitialized.
    ///
    /// This operation has O(1) complexity.
    pub fn insertBefore(self: *Self, before: *Self.Link, link: *Self.Link) void {
        assert(self.contains(before));
        assert(!self.contains(link));

        link.next = before;
        link.prev = before.prev;
        before.prev.* = link;
        before.prev = &link.next;
    }

    test insertBefore {
        var queue: Queue = undefined;
        queue.empty();
        var links: [4]Queue.Link = undefined;

        queue.insertTail(&links[3]);
        queue.insertBefore(&links[3], &links[2]);
        queue.insertBefore(&links[2], &links[1]);
        queue.insertBefore(&links[1], &links[0]);

        try testing.expectEqual(4, queue.len());
        for (&links, 0..) |*link, i|
            try testing.expectEqual(link, queue.get(i));
    }

    /// Replaces an existing link with another one.
    ///
    /// Asserts that the list contains the existing but not the replacement
    /// link.
    /// The given link may be uninitialized.
    ///
    /// This operation has O(1) complexity.
    pub fn replace(self: *Self, old: *Self.Link, new: *Self.Link) void {
        return Mixin(Self).replace(self, old, new);
    }

    test replace {
        var queue: Queue = undefined;
        queue.empty();
        var old_link: Queue.Link = undefined;
        var links: [4]Queue.Link = undefined;

        queue.insertHead(&old_link);
        queue.insertAfter(&old_link, &links[1]);
        queue.insertAfter(&links[1], &links[2]);
        queue.insertAfter(&links[2], &links[3]);

        queue.replace(&old_link, &links[0]);

        for (&links, 0..) |*link, i|
            try testing.expectEqual(link, queue.get(i));
    }

    /// Removes and returns the queue's head.
    ///
    /// This operation has O(1) complexity.
    pub fn removeHead(self: *Self) ?*Self.Link {
        const link = self.head orelse return null;
        self.head = link.next;
        if (link.next) |next| {
            @branchHint(.likely);
            next.prev = link.prev;
        } else {
            self.tail = &self.head;
        }

        link.* = undefined;
        return link;
    }

    /// Removes and returns the queue's tail.
    ///
    /// This operation has O(1) complexity.
    pub fn removeTail(self: *Self) ?*Self.Link {
        const link = self.tail.* orelse return null;
        self.tail = link.prev;
        self.tail.* = null;

        link.* = undefined;
        return link;
    }

    /// Removes and returns the link after the given link.
    ///
    /// Asserts that the list contains the predecessor link.
    ///
    /// This operation has O(1) complexity.
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

    test removeAfter {
        var queue: Queue = undefined;
        queue.empty();
        var links: [4]Queue.Link = undefined;

        for (&links) |*link|
            queue.insertTail(link);
        try testing.expectEqual(&links[1], queue.removeAfter(&links[0]));
        try testing.expectEqual(&links[3], queue.removeAfter(&links[2]));
    }

    /// Removes and returns the link before the given link.
    ///
    /// Asserts that the list contains the sucessor link.
    ///
    /// This operation has O(1) complexity.
    pub fn removeBefore(self: *Self, before: *Self.Link) ?*Self.Link {
        assert(self.contains(before));

        const link = getLink(before.prev) orelse return null;
        link.prev.* = before;
        before.prev = link.prev;

        link.* = undefined;
        return link;
    }

    /// Removes the given link from the queue.
    ///
    /// Asserts that the queue contains the given link.
    ///
    /// This operation has O(1) complexity.
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

/// A double-linked queue cursor that can be moved in both forward and reverse
/// directions and permits before/after link insertion and removal.
pub const Cursor = struct {
    queue: *Queue,
    ptr: *?*Link,

    /// Returns the next link in the iterator's sequence without advancing it.
    pub fn peekNext(self: *Cursor) ?*Link {
        return self.ptr.* orelse return null;
    }

    /// Moves the cursor to the next link.
    pub fn moveNext(self: *Cursor) ?*Link {
        const link = self.peekNext() orelse return null;
        self.ptr = &link.next;
        return link;
    }

    test moveNext {
        var queue: Queue = undefined;
        queue.empty();

        var links: [4]Queue.Link = undefined;
        for (&links) |*link|
            queue.insertTail(link);

        var cur = queue.cursor();
        try testing.expectEqual(&links[0], cur.moveNext());
        try testing.expectEqual(&links[1], cur.moveNext());
        try testing.expectEqual(&links[2], cur.moveNext());
        try testing.expectEqual(&links[3], cur.moveNext());
        try testing.expectEqual(null, cur.moveNext());
    }

    pub fn insertNext(self: *Cursor, link: *Link) void {
        assert(!self.queue.contains(link));

        link.next = self.peekNext();
        link.prev = self.ptr;
        self.ptr.* = link;
        if (link.next == null) {
            @branchHint(.unlikely);
            self.queue.tail = &link.next;
        }
    }

    test insertNext {
        var queue: Queue = undefined;
        queue.empty();

        var links: [4]Queue.Link = undefined;
        queue.insertTail(&links[0]);
        queue.insertTail(&links[1]);
        queue.insertTail(&links[3]);

        var cur = queue.cursor();
        try testing.expectEqual(&links[0], cur.moveNext());
        try testing.expectEqual(&links[1], cur.moveNext());

        cur.insertNext(&links[2]);
        try testing.expectEqual(&links[2], cur.moveNext());
        try testing.expectEqual(&links[3], cur.moveNext());

        for (&links, 0..) |*link, i|
            try testing.expectEqual(link, queue.get(i));
    }

    /// Removes the cursor's successor link.
    pub fn removeNext(self: *Cursor) ?*Link {
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

    pub fn peekPrev(self: *const Cursor) ?*Link {
        return Queue.getLink(self.ptr) orelse return null;
    }

    pub fn movePrev(self: *Cursor) ?*Link {
        const link = self.peekPrev() orelse return null;
        self.ptr = link.prev;
        return link;
    }
};

/// A link in a double-linked list or queue.
pub const Link = extern struct {
    /// The pointer to the next link.
    next: ?*Link,
    /// The pointer to the previous link's next pointer.
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

        fn replace(self: *Self, old: *Link, new: *Link) void {
            assert(self.contains(old));
            assert(!self.contains(new));

            new.* = old.*;
            new.prev.* = new;
            if (new.next) |next| {
                @branchHint(.likely);
                next.prev = &new.next;
            }

            old.* = undefined;
        }
    };
}

const testing = std.testing;

test "queue cursor" {
    var queue: Queue = undefined;
    var links: [4]Queue.Link = undefined;
    queue.empty();

    for (&links) |*link|
        queue.insertTail(link);

    var cur = queue.cursor();
    try testing.expectEqual(&links[0], cur.moveNext());
    try testing.expectEqual(&links[1], cur.moveNext());
    try testing.expectEqual(&links[2], cur.moveNext());
    try testing.expectEqual(&links[3], cur.moveNext());
    try testing.expectEqual(null, cur.moveNext());
    try testing.expectEqual(&links[3], cur.movePrev());
    try testing.expectEqual(&links[2], cur.movePrev());
    try testing.expectEqual(&links[1], cur.movePrev());
    try testing.expectEqual(&links[0], cur.movePrev());
    try testing.expectEqual(null, cur.movePrev());
    try testing.expectEqual(&links[0], cur.moveNext());
}
