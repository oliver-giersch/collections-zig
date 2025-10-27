const single = @This();

const std = @import("std");
const assert = std.debug.assert;

/// An intrusive, single-linked list.
///
/// This type of list is the most space efficient and is well suited for head
/// insertion and forward iteration, but less than ideal for removal of links
/// other than head and does not allow tail insertion.
///
/// All insertion methods assume uninitialized link arguments and will fully
/// initialize any link that is inserted.
/// All removal methods must be assumed to uninitialize the contents of the
/// removed links but not the containing structures.
///
/// The user is entirely responsible for managing the lifetime of list links.
/// Each link must live for at least as long as it is part of the list.
pub const List = struct {
    const Self = @This();

    /// The initializer value for an empty list.
    pub const empty: Self = .{ .head = null };

    /// The link type for connecting list items.
    pub const Link = single.Link;

    /// A forward const iterator over all links in the list.
    pub const ConstIterator = GenericIterator(true);
    /// A forward iterator over all links in the list.
    pub const Iterator = GenericIterator(false);

    /// The list's head link.
    head: ?*Self.Link,

    /// Returns true if the list is empty.
    ///
    /// This operation has O(1) complexity.
    pub fn isEmpty(self: *const Self) bool {
        return Mixin(Self).isEmpty(self);
    }

    test isEmpty {
        var list: List = .empty;
        try testing.expect(list.isEmpty());
    }

    /// Returns true if the list contains the given link.
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

    /// Returns the link at the given index.
    ///
    /// This operation has O(n) complexity in the worst case.
    pub fn getConst(self: *const Self, idx: usize) ?*const Self.Link {
        return Mixin(Self).getConst(self, idx);
    }

    /// Returns an iterator over the items in the list.
    pub fn iter(self: *Self) Iterator {
        return Mixin(Self).iter(self);
    }

    test iter {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        var it = list.iter();
        var i: usize = 0;

        while (it.next()) |link| {
            try testing.expectEqual(&links[i], link);
            i += 1;
        }
    }

    /// Returns a const iterator over the items in the list.
    pub fn constIter(self: *const Self) ConstIterator {
        return Mixin(Self).constIter(self);
    }

    test constIter {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        var it = list.constIter();
        var i: usize = 0;

        while (it.next()) |link| {
            try testing.expectEqual(&links[i], link);
            i += 1;
        }
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
        self.head = link;
    }

    test insertHead {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        try testing.expectEqual(&links[0], list.getConst(0));
        try testing.expectEqual(&links[1], list.getConst(1));
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
        after.next = link;
    }

    test insertAfter {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[0]);
        list.insertAfter(&links[0], &links[1]);

        try testing.expectEqual(&links[0], list.getConst(0));
        try testing.expectEqual(&links[1], list.getConst(1));
    }

    /// Removes and returns the list's head.
    ///
    /// This operation has O(1) complexity.
    pub fn removeHead(self: *Self) ?*Self.Link {
        const link = self.head orelse return null;
        self.head = link.next;

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
    /// This operation has O(1) complexity.
    pub fn removeAfter(self: *Self, after: *Self.Link) ?*Self.Link {
        assert(self.contains(after));

        const link = after.next orelse return null;
        after.next = link.next;
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

    /// Removes the given link from the link.
    ///
    /// Asserts that the link is contained in the list.
    ///
    /// This operation has O(n) complexity in the worst case.
    pub fn remove(self: *Self, link: *Self.Link) void {
        assert(self.contains(link));

        const prev = Mixin(Self).findPrev(self, link);
        prev.* = link.next;
        link.* = undefined;
    }

    test remove {
        var list: List = .empty;
        var links: [2]List.Link = undefined;

        list.insertHead(&links[1]);
        list.insertHead(&links[0]);

        list.remove(&links[0]);
        try testing.expect(!list.contains(&links[0]));
        try testing.expect(list.contains(&links[1]));
    }
};

/// An intrusive, single-linked queue optimized for tail-insertion.
pub const Queue = struct {
    const Self = @This();

    /// The link type for connecting queue items.
    pub const Link = single.Link;

    pub const ConstIterator = GenericIterator(true);
    pub const Iterator = GenericIterator(false);

    /// The queue's head.
    head: ?*Self.Link,
    /// The pointer to the tail of the queue.
    ///
    /// This is a pointer to the pointer to the tail link.
    /// Points at `&self.head`, if the queue is empty.
    ///
    /// Copying an empty queue invalidates this field.
    tail: *?*Self.Link,

    /// Initialize the queue as empty.
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

        queue.insertTail(&links[0]);
        queue.insertTail(&links[1]);
        try testing.expect(queue.contains(&links[0]));
        try testing.expect(queue.contains(&links[1]));
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
        var queue: Queue = undefined;
        var links: [2]Queue.Link = undefined;

        queue.insertTail(&links[0]);
        queue.insertTail(&links[1]);
        try testing.expectEqual(2, queue.len());
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

    /// Returns an iterator over the items in the list.
    pub fn iter(self: *Self) Iterator {
        return Mixin(Self).iter(self);
    }

    /// Returns a const iterator over the items in the list.
    pub fn constIter(self: *const Self) ConstIterator {
        return Mixin(Self).constIter(self);
    }

    /// Appends the given list to this list's tail.
    pub fn concat(self: *Self, other: *const Self) void {
        if (other.isEmpty()) {
            @branchHint(.unlikely);
            return;
        }

        self.tail.* = other.head;
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

    /// Inserts the given link at the list's head.
    ///
    /// This operation has O(1) complexity.
    pub fn insertHead(self: *Self, link: *Self.Link) void {
        assert(!self.contains(link));

        link.next = self.head;
        self.head = link;
        if (link.next == null) {
            @branchHint(.unlikely);
            self.tail = &link.next;
        }
    }

    /// Inserts the given link at the queue's tail.
    ///
    /// This operation has O(1) complexity.
    pub fn insertTail(self: *Self, link: *Self.Link) void {
        assert(!self.contains(link));

        link.next = null;
        self.tail.* = link;
        self.tail = &link.next;
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
        after.next = link;
        if (link.next == null) {
            @branchHint(.unlikely);
            self.tail = &link.next;
        }
    }

    /// Removes and returns the queue's head.
    ///
    /// This operation has O(1) complexity.
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

    /// Removes and returns the queue's tail.
    ///
    /// This operation has O(1) complexity.
    pub fn removeTail(self: *Self) ?*Self.Link {
        const link = self.tail.* orelse return null;
        const prev = Mixin(Self).findPrev(self, link);
        self.tail = prev;
        prev.* = null;

        link.* = undefined;
        return link;
    }

    /// Removes and returns the link after the given link.
    ///
    /// This operation has O(1) complexity.
    pub fn removeAfter(self: *Self, after: *Self.Link) ?*Self.Link {
        assert(self.contains(after));

        const link = after.next orelse return null;
        after.next = link.next;

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

        const prev = Mixin(Self).findPrev(self, link);
        prev.* = link.next;
        if (link.next == null) {
            @branchHint(.unlikely);
            self.tail = prev;
        }
        link.* = undefined;
    }

    test remove {
        var queue: Queue = undefined;
        queue.empty();
        var links: [2]Queue.Link = undefined;

        queue.insertHead(&links[0]);
        queue.insertHead(&links[1]);

        queue.remove(&links[0]);
        try testing.expect(!queue.contains(&links[0]));
        try testing.expect(queue.contains(&links[1]));
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

        /// The item returned by each iteration.
        pub const Item = if (is_const) *const Link else *Link;

        /// The pointer to the next item in the iterator sequence.
        link: ?Item,

        /// Returns the next item in the iterator sequence without advancing
        /// the iterator.
        pub fn peekNext(self: *const Self) ?Item {
            return self.link;
        }

        /// Returns the next item in the iterator sequence.
        ///
        /// The iterator does not depend on items once they have been returned
        /// by it, meaning callers are free to handle them as they see fit,
        /// including removing them and freeing their memory.
        pub fn next(self: *Self) ?Item {
            const link = self.peekNext() orelse return null;
            self.link = link.next;
            return link;
        }
    };
}

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

        fn constIter(self: *const Self) GenericIterator(true) {
            return .{ .link = self.head };
        }

        fn iter(self: *Self) GenericIterator(false) {
            return .{ .link = self.head };
        }

        fn findPrev(self: *Self, link: *Link) *?*Link {
            var prev: *?*Link = &self.head;
            while (prev.*) |curr| {
                if (curr == link)
                    return prev;
                prev = &link.next;
            }

            unreachable;
        }
    };
}

const testing = std.testing;

test "list is empty" {
    var list: List = .empty;
    var links: [2]List.Link = undefined;
    try testing.expect(list.isEmpty());

    list.head = &links[0];
    links[0].next = &links[1];
    links[1].next = null;
    try testing.expect(!list.isEmpty());
}

test "list iter" {
    var list: List = .empty;
    var links: [2]List.Link = undefined;

    list.head = &links[0];
    links[0].next = &links[1];
    links[1].next = null;

    var it = list.iter();
    const l0 = it.next() orelse unreachable;
    try testing.expectEqual(&links[0], l0);
    const l1 = it.next() orelse unreachable;
    try testing.expectEqual(&links[1], l1);
    try testing.expectEqual(null, it.next());
}

test "list const iter" {
    var list: List = .empty;
    var links: [2]List.Link = undefined;

    list.head = &links[0];
    links[0].next = &links[1];
    links[1].next = null;

    var it = list.constIter();
    const l0 = it.next() orelse unreachable;
    try testing.expectEqual(&links[0], l0);
    const l1 = it.next() orelse unreachable;
    try testing.expectEqual(&links[1], l1);
    try testing.expectEqual(null, it.next());
}

test "list iter remove and free" {
    const allocator = testing.allocator;

    var list: List = .empty;
    var links: [4]*List.Link = undefined;

    for (&links) |*link|
        link.* = try allocator.create(List.Link);

    list.insertHead(links[3]);
    list.insertHead(links[2]);
    list.insertHead(links[1]);
    list.insertHead(links[0]);

    var it = list.iter();
    var i: usize = 0;

    while (it.next()) |link| {
        try testing.expectEqual(links[i], link);
        list.remove(link);
        allocator.destroy(link);
        i += 1;
    }

    try testing.expectEqual(0, list.len());
    try testing.expect(list.isEmpty());
}

test "list contains" {
    var list: List = .empty;
    var links: [2]List.Link = undefined;

    list.head = &links[0];
    links[0].next = &links[1];
    links[1].next = null;

    try testing.expect(list.contains(&links[0]));
    try testing.expect(list.contains(&links[1]));
}

test "list len" {
    var list: List = .empty;
    var links: [2]List.Link = undefined;

    list.head = &links[0];
    links[0].next = &links[1];
    links[1].next = null;

    try testing.expectEqual(2, list.len());
}

test "get" {
    var list: List = .empty;
    var links: [4]List.Link = undefined;

    list.insertHead(&links[0]);
    list.insertAfter(&links[0], &links[1]);
    list.insertAfter(&links[1], &links[2]);
    list.insertAfter(&links[2], &links[3]);

    try testing.expectEqual(&links[0], list.get(0));
    try testing.expectEqual(&links[1], list.get(1));
    try testing.expectEqual(&links[2], list.get(2));
    try testing.expectEqual(&links[3], list.get(3));
    try testing.expectEqual(null, list.get(4));
}

test "get const" {
    var list: List = .empty;
    var links: [4]List.Link = undefined;

    list.insertHead(&links[0]);
    list.insertAfter(&links[0], &links[1]);
    list.insertAfter(&links[1], &links[2]);
    list.insertAfter(&links[2], &links[3]);

    try testing.expectEqual(&links[0], list.getConst(0));
    try testing.expectEqual(&links[1], list.getConst(1));
    try testing.expectEqual(&links[2], list.getConst(2));
    try testing.expectEqual(&links[3], list.getConst(3));
    try testing.expectEqual(null, list.getConst(4));
}

test "list insert head" {
    var list: List = .empty;
    var links: [2]List.Link = undefined;

    list.insertHead(&links[0]);
    list.insertHead(&links[1]);

    try testing.expect(list.contains(&links[0]));
    try testing.expect(list.contains(&links[1]));
}

test "list insert after" {
    var list: List = .empty;
    var links: [4]List.Link = undefined;

    list.insertHead(&links[0]);
    list.insertAfter(&links[0], &links[1]);
    list.insertAfter(&links[1], &links[2]);
    list.insertAfter(&links[2], &links[3]);

    var it = list.constIter();
    const l0 = it.next() orelse unreachable;
    const l1 = it.next() orelse unreachable;
    const l2 = it.next() orelse unreachable;
    const l3 = it.next() orelse unreachable;
    try testing.expectEqual(null, it.next());

    try testing.expectEqual(l0, &links[0]);
    try testing.expectEqual(l1, &links[1]);
    try testing.expectEqual(l2, &links[2]);
    try testing.expectEqual(l3, &links[3]);
}

test "queue remove" {
    var queue: Queue = undefined;
    queue.empty();
    var links: [2]Queue.Link = undefined;

    queue.insertTail(&links[0]);
    queue.insertTail(&links[1]);

    queue.remove(&links[1]);
    queue.remove(&links[0]);
    try testing.expect(queue.isEmpty());
}
