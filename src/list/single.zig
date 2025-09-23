const single = @This();

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
pub const List = struct {
    const Self = @This();

    /// The initializer value for an empty list.
    pub const empty: Self = .{ .head = null };

    /// The link type for connecting list items.
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

    pub fn insertHead(self: *Self, link: *Self.Link) void {
        assert(!self.contains(link));

        link.next = self.head;
        self.head = link;
    }

    /// Inserts the given link after the given predecessor link.
    ///
    /// Asserts that the list contains the predecessor but not the link itself.
    /// The given link may be uninitialized.
    ///
    /// This operation is O(n).
    ///
    /// # Examples
    ///
    /// ```
    /// var list: List = .empty;
    /// var links: [2]List.Link = undefined;
    ///
    /// list.insertHead(&links[0]);
    /// list.insertAfter(&links[0], &links[1]);
    /// ```
    pub fn insertAfter(self: *Self, after: *Self.Link, link: *Self.Link) void {
        assert(self.contains(after));
        assert(!self.contains(link));

        link.next = after.next;
        after.next = link;
    }

    /// Removes the head link from the list.
    ///
    /// This operation is O(1).
    pub fn removeHead(self: *Self) ?*Self.Link {
        const link = self.head orelse return null;
        self.head = link.next;

        link.* = undefined;
        return link;
    }

    /// Removes the given link from the link.
    ///
    /// Asserts that the link is contained in the list.
    ///
    /// This operation is O(n) in the worst case.
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

    /// Appends the given list to the tail of this list.
    pub fn concat(self: *Self, other: *const Self) void {
        if (other.isEmpty()) {
            @branchHint(.unlikely);
            return;
        }

        self.tail.* = other.head;
        self.tail = other.tail;
    }

    /// Inserts the given link at the head of the list.
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

    pub fn removeTail(self: *Self) ?*Self.Link {
        const link = self.tail.* orelse return null;
        const prev = Mixin(Self).findPrev(self, link);
        self.tail = prev;
        prev.* = null;

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
        pub fn next(self: *Self) ?Item {
            const link = self.peekNext() orelse return null;
            self.link = link.next;
            return link;
        }
    };
}

fn Mixin(comptime Self: type) type {
    return struct {
        /// Returns true if the list is empty.
        ///
        /// This operation is O(1).
        pub fn isEmpty(self: *const Self) bool {
            return self.head == null;
        }

        /// Returns true if the list contains the given link.
        ///
        /// This operation is O(n) in the worst case.
        pub fn contains(self: *const Self, link: *const Link) bool {
            var it = self.constIter();
            while (it.next()) |curr| {
                if (curr == link)
                    return true;
            }

            return false;
        }

        /// Returns the length of the list.
        ///
        /// This operation is O(n). Consider storing and maintaining the length
        /// separately, if it is needed frequently.
        pub fn len(self: *const Self) usize {
            var it = self.constIter();
            var count: usize = 0;
            while (it.next()) |_| {
                count += 1;
            }

            return count;
        }

        /// Returns a constant iterator over the items in the list.
        pub fn constIter(self: *const Self) GenericIterator(true) {
            return .{ .ptr = &self.head };
        }

        /// Returns an iterator over the items in the list.
        pub fn iter(self: *Self) GenericIterator(false) {
            return .{ .ptr = &self.head };
        }

        fn findPrev(self: *Self, link: *const Link) *?*Link {
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

const std = @import("std");
const assert = std.debug.assert;

const tt = std.testing;

test "slist insert head" {
    var list: List = .empty;
    try tt.expect(list.isEmpty());
    var links: [2]List.Link = undefined;

    list.insertHead(&links[0]);
    list.insertHead(&links[1]);

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
