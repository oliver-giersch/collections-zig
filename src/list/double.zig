const double = @This();

pub const List = extern struct {
    const Self = @This();

    pub const Link = double.Link;

    head: ?*Self.Link,

    pub const isEmpty = Mixin(Self).isEmpty;
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
    };
}
