const std = @import("std");
const zimq = @import("zimq");

const Self = @This();

allocator: std.mem.Allocator,
shouldConsume: bool,
stream_url: [:0]const u8,
context: ?*zimq.Context,

pub const Opts = struct {
    stream_url: []const u8 = "tcp://localhost:5555",
};

pub fn init(allocator: std.mem.Allocator, opts: Opts) !Self {
    return Self{
        .allocator = allocator,
        .stream_url = try allocator.dupeZ(u8, opts.stream_url),
        .shouldConsume = true,
        .context = null,
    };
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.stream_url);
    self.deinitStream();
}

fn deinitStream(self: *Self) void {
    if (self.context) |ctx| {
        ctx.deinit();
        self.context = null;
    }
}

pub fn stop(self: *Self) void {
    self.shouldConsume = false;
}

pub fn start(self: *Self) void {
    self.shouldConsume = true;
}

pub fn isRunning(self: *Self) bool {
    return self.shouldConsume;
}

fn connectSocket(self: *Self) !*zimq.Socket {
    self.deinitStream();
    std.debug.print("connecting to data stream...\n", .{});

    self.context = try zimq.Context.init();
    const socket = try zimq.Socket.init(self.context.?, .pull);
    try socket.bind(self.stream_url);

    std.debug.print("data stream connected!\n", .{});
    return socket;
}

pub fn consume(self: *Self, cb: fn ([]const u8) void) !void {
    var buff = zimq.Message.empty();

    while (self.shouldConsume) {
        // Establish connection
        const stream = self.connectSocket() catch |err| {
            std.debug.print("Connection failed: {}, retrying in 5 seconds...\n", .{err});
            std.Thread.sleep(5 * std.time.ns_per_s);
            continue;
        };
        defer stream.deinit();

        // Message processing loop
        while (self.shouldConsume) {
            _ = stream.recvMsg(&buff, .{}) catch |err| {
                std.debug.print("Failed to receive message from stream: {}, reconnecting...\n", .{err});
                break;
            };
            const msg = buff.slice();
            cb(msg);
        }

        std.debug.print("Reconnecting in 2 seconds...\n", .{});
        std.Thread.sleep(2 * std.time.ns_per_s);
    }
}
