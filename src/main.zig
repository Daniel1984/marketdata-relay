const std = @import("std");
const nats = @import("./nats-client.zig");

fn handleMessage(msg: []const u8) void {
    std.debug.print("Received: {s}\n", .{msg});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var nc = try nats.init(allocator, .{});
    defer nc.deinit();

    try nc.consume(handleMessage);
}
