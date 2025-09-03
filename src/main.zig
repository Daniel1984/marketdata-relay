const std = @import("std");
const stream = @import("./stream.zig");

var last_message_time: i64 = 0;

fn handleMessage(_: []const u8) void {
    const current_time = std.time.milliTimestamp();

    if (last_message_time == 0) {
        std.debug.print("Received (first message)\n", .{});
    } else {
        const time_diff = current_time - last_message_time;
        const seconds: f64 = @as(f64, @floatFromInt(time_diff)) / 1000.0;
        std.debug.print("Received: (time since last: {d:.3}s)\n", .{seconds});
    }

    last_message_time = current_time;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var s = try stream.init(allocator, .{});
    defer s.deinit();

    try s.consume(handleMessage);
}
