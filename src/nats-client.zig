const std = @import("std");

const Self = @This();

allocator: std.mem.Allocator,
topic: []const u8,
host: []const u8,
port: u16,
shouldConsume: bool,

pub const NatsOpts = struct {
    topic: []const u8 = "market",
    host: []const u8 = "127.0.0.1",
    port: u16 = 4222,
};

pub fn init(allocator: std.mem.Allocator, opts: NatsOpts) !Self {
    return Self{
        .allocator = allocator,
        .topic = try allocator.dupe(u8, opts.topic),
        .host = try allocator.dupe(u8, opts.host),
        .port = opts.port,
        .shouldConsume = true,
    };
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.topic);
    self.allocator.free(self.host);
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

fn connectToNats(self: *Self) !std.net.Stream {
    std.debug.print("Connecting to NATS server...\n", .{});

    const stream = try std.net.tcpConnectToHost(self.allocator, self.host, self.port);

    // Read INFO message
    var buf: [1024]u8 = undefined;
    _ = try stream.read(&buf);

    // Send CONNECT
    _ = try stream.writeAll("CONNECT {}\r\n");

    // Subscribe to subject
    const sub_command = try std.fmt.allocPrint(self.allocator, "SUB {s} 1\r\n", .{self.topic});
    defer self.allocator.free(sub_command);
    _ = try stream.writeAll(sub_command);

    std.debug.print("Connected and subscribed to '{s}'\n", .{self.topic});
    return stream;
}

pub fn consume(self: *Self, cb: fn ([]const u8) void) !void {
    var buf: [1024]u8 = undefined;

    while (self.shouldConsume) {
        // Establish connection
        const stream = self.connectToNats() catch |err| {
            std.debug.print("Connection failed: {}, retrying in 5 seconds...\n", .{err});
            std.Thread.sleep(5 * std.time.ns_per_s);
            continue;
        };
        defer stream.close();

        // Message processing loop
        while (self.shouldConsume) {
            const n = stream.read(&buf) catch |err| {
                std.debug.print("Read error: {}, reconnecting...\n", .{err});
                break; // Break inner loop to reconnect
            };

            if (n > 0) {
                const msg = buf[0..n];

                // Handle PING - respond with PONG to keep connection alive
                if (std.mem.indexOf(u8, msg, "PING") != null) {
                    stream.writeAll("PONG\r\n") catch |err| {
                        std.debug.print("Failed to send PONG: {}, reconnecting...\n", .{err});
                        break;
                    };
                    std.debug.print("Sent PONG (keepalive)\n", .{});
                    continue;
                }

                // Handle stale connection - need to reconnect
                if (std.mem.indexOf(u8, msg, "-ERR 'Stale Connection'") != null) {
                    std.debug.print("Stale connection detected, reconnecting...\n", .{});
                    break; // Break inner loop to reconnect
                }

                // Handle regular messages
                cb(msg);
            } else if (n == 0) {
                std.debug.print("Connection closed by server, reconnecting...\n", .{});
                break; // Break inner loop to reconnect
            }
        }

        std.debug.print("Reconnecting in 2 seconds...\n", .{});
        std.Thread.sleep(2 * std.time.ns_per_s);
    }
}

// const OrderBookUpdate = struct {
//     topic: []const u8,
//     type: []const u8,
//     subject: []const u8,
//     data: struct {
//         asks: [][][2][]const u8, // array of arrays ["price","size"]
//         bids: [][][2][]const u8,
//         timestamp: i64,
//     },
// };

// pub fn parsePayload(allocator: std.mem.Allocator, payload: []const u8) !OrderBookUpdate {
//     var parsed = try std.json.parse(OrderBookUpdate, &std.json.TokenStream.init(payload), .{
//         .allocator = allocator,
//     });
//     defer parsed.deinit();

//     return parsed.value;
// }

// fn parseHeader(line: []const u8) !usize {
//     // Example: "MSG market 1 375"
//     var it = std.mem.tokenizeAny(u8, line, " ");
//     _ = it.next(); // skip "MSG"
//     _ = it.next(); // subject
//     _ = it.next(); // sid
//     const size_str = it.next() orelse return error.BadHeader;

//     return try std.fmt.parseInt(usize, size_str, 10);
// }

// // below reading message from raw nats string response, can be applied to other sevices
// fn parseMessage(line: []const u8) !void {
//     while (true) {
//         var buf: [2048]u8 = undefined;
//         const n = try stream.read(&buf);
//         if (n == 0) continue;

//         // Split into header and payload
//         if (std.mem.indexOf(u8, buf[0..n], "\r\n")) |idx| {
//             const header_line = buf[0..idx];
//             const size = try parseHeader(header_line);

//             // Payload starts after header + \r\n
//             const start = idx + 2;
//             const payload = buf[start .. start + size];

//             std.debug.print("Got JSON payload: {s}\n", .{payload});

//             // (Optional) Parse into struct
//             const update = try parsePayload(allocator, payload);
//             std.debug.print("Update for {s}, bids={d}, asks={d}\n", .{ update.topic, update.data.bids.len, update.data.asks.len });
//         }
//     }
// }
