// WebSocket Connection Manager
//
// Manages the WebSocket lifecycle to a SpacetimeDB instance:
// connect, authenticate, send/receive messages, reconnect.
//
// This module handles framing and the connection state machine.
// The actual WebSocket transport is abstracted behind a simple interface
// so it can be backed by websocket.zig (Karl Seguin) or a test mock.

const std = @import("std");
const types = @import("types.zig");
const protocol = @import("protocol.zig");

const ClientMessage = protocol.ClientMessage;
const ServerMessage = protocol.ServerMessage;
const Identity = types.Identity;
const ConnectionId = types.ConnectionId;

pub const WS_SUBPROTOCOL = "v2.bsatn.spacetimedb";

pub const Config = struct {
    host: []const u8,
    database: []const u8,
    token: ?[]const u8 = null,
    compression: protocol.Compression = .none,
    max_reconnect_attempts: u32 = 5,
    base_backoff_ms: u64 = 1000,
    max_backoff_ms: u64 = 10000,
};

pub const ConnectionState = enum {
    disconnected,
    connecting,
    connected,
    authenticated,
    closing,
};

/// Events emitted by the connection manager.
pub const Event = union(enum) {
    connected,
    authenticated: struct {
        identity: Identity,
        connection_id: ConnectionId,
        token: []const u8,
    },
    message: ServerMessage,
    disconnected: struct {
        reason: DisconnectReason,
        attempt: u32,
    },
    reconnecting: struct {
        attempt: u32,
    },
    @"error": struct {
        message: []const u8,
    },
};

pub const DisconnectReason = enum {
    normal,
    server_closed,
    transport_error,
    protocol_error,
};

/// Abstract WebSocket transport interface.
/// Implement this for the actual WebSocket library or for testing.
pub const Transport = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        send: *const fn (ptr: *anyopaque, data: []const u8) anyerror!void,
        receive: *const fn (ptr: *anyopaque) anyerror!?[]const u8,
        close: *const fn (ptr: *anyopaque) void,
        isOpen: *const fn (ptr: *anyopaque) bool,
    };

    pub fn send(self: Transport, data: []const u8) !void {
        return self.vtable.send(self.ptr, data);
    }

    pub fn receive(self: Transport) !?[]const u8 {
        return self.vtable.receive(self.ptr);
    }

    pub fn close(self: Transport) void {
        self.vtable.close(self.ptr);
    }

    pub fn isOpen(self: Transport) bool {
        return self.vtable.isOpen(self.ptr);
    }
};

/// The connection manager state machine.
pub const Connection = struct {
    allocator: std.mem.Allocator,
    config: Config,
    state: ConnectionState,
    transport: ?Transport,
    identity: ?Identity,
    connection_id: ?ConnectionId,
    token: ?[]const u8,
    next_request_id: u32,
    next_query_set_id: u32,
    reconnect_attempts: u32,

    pub fn init(allocator: std.mem.Allocator, config: Config) Connection {
        return .{
            .allocator = allocator,
            .config = config,
            .state = .disconnected,
            .transport = null,
            .identity = null,
            .connection_id = null,
            .token = config.token,
            .next_request_id = 1,
            .next_query_set_id = 1,
            .reconnect_attempts = 0,
        };
    }

    pub fn deinit(self: *Connection) void {
        if (self.transport) |t| {
            t.close();
        }
        self.state = .disconnected;
    }

    /// Build the WebSocket URL for this connection.
    pub fn buildUrl(self: *const Connection, allocator: std.mem.Allocator) ![]u8 {
        const compression_str = switch (self.config.compression) {
            .none => "None",
            .gzip => "Gzip",
            .brotli => "Brotli",
        };
        return std.fmt.allocPrint(
            allocator,
            "ws://{s}/v1/database/{s}/subscribe?compression={s}",
            .{ self.config.host, self.config.database, compression_str },
        );
    }

    /// Connect using the provided transport.
    pub fn connect(self: *Connection, transport: Transport) void {
        self.transport = transport;
        self.state = .connected;
        self.reconnect_attempts = 0;
    }

    /// Get the next request ID (monotonically increasing).
    pub fn nextRequestId(self: *Connection) u32 {
        const id = self.next_request_id;
        self.next_request_id += 1;
        return id;
    }

    /// Get the next query set ID.
    pub fn nextQuerySetId(self: *Connection) u32 {
        const id = self.next_query_set_id;
        self.next_query_set_id += 1;
        return id;
    }

    /// Send a client message over the WebSocket.
    pub fn sendMessage(self: *Connection, msg: ClientMessage) !void {
        const transport = self.transport orelse return error.NotConnected;
        const encoded = try msg.encode(self.allocator);
        defer self.allocator.free(encoded);
        try transport.send(encoded);
    }

    /// Send a Subscribe message. Returns the request_id and query_set_id.
    pub fn subscribe(self: *Connection, queries: []const []const u8) !struct { request_id: u32, query_set_id: u32 } {
        const request_id = self.nextRequestId();
        const query_set_id = self.nextQuerySetId();
        try self.sendMessage(.{ .subscribe = .{
            .request_id = request_id,
            .query_set_id = query_set_id,
            .query_strings = queries,
        } });
        return .{ .request_id = request_id, .query_set_id = query_set_id };
    }

    /// Send a CallReducer message. Returns the request_id.
    pub fn callReducer(self: *Connection, reducer_name: []const u8, args: []const u8) !u32 {
        const request_id = self.nextRequestId();
        try self.sendMessage(.{ .call_reducer = .{
            .request_id = request_id,
            .reducer = reducer_name,
            .args = args,
        } });
        return request_id;
    }

    /// Send a OneOffQuery. Returns the request_id.
    pub fn oneOffQuery(self: *Connection, query: []const u8) !u32 {
        const request_id = self.nextRequestId();
        try self.sendMessage(.{ .one_off_query = .{
            .request_id = request_id,
            .query_string = query,
        } });
        return request_id;
    }

    /// Process a received binary frame into a ServerMessage.
    /// Handles the compression envelope for server messages.
    pub fn processFrame(self: *Connection, frame: []const u8) !Event {
        const result = try protocol.decompress(self.allocator, frame);
        defer result.deinit(self.allocator);

        const msg = try ServerMessage.decode(self.allocator, result.data);

        // Handle InitialConnection specially — update our state
        switch (msg) {
            .initial_connection => |ic| {
                self.identity = ic.identity;
                self.connection_id = ic.connection_id;
                self.token = ic.token;
                self.state = .authenticated;
                return .{ .authenticated = .{
                    .identity = ic.identity,
                    .connection_id = ic.connection_id,
                    .token = ic.token,
                } };
            },
            else => return .{ .message = msg },
        }
    }

    /// Calculate backoff delay for reconnection.
    pub fn backoffMs(self: *const Connection) u64 {
        const delay = self.config.base_backoff_ms * (@as(u64, self.reconnect_attempts) + 1);
        return @min(delay, self.config.max_backoff_ms);
    }

    /// Should we attempt reconnection?
    pub fn shouldReconnect(self: *const Connection) bool {
        return self.reconnect_attempts < self.config.max_reconnect_attempts;
    }

    /// Record a disconnect and increment attempt counter.
    pub fn recordDisconnect(self: *Connection) Event {
        self.state = .disconnected;
        self.transport = null;
        const attempt = self.reconnect_attempts;
        self.reconnect_attempts += 1;
        return .{ .disconnected = .{
            .reason = .transport_error,
            .attempt = attempt,
        } };
    }
};

pub const SendError = error{NotConnected} || std.mem.Allocator.Error;

// ============================================================
// Tests
// ============================================================

/// Mock transport for testing.
const MockTransport = struct {
    sent_data: std.ArrayListUnmanaged([]u8),
    receive_queue: std.ArrayListUnmanaged([]const u8),
    is_open: bool,
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) MockTransport {
        return .{
            .sent_data = .{},
            .receive_queue = .{},
            .is_open = true,
            .allocator = allocator,
        };
    }

    fn deinit(self: *MockTransport) void {
        for (self.sent_data.items) |data| self.allocator.free(data);
        self.sent_data.deinit(self.allocator);
        self.receive_queue.deinit(self.allocator);
    }

    fn transport(self: *MockTransport) Transport {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &.{
                .send = @ptrCast(&sendFn),
                .receive = @ptrCast(&receiveFn),
                .close = @ptrCast(&closeFn),
                .isOpen = @ptrCast(&isOpenFn),
            },
        };
    }

    fn sendFn(self: *MockTransport, data: []const u8) !void {
        const copy = try self.allocator.dupe(u8, data);
        try self.sent_data.append(self.allocator, copy);
    }

    fn receiveFn(self: *MockTransport) !?[]const u8 {
        if (self.receive_queue.items.len > 0) {
            return self.receive_queue.orderedRemove(0);
        }
        return null;
    }

    fn closeFn(self: *MockTransport) void {
        self.is_open = false;
    }

    fn isOpenFn(self: *MockTransport) bool {
        return self.is_open;
    }
};

test "Connection init" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, .{
        .host = "localhost:3000",
        .database = "test_db",
    });
    defer conn.deinit();

    try std.testing.expect(conn.state == .disconnected);
    try std.testing.expectEqual(@as(u32, 1), conn.next_request_id);
    try std.testing.expectEqual(@as(u32, 1), conn.next_query_set_id);
}

test "Connection buildUrl" {
    const allocator = std.testing.allocator;
    const conn = Connection.init(allocator, .{
        .host = "localhost:3000",
        .database = "my_db",
        .compression = .none,
    });

    const url = try conn.buildUrl(allocator);
    defer allocator.free(url);

    try std.testing.expectEqualStrings(
        "ws://localhost:3000/v1/database/my_db/subscribe?compression=None",
        url,
    );
}

test "Connection buildUrl with gzip" {
    const allocator = std.testing.allocator;
    const conn = Connection.init(allocator, .{
        .host = "example.com",
        .database = "prod",
        .compression = .gzip,
    });

    const url = try conn.buildUrl(allocator);
    defer allocator.free(url);

    try std.testing.expectEqualStrings(
        "ws://example.com/v1/database/prod/subscribe?compression=Gzip",
        url,
    );
}

test "Connection request ID incrementing" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, .{
        .host = "localhost",
        .database = "test",
    });

    try std.testing.expectEqual(@as(u32, 1), conn.nextRequestId());
    try std.testing.expectEqual(@as(u32, 2), conn.nextRequestId());
    try std.testing.expectEqual(@as(u32, 3), conn.nextRequestId());
}

test "Connection query set ID incrementing" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, .{
        .host = "localhost",
        .database = "test",
    });

    try std.testing.expectEqual(@as(u32, 1), conn.nextQuerySetId());
    try std.testing.expectEqual(@as(u32, 2), conn.nextQuerySetId());
}

test "Connection connect sets state" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, .{
        .host = "localhost",
        .database = "test",
    });
    defer conn.deinit();

    var mock = MockTransport.init(allocator);
    defer mock.deinit();
    conn.connect(mock.transport());

    try std.testing.expect(conn.state == .connected);
}

test "Connection send message via mock" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, .{
        .host = "localhost",
        .database = "test",
    });
    defer conn.deinit();

    var mock = MockTransport.init(allocator);
    defer mock.deinit();
    conn.connect(mock.transport());

    const queries = [_][]const u8{"SELECT * FROM users"};
    const result = try conn.subscribe(&queries);

    try std.testing.expectEqual(@as(u32, 1), result.request_id);
    try std.testing.expectEqual(@as(u32, 1), result.query_set_id);
    try std.testing.expectEqual(@as(usize, 1), mock.sent_data.items.len);
}

test "Connection backoff calculation" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, .{
        .host = "localhost",
        .database = "test",
        .base_backoff_ms = 1000,
        .max_backoff_ms = 5000,
    });

    conn.reconnect_attempts = 0;
    try std.testing.expectEqual(@as(u64, 1000), conn.backoffMs());

    conn.reconnect_attempts = 1;
    try std.testing.expectEqual(@as(u64, 2000), conn.backoffMs());

    conn.reconnect_attempts = 4;
    try std.testing.expectEqual(@as(u64, 5000), conn.backoffMs());

    conn.reconnect_attempts = 10;
    try std.testing.expectEqual(@as(u64, 5000), conn.backoffMs()); // capped
}

test "Connection shouldReconnect" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, .{
        .host = "localhost",
        .database = "test",
        .max_reconnect_attempts = 3,
    });

    conn.reconnect_attempts = 0;
    try std.testing.expect(conn.shouldReconnect());

    conn.reconnect_attempts = 2;
    try std.testing.expect(conn.shouldReconnect());

    conn.reconnect_attempts = 3;
    try std.testing.expect(!conn.shouldReconnect());
}

test "Connection processFrame InitialConnection" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, .{
        .host = "localhost",
        .database = "test",
    });
    defer conn.deinit();

    var mock = MockTransport.init(allocator);
    defer mock.deinit();
    conn.connect(mock.transport());

    // Build a server frame: compression(0x00) + InitialConnection message
    const bsatn_mod = @import("bsatn.zig");
    var enc = bsatn_mod.Encoder.init();
    defer enc.deinit(allocator);

    try enc.encodeU8(allocator, 0x00); // no compression
    try enc.encodeU8(allocator, 0); // InitialConnection tag
    var identity: [32]u8 = undefined;
    @memset(&identity, 0xAA);
    try enc.appendRaw(allocator, &identity);
    var conn_id: [16]u8 = undefined;
    @memset(&conn_id, 0xBB);
    try enc.appendRaw(allocator, &conn_id);
    try enc.encodeString(allocator, "test-token-123");

    const event = try conn.processFrame(enc.writtenSlice());

    try std.testing.expect(event == .authenticated);
    try std.testing.expect(conn.state == .authenticated);
    try std.testing.expectEqualSlices(u8, &identity, &conn.identity.?);
    try std.testing.expectEqualStrings("test-token-123", conn.token.?);
}

test "Connection recordDisconnect" {
    const allocator = std.testing.allocator;
    var conn = Connection.init(allocator, .{
        .host = "localhost",
        .database = "test",
    });

    var mock = MockTransport.init(allocator);
    defer mock.deinit();
    conn.connect(mock.transport());

    const event = conn.recordDisconnect();
    try std.testing.expect(event == .disconnected);
    try std.testing.expect(conn.state == .disconnected);
    try std.testing.expectEqual(@as(u32, 1), conn.reconnect_attempts);
}
