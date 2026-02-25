// SpacetimeDB High-Level Client
//
// Ties together WebSocket connection, client cache, schema, and HTTP client
// into a single unified API. Provides callback-driven event handling and
// both iterator and callback access to table data.
//
// Lifecycle: init → connect → authenticate → subscribe → handle events
//
// Users implement the EventHandler interface to receive callbacks for
// connection events, row changes, reducer results, etc.

const std = @import("std");
const types = @import("types.zig");
const protocol = @import("protocol.zig");
const schema_mod = @import("schema.zig");
const row_decoder = @import("row_decoder.zig");
const value_encoder = @import("value_encoder.zig");
const client_cache = @import("client_cache.zig");
const websocket = @import("websocket.zig");
const http_client = @import("http_client.zig");

const Identity = types.Identity;
const ConnectionId = types.ConnectionId;
const AlgebraicValue = types.AlgebraicValue;
const Column = types.Column;
const Schema = schema_mod.Schema;
const Row = row_decoder.Row;
const RowChange = client_cache.RowChange;
const ClientCache = client_cache.ClientCache;
const Connection = websocket.Connection;
const ServerMessage = protocol.ServerMessage;

/// Configuration for the SpacetimeDB client.
pub const ClientConfig = struct {
    host: []const u8,
    database: []const u8,
    token: ?[]const u8 = null,
    compression: protocol.Compression = .none,
    subscriptions: []const []const u8 = &.{},
    max_reconnect_attempts: u32 = 5,
    base_backoff_ms: u64 = 1000,
    max_backoff_ms: u64 = 10000,
};

/// Event handler interface. Implement this to receive callbacks.
pub const EventHandler = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        onConnect: ?*const fn (ptr: *anyopaque, identity: Identity, connection_id: ConnectionId, token: []const u8) void = null,
        onDisconnect: ?*const fn (ptr: *anyopaque, reason: websocket.DisconnectReason) void = null,
        onSubscribeApplied: ?*const fn (ptr: *anyopaque, table_name: []const u8, row_count: usize) void = null,
        onInsert: ?*const fn (ptr: *anyopaque, table_name: []const u8, row: *const Row) void = null,
        onDelete: ?*const fn (ptr: *anyopaque, table_name: []const u8, row: *const Row) void = null,
        onUpdate: ?*const fn (ptr: *anyopaque, table_name: []const u8, old_row: *const Row, new_row: *const Row) void = null,
        onReducerResult: ?*const fn (ptr: *anyopaque, request_id: u32, result: protocol.ReducerOutcome) void = null,
        onError: ?*const fn (ptr: *anyopaque, message: []const u8) void = null,
    };

    pub fn onConnect(self: EventHandler, identity: Identity, connection_id: ConnectionId, token: []const u8) void {
        if (self.vtable.onConnect) |f| f(self.ptr, identity, connection_id, token);
    }

    pub fn onDisconnect(self: EventHandler, reason: websocket.DisconnectReason) void {
        if (self.vtable.onDisconnect) |f| f(self.ptr, reason);
    }

    pub fn onSubscribeApplied(self: EventHandler, table_name: []const u8, row_count: usize) void {
        if (self.vtable.onSubscribeApplied) |f| f(self.ptr, table_name, row_count);
    }

    pub fn onInsert(self: EventHandler, table_name: []const u8, row: *const Row) void {
        if (self.vtable.onInsert) |f| f(self.ptr, table_name, row);
    }

    pub fn onDelete(self: EventHandler, table_name: []const u8, row: *const Row) void {
        if (self.vtable.onDelete) |f| f(self.ptr, table_name, row);
    }

    pub fn onUpdate(self: EventHandler, table_name: []const u8, old_row: *const Row, new_row: *const Row) void {
        if (self.vtable.onUpdate) |f| f(self.ptr, table_name, old_row, new_row);
    }

    pub fn onReducerResult(self: EventHandler, request_id: u32, result: protocol.ReducerOutcome) void {
        if (self.vtable.onReducerResult) |f| f(self.ptr, request_id, result);
    }

    pub fn onError(self: EventHandler, message: []const u8) void {
        if (self.vtable.onError) |f| f(self.ptr, message);
    }

    /// Create a no-op handler (for testing or when no callbacks needed).
    pub fn noop() EventHandler {
        const S = struct {
            var dummy: u8 = 0;
        };
        return .{
            .ptr = @ptrCast(&S.dummy),
            .vtable = &.{},
        };
    }
};

/// The high-level SpacetimeDB client.
pub const SpacetimeClient = struct {
    allocator: std.mem.Allocator,
    config: ClientConfig,
    connection: Connection,
    cache: ClientCache,
    schema: ?Schema,
    handler: EventHandler,
    /// Track which query_set_ids map to which subscriptions.
    active_subscriptions: std.AutoHashMapUnmanaged(u32, []const []const u8),

    pub fn init(allocator: std.mem.Allocator, config: ClientConfig, handler: EventHandler) SpacetimeClient {
        const ws_config = websocket.Config{
            .host = config.host,
            .database = config.database,
            .token = config.token,
            .compression = config.compression,
            .max_reconnect_attempts = config.max_reconnect_attempts,
            .base_backoff_ms = config.base_backoff_ms,
            .max_backoff_ms = config.max_backoff_ms,
        };

        // Cache initialized with empty schema; replaced after schema fetch.
        const empty_schema = Schema{
            .tables = &.{},
            .reducers = &.{},
            .typespace = &.{},
            .allocator = allocator,
        };

        return .{
            .allocator = allocator,
            .config = config,
            .connection = Connection.init(allocator, ws_config),
            .cache = ClientCache.init(allocator, empty_schema),
            .schema = null,
            .handler = handler,
            .active_subscriptions = .{},
        };
    }

    pub fn deinit(self: *SpacetimeClient) void {
        self.connection.deinit();
        self.cache.deinit();
        self.active_subscriptions.deinit(self.allocator);
    }

    /// Set the schema (typically after HTTP fetch).
    pub fn setSchema(self: *SpacetimeClient, s: Schema) void {
        self.schema = s;
        // Reinitialize cache with new schema
        self.cache.deinit();
        self.cache = ClientCache.init(self.allocator, s);
    }

    /// Connect using the provided transport.
    pub fn connect(self: *SpacetimeClient, transport: websocket.Transport) void {
        self.connection.connect(transport);
    }

    /// Build the WebSocket URL for this client.
    pub fn buildUrl(self: *const SpacetimeClient) ![]u8 {
        return self.connection.buildUrl(self.allocator);
    }

    /// Subscribe to SQL queries. Returns the query_set_id.
    pub fn subscribe(self: *SpacetimeClient, queries: []const []const u8) !u32 {
        const result = try self.connection.subscribe(queries);
        try self.active_subscriptions.put(self.allocator, result.query_set_id, queries);
        return result.query_set_id;
    }

    /// Subscribe to the configured default subscriptions.
    pub fn subscribeDefault(self: *SpacetimeClient) !?u32 {
        if (self.config.subscriptions.len == 0) return null;
        return try self.subscribe(self.config.subscriptions);
    }

    /// Call a reducer by name with pre-encoded BSATN args.
    pub fn callReducerRaw(self: *SpacetimeClient, reducer_name: []const u8, args: []const u8) !u32 {
        return self.connection.callReducer(reducer_name, args);
    }

    /// Call a reducer by name with field values (auto-encoded via schema).
    pub fn callReducer(
        self: *SpacetimeClient,
        reducer_name: []const u8,
        fields: []const AlgebraicValue.FieldValue,
    ) !u32 {
        const s = self.schema orelse return error.NoSchema;
        const reducer = s.getReducer(reducer_name) orelse return error.UnknownReducer;
        const args = try value_encoder.encodeReducerArgs(self.allocator, fields, reducer.params);
        defer self.allocator.free(args);
        return self.connection.callReducer(reducer_name, args);
    }

    /// Execute a one-off SQL query.
    pub fn query(self: *SpacetimeClient, sql: []const u8) !u32 {
        return self.connection.oneOffQuery(sql);
    }

    // ============================================================
    // Cache access (read-only)
    // ============================================================

    /// Get all rows from a table.
    pub fn getAll(self: *SpacetimeClient, table_name: []const u8) ![]const Row {
        return self.cache.getTableRows(table_name);
    }

    /// Get the number of rows in a table.
    pub fn count(self: *SpacetimeClient, table_name: []const u8) usize {
        return self.cache.tableRowCount(table_name);
    }

    /// Get the parsed schema.
    pub fn getSchema(self: *const SpacetimeClient) ?Schema {
        return self.schema;
    }

    // ============================================================
    // Event processing
    // ============================================================

    /// Process a raw server frame (compression envelope + BSATN).
    /// Call this when the WebSocket receives a binary message.
    pub fn processFrame(self: *SpacetimeClient, frame: []const u8) !void {
        const event = try self.connection.processFrame(frame);
        try self.handleEvent(event);
    }

    /// Handle a connection event.
    fn handleEvent(self: *SpacetimeClient, event: websocket.Event) !void {
        switch (event) {
            .connected => {},
            .authenticated => |auth| {
                self.handler.onConnect(auth.identity, auth.connection_id, auth.token);
            },
            .message => |msg| {
                try self.handleServerMessage(msg);
            },
            .disconnected => |dc| {
                self.handler.onDisconnect(dc.reason);
            },
            .reconnecting => {},
            .@"error" => |e| {
                self.handler.onError(e.message);
            },
        }
    }

    /// Handle a decoded server message.
    fn handleServerMessage(self: *SpacetimeClient, msg: ServerMessage) !void {
        switch (msg) {
            .initial_connection => {
                // Already handled by processFrame → authenticated event
            },
            .subscribe_applied => |sa| {
                const changes = try self.cache.applySubscribeApplied(sa.rows);
                defer self.allocator.free(changes);
                // Fire per-table subscribe callbacks
                for (sa.rows) |tr| {
                    self.handler.onSubscribeApplied(tr.table_name, 0);
                }
                // Fire per-row callbacks
                self.dispatchChanges(changes);
            },
            .transaction_update => |tu| {
                const changes = try self.cache.applyTransactionUpdate(tu.query_sets);
                defer self.allocator.free(changes);
                self.dispatchChanges(changes);
            },
            .reducer_result => |rr| {
                // If reducer result contains a transaction, apply it
                switch (rr.result) {
                    .ok => |ok_result| {
                        const changes = try self.cache.applyTransactionUpdate(ok_result.transaction);
                        defer self.allocator.free(changes);
                        self.dispatchChanges(changes);
                    },
                    else => {},
                }
                self.handler.onReducerResult(rr.request_id, rr.result);
            },
            .subscription_error => |se| {
                self.handler.onError(se.@"error");
            },
            .one_off_query_result => {},
            .unsubscribe_applied => {},
            .procedure_result => {},
        }
    }

    /// Dispatch row changes to the appropriate handler callbacks.
    fn dispatchChanges(self: *SpacetimeClient, changes: []const RowChange) void {
        for (changes) |change| {
            switch (change) {
                .insert => |ins| self.handler.onInsert(ins.table_name, ins.row),
                .delete => |del| self.handler.onDelete(del.table_name, del.row),
                .update => |upd| self.handler.onUpdate(upd.table_name, upd.old_row, upd.new_row),
            }
        }
    }
};

pub const ClientError = error{
    NoSchema,
    UnknownReducer,
} || value_encoder.EncodeError || websocket.SendError || protocol.Error;

// ============================================================
// Tests
// ============================================================

test "SpacetimeClient init and deinit" {
    const allocator = std.testing.allocator;
    var client = SpacetimeClient.init(allocator, .{
        .host = "localhost:3000",
        .database = "test_db",
    }, EventHandler.noop());
    defer client.deinit();

    try std.testing.expect(client.schema == null);
    try std.testing.expectEqual(@as(usize, 0), client.count("users"));
}

test "SpacetimeClient buildUrl" {
    const allocator = std.testing.allocator;
    var client = SpacetimeClient.init(allocator, .{
        .host = "localhost:3000",
        .database = "my_db",
    }, EventHandler.noop());
    defer client.deinit();

    const url = try client.buildUrl();
    defer allocator.free(url);
    try std.testing.expectEqualStrings(
        "ws://localhost:3000/v1/database/my_db/subscribe?compression=None",
        url,
    );
}

test "SpacetimeClient setSchema" {
    const allocator = std.testing.allocator;
    var client = SpacetimeClient.init(allocator, .{
        .host = "localhost:3000",
        .database = "test_db",
    }, EventHandler.noop());
    defer client.deinit();

    const tables = [_]schema_mod.TableDef{.{
        .name = "users",
        .columns = &[_]Column{
            .{ .name = "id", .type = .u64 },
            .{ .name = "name", .type = .string },
        },
        .primary_key = &[_]u32{0},
    }};
    const s = Schema{
        .tables = &tables,
        .reducers = &.{},
        .typespace = &.{},
        .allocator = allocator,
    };

    client.setSchema(s);
    try std.testing.expect(client.schema != null);
}

test "EventHandler noop" {
    const handler = EventHandler.noop();
    // Should not crash when called
    var identity: Identity = undefined;
    @memset(&identity, 0);
    var conn_id: ConnectionId = undefined;
    @memset(&conn_id, 0);
    handler.onConnect(identity, conn_id, "token");
    handler.onDisconnect(.normal);
    handler.onError("test error");
}

test "SpacetimeClient subscribe tracking" {
    const allocator = std.testing.allocator;

    const MockWsTransport = struct {
        sent: std.ArrayListUnmanaged([]u8),
        alloc: std.mem.Allocator,

        fn init(a: std.mem.Allocator) @This() {
            return .{ .sent = .{}, .alloc = a };
        }
        fn deinitSelf(self: *@This()) void {
            for (self.sent.items) |d| self.alloc.free(d);
            self.sent.deinit(self.alloc);
        }
        fn transport(self: *@This()) websocket.Transport {
            return .{
                .ptr = @ptrCast(self),
                .vtable = &.{
                    .send = @ptrCast(&sendFn),
                    .receive = @ptrCast(&recvFn),
                    .close = @ptrCast(&closeFn),
                    .isOpen = @ptrCast(&isOpenFn),
                },
            };
        }
        fn sendFn(self: *@This(), data: []const u8) !void {
            try self.sent.append(self.alloc, try self.alloc.dupe(u8, data));
        }
        fn recvFn(_: *@This()) !?[]const u8 {
            return null;
        }
        fn closeFn(_: *@This()) void {}
        fn isOpenFn(_: *@This()) bool {
            return true;
        }
    };

    var mock = MockWsTransport.init(allocator);
    defer mock.deinitSelf();

    var client = SpacetimeClient.init(allocator, .{
        .host = "localhost:3000",
        .database = "test_db",
    }, EventHandler.noop());
    defer client.deinit();

    client.connect(mock.transport());

    const queries = [_][]const u8{"SELECT * FROM users"};
    const qs_id = try client.subscribe(&queries);

    try std.testing.expectEqual(@as(u32, 1), qs_id);
    try std.testing.expectEqual(@as(usize, 1), mock.sent.items.len);
}
