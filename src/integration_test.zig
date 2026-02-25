// Integration Tests Against Live SpacetimeDB
//
// Requires a running SpacetimeDB at localhost:3000 with testmodule published.
// testmodule has: Person table (id u64, name string, age u32),
// reducers: add_person(name string, age u32), say_hello().
//
// Run: zig build integration-test

const std = @import("std");
const http_client = @import("http_client.zig");
const schema_mod = @import("schema.zig");
const protocol = @import("protocol.zig");
const websocket = @import("websocket.zig");
const bsatn = @import("bsatn.zig");
const types = @import("types.zig");
const value_encoder = @import("value_encoder.zig");

const HOST = "localhost:3000";
const DATABASE = "testmodule";

/// Create an HTTP client using the StdHttpTransport.
/// Caller must keep the returned transport alive for the client's lifetime.
fn makeHttpClient(allocator: std.mem.Allocator, transport: *http_client.StdHttpTransport) http_client.Client {
    return http_client.Client.init(allocator, .{
        .host = HOST,
        .database = DATABASE,
    }, transport.transport());
}

/// Free allocated QuerySetUpdate memory from server messages.
fn freeQuerySets(allocator: std.mem.Allocator, query_sets: []const protocol.QuerySetUpdate) void {
    for (query_sets) |qs| {
        for (qs.tables) |t| allocator.free(t.rows);
        allocator.free(qs.tables);
    }
    allocator.free(query_sets);
}

/// Free allocated memory from a ReducerOutcome.
fn freeReducerOutcome(allocator: std.mem.Allocator, result: protocol.ReducerOutcome) void {
    switch (result) {
        .ok => |ok_result| freeQuerySets(allocator, ok_result.transaction),
        .ok_empty => {},
        .err => {},     // borrowed from frame buffer
        .internal_error => {}, // borrowed from frame buffer
    }
}

/// Receive the next data frame from a transport, skipping null returns
/// (ping/pong frames). Retries up to `max_attempts` times.
fn receiveFrame(allocator: std.mem.Allocator, transport: websocket.Transport, max_attempts: u32) ![]const u8 {
    for (0..max_attempts) |_| {
        if (try transport.receive()) |frame| {
            return frame;
        }
        // null means ping/pong — try again
    }
    // If we still have nothing, try one more blocking receive
    _ = allocator;
    return error.TestUnexpectedResult;
}

// ============================================================
// Test 1: HTTP Ping
// ============================================================

test "HTTP: Ping server" {
    const allocator = std.testing.allocator;
    var transport = http_client.StdHttpTransport{};
    var client = makeHttpClient(allocator, &transport);
    const ok = try client.ping();
    try std.testing.expect(ok);
}

// ============================================================
// Test 2: HTTP Fetch Schema
// ============================================================

test "HTTP: Fetch schema and verify structure" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var transport = http_client.StdHttpTransport{};
    var client = makeHttpClient(allocator, &transport);
    const s = try client.fetchSchema();

    // Verify person table
    const person = s.getTable("person") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 3), person.columns.len);

    // Verify column names and types
    try std.testing.expectEqualStrings("id", person.columns[0].name.?);
    try std.testing.expect(person.columns[0].type == .u64);
    try std.testing.expectEqualStrings("name", person.columns[1].name.?);
    try std.testing.expect(person.columns[1].type == .string);
    try std.testing.expectEqualStrings("age", person.columns[2].name.?);
    try std.testing.expect(person.columns[2].type == .u32);

    // Verify reducers
    const add_person = s.getReducer("add_person") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 2), add_person.params.len);

    const say_hello = s.getReducer("say_hello") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), say_hello.params.len);
}

// ============================================================
// Test 3: HTTP Create Identity
// ============================================================

test "HTTP: Create identity" {
    const allocator = std.testing.allocator;
    var transport = http_client.StdHttpTransport{};
    var client = makeHttpClient(allocator, &transport);
    const body = try client.createIdentity();
    defer allocator.free(body);

    // Should be JSON with identity and token fields
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{});
    defer parsed.deinit();

    const root = parsed.value;
    // Check that identity field exists
    const identity = switch (root) {
        .object => |obj| obj.get("identity"),
        else => null,
    };
    try std.testing.expect(identity != null);

    // Check that token field exists
    const token = switch (root) {
        .object => |obj| obj.get("token"),
        else => null,
    };
    try std.testing.expect(token != null);
}

// ============================================================
// Test 4: WebSocket Connect and Authenticate
// ============================================================

test "WebSocket: Connect and receive InitialConnection" {
    const allocator = std.testing.allocator;

    var conn = websocket.Connection.init(allocator, .{
        .host = HOST,
        .database = DATABASE,
    });
    defer conn.deinit();

    try conn.connectReal();

    // Read the first frame — should be InitialConnection
    const transport = conn.transport.?;
    const frame = try receiveFrame(allocator, transport, 10);
    defer allocator.free(frame);

    const event = try conn.processFrame(frame);

    switch (event) {
        .authenticated => |auth| {
            // We got an identity and token
            try std.testing.expect(auth.token.len > 0);
            try std.testing.expect(conn.state == .authenticated);
        },
        else => return error.TestUnexpectedResult,
    }
}

// ============================================================
// Test 5: WebSocket Subscribe
// ============================================================

test "WebSocket: Subscribe to person table" {
    const allocator = std.testing.allocator;

    var conn = websocket.Connection.init(allocator, .{
        .host = HOST,
        .database = DATABASE,
    });
    defer conn.deinit();

    try conn.connectReal();

    // Read InitialConnection
    const transport = conn.transport.?;
    {
        const frame = try receiveFrame(allocator, transport, 10);
        defer allocator.free(frame);
        _ = try conn.processFrame(frame);
    }

    // Subscribe
    const queries = [_][]const u8{"SELECT * FROM person"};
    _ = try conn.subscribe(&queries);

    // Read SubscribeApplied
    const sub_frame = try receiveFrame(allocator, transport, 10);
    defer allocator.free(sub_frame);

    const sub_event = try conn.processFrame(sub_frame);
    switch (sub_event) {
        .message => |msg| {
            switch (msg) {
                .subscribe_applied => |sa| {
                    // We got initial rows (could be 0 or more)
                    try std.testing.expect(sa.rows.len > 0);
                    try std.testing.expectEqualStrings("person", sa.rows[0].table_name);
                    allocator.free(sa.rows);
                },
                else => return error.TestUnexpectedResult,
            }
        },
        else => return error.TestUnexpectedResult,
    }
}

// ============================================================
// Test 6: WebSocket Call Reducer
// ============================================================

test "WebSocket: Call add_person reducer" {
    const allocator = std.testing.allocator;

    var conn = websocket.Connection.init(allocator, .{
        .host = HOST,
        .database = DATABASE,
    });
    defer conn.deinit();

    try conn.connectReal();

    const transport = conn.transport.?;

    // Read InitialConnection
    {
        const frame = try receiveFrame(allocator, transport, 10);
        defer allocator.free(frame);
        _ = try conn.processFrame(frame);
    }

    // Subscribe first so we can see the transaction update
    const queries = [_][]const u8{"SELECT * FROM person"};
    _ = try conn.subscribe(&queries);

    // Read SubscribeApplied
    {
        const frame = try receiveFrame(allocator, transport, 10);
        defer allocator.free(frame);
        const event = try conn.processFrame(frame);
        switch (event) {
            .message => |msg| {
                switch (msg) {
                    .subscribe_applied => |sa| allocator.free(sa.rows),
                    else => {},
                }
            },
            else => {},
        }
    }

    // Encode add_person args: name (string) + age (u32)
    const unique_name = "zig-integration-test";
    const params = [_]types.Column{
        .{ .name = "name", .type = .string },
        .{ .name = "age", .type = .u32 },
    };
    const fields = [_]types.AlgebraicValue.FieldValue{
        .{ .name = "name", .value = .{ .string = unique_name } },
        .{ .name = "age", .value = .{ .u32 = 99 } },
    };
    const args = try value_encoder.encodeReducerArgs(allocator, &fields, &params);
    defer allocator.free(args);

    _ = try conn.callReducer("add_person", args);

    // Read server responses — expect ReducerResult or TransactionUpdate
    // The server may send multiple messages; we look for a ReducerResult
    var got_reducer_result = false;
    for (0..20) |_| {
        const frame = receiveFrame(allocator, transport, 5) catch break;
        defer allocator.free(frame);

        const event = conn.processFrame(frame) catch continue;
        switch (event) {
            .message => |msg| {
                switch (msg) {
                    .reducer_result => |rr| {
                        freeReducerOutcome(allocator, rr.result);
                        got_reducer_result = true;
                        break;
                    },
                    .transaction_update => |tu| freeQuerySets(allocator, tu.query_sets),
                    else => {},
                }
            },
            else => {},
        }
    }

    try std.testing.expect(got_reducer_result);
}

// ============================================================
// Test 7: Call say_hello reducer (no args)
// ============================================================

test "WebSocket: Call say_hello reducer" {
    const allocator = std.testing.allocator;

    var conn = websocket.Connection.init(allocator, .{
        .host = HOST,
        .database = DATABASE,
    });
    defer conn.deinit();

    try conn.connectReal();

    const transport = conn.transport.?;

    // Read InitialConnection
    {
        const frame = try receiveFrame(allocator, transport, 10);
        defer allocator.free(frame);
        _ = try conn.processFrame(frame);
    }

    // Call say_hello with empty args
    _ = try conn.callReducer("say_hello", &[_]u8{});

    // Read ReducerResult
    var got_result = false;
    for (0..20) |_| {
        const frame = receiveFrame(allocator, transport, 5) catch break;
        defer allocator.free(frame);

        const event = conn.processFrame(frame) catch continue;
        switch (event) {
            .message => |msg| {
                switch (msg) {
                    .reducer_result => |rr| {
                        freeReducerOutcome(allocator, rr.result);
                        got_result = true;
                        break;
                    },
                    else => {},
                }
            },
            else => {},
        }
    }

    try std.testing.expect(got_result);
}
