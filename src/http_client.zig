// HTTP REST Client for SpacetimeDB
//
// Provides HTTP endpoint wrappers for schema fetching, identity management,
// and database operations. Uses Zig's std.http.Client.
//
// Primary endpoints:
//   GET  /v1/database/{name}/schema?version=9  — Fetch schema
//   POST /v1/identity                          — Create identity
//   GET  /v1/identity/{id}/verify              — Verify token
//   POST /v1/database/{name}/call/{reducer}    — Call reducer via HTTP
//   POST /v1/database/{name}/sql               — Execute SQL

const std = @import("std");
const schema_mod = @import("schema.zig");

const Schema = schema_mod.Schema;

pub const HttpError = error{
    ConnectionFailed,
    RequestFailed,
    InvalidResponse,
    Unauthorized,
    NotFound,
    ServerError,
} || schema_mod.SchemaError || std.mem.Allocator.Error || std.fmt.BufPrintError;

/// Configuration for the HTTP client.
pub const Config = struct {
    host: []const u8,
    database: []const u8,
    token: ?[]const u8 = null,
};

/// Build a base URL from config.
pub fn buildBaseUrl(allocator: std.mem.Allocator, host: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "http://{s}/v1", .{host});
}

/// Build the schema fetch URL.
pub fn buildSchemaUrl(allocator: std.mem.Allocator, config: Config) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "http://{s}/v1/database/{s}/schema?version=9",
        .{ config.host, config.database },
    );
}

/// Build the identity creation URL.
pub fn buildIdentityUrl(allocator: std.mem.Allocator, host: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "http://{s}/v1/identity", .{host});
}

/// Build a reducer call URL.
pub fn buildReducerUrl(allocator: std.mem.Allocator, config: Config, reducer_name: []const u8) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "http://{s}/v1/database/{s}/call/{s}",
        .{ config.host, config.database, reducer_name },
    );
}

/// Build a SQL query URL.
pub fn buildSqlUrl(allocator: std.mem.Allocator, config: Config) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "http://{s}/v1/database/{s}/sql",
        .{ config.host, config.database },
    );
}

/// Build the ping URL.
pub fn buildPingUrl(allocator: std.mem.Allocator, host: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "http://{s}/v1/ping", .{host});
}

/// Build an Authorization header value.
pub fn buildAuthHeader(allocator: std.mem.Allocator, token: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "Bearer {s}", .{token});
}

/// HTTP response from a SpacetimeDB endpoint.
pub const Response = struct {
    status: u16,
    body: []const u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *const Response) void {
        self.allocator.free(self.body);
    }

    pub fn isSuccess(self: *const Response) bool {
        return self.status >= 200 and self.status < 300;
    }
};

/// Abstract HTTP transport for testability.
/// In production, backed by std.http.Client. In tests, by a mock.
pub const HttpTransport = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        get: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, url: []const u8, auth: ?[]const u8) anyerror!Response,
        post: *const fn (ptr: *anyopaque, allocator: std.mem.Allocator, url: []const u8, body: ?[]const u8, auth: ?[]const u8) anyerror!Response,
    };

    pub fn get(self: HttpTransport, allocator: std.mem.Allocator, url: []const u8, auth: ?[]const u8) !Response {
        return self.vtable.get(self.ptr, allocator, url, auth);
    }

    pub fn post(self: HttpTransport, allocator: std.mem.Allocator, url: []const u8, body: ?[]const u8, auth: ?[]const u8) !Response {
        return self.vtable.post(self.ptr, allocator, url, body, auth);
    }
};

/// High-level HTTP client that wraps transport + config.
pub const Client = struct {
    allocator: std.mem.Allocator,
    config: Config,
    transport: HttpTransport,

    pub fn init(allocator: std.mem.Allocator, config: Config, transport: HttpTransport) Client {
        return .{
            .allocator = allocator,
            .config = config,
            .transport = transport,
        };
    }

    /// Fetch and parse the database schema.
    pub fn fetchSchema(self: *Client) !Schema {
        const url = try buildSchemaUrl(self.allocator, self.config);
        defer self.allocator.free(url);

        const resp = try self.transport.get(self.allocator, url, self.config.token);
        defer resp.deinit();

        if (!resp.isSuccess()) return HttpError.RequestFailed;

        return schema_mod.parse(self.allocator, resp.body);
    }

    /// Create a new identity. Returns the JSON response body (caller owns).
    pub fn createIdentity(self: *Client) ![]const u8 {
        const url = try buildIdentityUrl(self.allocator, self.config.host);
        defer self.allocator.free(url);

        const resp = try self.transport.post(self.allocator, url, null, null);
        if (!resp.isSuccess()) {
            resp.deinit();
            return HttpError.RequestFailed;
        }

        return resp.body; // Transfer ownership
    }

    /// Call a reducer via HTTP POST.
    pub fn callReducer(self: *Client, reducer_name: []const u8, args_json: []const u8) !Response {
        const url = try buildReducerUrl(self.allocator, self.config, reducer_name);
        defer self.allocator.free(url);

        var auth_header: ?[]u8 = null;
        if (self.config.token) |t| {
            auth_header = try buildAuthHeader(self.allocator, t);
        }
        defer if (auth_header) |h| self.allocator.free(h);

        return self.transport.post(self.allocator, url, args_json, auth_header);
    }

    /// Execute a SQL query via HTTP POST.
    pub fn executeSql(self: *Client, sql: []const u8) !Response {
        const url = try buildSqlUrl(self.allocator, self.config);
        defer self.allocator.free(url);

        var auth_header: ?[]u8 = null;
        if (self.config.token) |t| {
            auth_header = try buildAuthHeader(self.allocator, t);
        }
        defer if (auth_header) |h| self.allocator.free(h);

        return self.transport.post(self.allocator, url, sql, auth_header);
    }

    /// Ping the server.
    pub fn ping(self: *Client) !bool {
        const url = try buildPingUrl(self.allocator, self.config.host);
        defer self.allocator.free(url);

        const resp = try self.transport.get(self.allocator, url, null);
        defer resp.deinit();

        return resp.isSuccess();
    }
};

// ============================================================
// Tests
// ============================================================

test "buildSchemaUrl" {
    const allocator = std.testing.allocator;
    const url = try buildSchemaUrl(allocator, .{
        .host = "localhost:3000",
        .database = "my_db",
    });
    defer allocator.free(url);
    try std.testing.expectEqualStrings(
        "http://localhost:3000/v1/database/my_db/schema?version=9",
        url,
    );
}

test "buildIdentityUrl" {
    const allocator = std.testing.allocator;
    const url = try buildIdentityUrl(allocator, "localhost:3000");
    defer allocator.free(url);
    try std.testing.expectEqualStrings(
        "http://localhost:3000/v1/identity",
        url,
    );
}

test "buildReducerUrl" {
    const allocator = std.testing.allocator;
    const url = try buildReducerUrl(allocator, .{
        .host = "localhost:3000",
        .database = "test_db",
    }, "say_hello");
    defer allocator.free(url);
    try std.testing.expectEqualStrings(
        "http://localhost:3000/v1/database/test_db/call/say_hello",
        url,
    );
}

test "buildSqlUrl" {
    const allocator = std.testing.allocator;
    const url = try buildSqlUrl(allocator, .{
        .host = "example.com",
        .database = "prod",
    });
    defer allocator.free(url);
    try std.testing.expectEqualStrings(
        "http://example.com/v1/database/prod/sql",
        url,
    );
}

test "buildPingUrl" {
    const allocator = std.testing.allocator;
    const url = try buildPingUrl(allocator, "localhost:3000");
    defer allocator.free(url);
    try std.testing.expectEqualStrings(
        "http://localhost:3000/v1/ping",
        url,
    );
}

test "buildAuthHeader" {
    const allocator = std.testing.allocator;
    const header = try buildAuthHeader(allocator, "my-jwt-token");
    defer allocator.free(header);
    try std.testing.expectEqualStrings("Bearer my-jwt-token", header);
}

test "Response isSuccess" {
    const allocator = std.testing.allocator;
    const body = try allocator.dupe(u8, "ok");

    var r200 = Response{ .status = 200, .body = body, .allocator = allocator };
    try std.testing.expect(r200.isSuccess());
    r200.deinit();

    const body2 = try allocator.dupe(u8, "err");
    var r404 = Response{ .status = 404, .body = body2, .allocator = allocator };
    try std.testing.expect(!r404.isSuccess());
    r404.deinit();

    const body3 = try allocator.dupe(u8, "");
    var r204 = Response{ .status = 204, .body = body3, .allocator = allocator };
    try std.testing.expect(r204.isSuccess());
    r204.deinit();
}

/// Real HTTP transport using Zig's std.http.Client.
pub const StdHttpTransport = struct {
    pub fn transport(self: *StdHttpTransport) HttpTransport {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &.{
                .get = @ptrCast(&doGet),
                .post = @ptrCast(&doPost),
            },
        };
    }

    fn doGet(_: *StdHttpTransport, allocator: std.mem.Allocator, url: []const u8, auth: ?[]const u8) !Response {
        return doRequest(allocator, url, .GET, null, auth);
    }

    fn doPost(_: *StdHttpTransport, allocator: std.mem.Allocator, url: []const u8, body: ?[]const u8, auth: ?[]const u8) !Response {
        return doRequest(allocator, url, .POST, body, auth);
    }

    fn doRequest(
        allocator: std.mem.Allocator,
        url_str: []const u8,
        method: std.http.Method,
        body: ?[]const u8,
        auth: ?[]const u8,
    ) !Response {
        var client: std.http.Client = .{ .allocator = allocator };
        defer client.deinit();

        var resp_writer = std.Io.Writer.Allocating.init(allocator);
        defer resp_writer.deinit();

        // Zig 0.15 asserts POST must have a body; use empty string if null
        const payload: ?[]const u8 = body orelse if (method == .POST) "" else null;

        const result = client.fetch(.{
            .location = .{ .url = url_str },
            .method = method,
            .payload = payload,
            .extra_headers = if (auth) |a|
                &.{.{ .name = "authorization", .value = a }}
            else
                &.{},
            .response_writer = &resp_writer.writer,
        }) catch return HttpError.ConnectionFailed;

        const status: u16 = @intFromEnum(result.status);

        // Take ownership of the collected body
        var list = resp_writer.toArrayList();
        const resp_body = list.toOwnedSlice(allocator) catch {
            list.deinit(allocator);
            return HttpError.InvalidResponse;
        };

        return .{
            .status = status,
            .body = resp_body,
            .allocator = allocator,
        };
    }
};

/// Mock transport for testing.
const MockHttpTransport = struct {
    response_status: u16,
    response_body: []const u8,

    fn transport(self: *MockHttpTransport) HttpTransport {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &.{
                .get = @ptrCast(&getFn),
                .post = @ptrCast(&postFn),
            },
        };
    }

    fn getFn(self: *MockHttpTransport, allocator: std.mem.Allocator, _: []const u8, _: ?[]const u8) !Response {
        return .{
            .status = self.response_status,
            .body = try allocator.dupe(u8, self.response_body),
            .allocator = allocator,
        };
    }

    fn postFn(self: *MockHttpTransport, allocator: std.mem.Allocator, _: []const u8, _: ?[]const u8, _: ?[]const u8) !Response {
        return .{
            .status = self.response_status,
            .body = try allocator.dupe(u8, self.response_body),
            .allocator = allocator,
        };
    }
};

test "Client ping success" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "pong",
    };

    var client = Client.init(allocator, .{
        .host = "localhost:3000",
        .database = "test",
    }, mock.transport());

    const result = try client.ping();
    try std.testing.expect(result);
}

test "Client ping failure" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 503,
        .response_body = "unavailable",
    };

    var client = Client.init(allocator, .{
        .host = "localhost:3000",
        .database = "test",
    }, mock.transport());

    const result = try client.ping();
    try std.testing.expect(!result);
}
