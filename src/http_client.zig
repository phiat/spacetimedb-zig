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

/// Build the verify identity URL.
pub fn buildVerifyIdentityUrl(allocator: std.mem.Allocator, host: []const u8, identity_hex: []const u8) ![]u8 {
    return std.fmt.allocPrint(allocator, "http://{s}/v1/identity/{s}/verify", .{ host, identity_hex });
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

    /// Verify an identity token. Returns true if the token is valid for the identity.
    pub fn verifyIdentity(self: *Client, identity_hex: []const u8, token: []const u8) !bool {
        const url = try buildVerifyIdentityUrl(self.allocator, self.config.host, identity_hex);
        defer self.allocator.free(url);

        const auth = try buildAuthHeader(self.allocator, token);
        defer self.allocator.free(auth);

        const resp = try self.transport.get(self.allocator, url, auth);
        defer resp.deinit();

        return resp.isSuccess();
    }

    /// Ping the server.
    pub fn ping(self: *Client) !bool {
        const url = try buildPingUrl(self.allocator, self.config.host);
        defer self.allocator.free(url);

        const resp = try self.transport.get(self.allocator, url, null);
        defer resp.deinit();

        return resp.isSuccess();
    }

    // ============================================================
    // Database management endpoints
    // ============================================================

    /// Get database metadata. Returns JSON response body (caller owns).
    pub fn getDatabase(self: *Client, name_or_identity: []const u8) !Response {
        const url = try std.fmt.allocPrint(self.allocator, "http://{s}/v1/database/{s}", .{ self.config.host, name_or_identity });
        defer self.allocator.free(url);
        return self.transport.get(self.allocator, url, null);
    }

    /// Get database identity hex string. Returns JSON response body (caller owns).
    pub fn getDatabaseIdentity(self: *Client, name_or_identity: []const u8) !Response {
        const url = try std.fmt.allocPrint(self.allocator, "http://{s}/v1/database/{s}/identity", .{ self.config.host, name_or_identity });
        defer self.allocator.free(url);
        return self.transport.get(self.allocator, url, null);
    }

    /// Publish a WASM module to a database.
    pub fn publishDatabase(self: *Client, name_or_identity: []const u8, wasm_binary: []const u8, token: []const u8) !Response {
        const url = try std.fmt.allocPrint(self.allocator, "http://{s}/v1/database/{s}", .{ self.config.host, name_or_identity });
        defer self.allocator.free(url);
        const auth = try buildAuthHeader(self.allocator, token);
        defer self.allocator.free(auth);
        return self.transport.post(self.allocator, url, wasm_binary, auth);
    }

    /// List all names for a database.
    pub fn getDatabaseNames(self: *Client, name_or_identity: []const u8) !Response {
        const url = try std.fmt.allocPrint(self.allocator, "http://{s}/v1/database/{s}/names", .{ self.config.host, name_or_identity });
        defer self.allocator.free(url);
        return self.transport.get(self.allocator, url, null);
    }

    /// Add a name to a database.
    pub fn addDatabaseName(self: *Client, name_or_identity: []const u8, new_name: []const u8, token: []const u8) !Response {
        const url = try std.fmt.allocPrint(self.allocator, "http://{s}/v1/database/{s}/names", .{ self.config.host, name_or_identity });
        defer self.allocator.free(url);
        const auth = try buildAuthHeader(self.allocator, token);
        defer self.allocator.free(auth);
        return self.transport.post(self.allocator, url, new_name, auth);
    }

    // ============================================================
    // Logs and public key endpoints
    // ============================================================

    /// Fetch database logs. Returns log text (caller owns response).
    pub fn getLogs(self: *Client, database: []const u8, token: []const u8, num_lines: ?u32) !Response {
        const url = if (num_lines) |n|
            try std.fmt.allocPrint(self.allocator, "http://{s}/v1/database/{s}/logs?num_lines={d}", .{ self.config.host, database, n })
        else
            try std.fmt.allocPrint(self.allocator, "http://{s}/v1/database/{s}/logs", .{ self.config.host, database });
        defer self.allocator.free(url);
        const auth = try buildAuthHeader(self.allocator, token);
        defer self.allocator.free(auth);
        return self.transport.get(self.allocator, url, auth);
    }

    /// Get the server's public key (PEM format) for token verification.
    pub fn getPublicKey(self: *Client) !Response {
        const url = try std.fmt.allocPrint(self.allocator, "http://{s}/v1/identity/public-key", .{self.config.host});
        defer self.allocator.free(url);
        return self.transport.get(self.allocator, url, null);
    }

    /// List databases owned by an identity.
    pub fn getDatabases(self: *Client, identity_hex: []const u8) !Response {
        const url = try std.fmt.allocPrint(self.allocator, "http://{s}/v1/identity/{s}/databases", .{ self.config.host, identity_hex });
        defer self.allocator.free(url);
        return self.transport.get(self.allocator, url, null);
    }

    /// Get a short-lived WebSocket token.
    pub fn getWebSocketToken(self: *Client, token: []const u8) !Response {
        const url = try std.fmt.allocPrint(self.allocator, "http://{s}/v1/identity/websocket-token", .{self.config.host});
        defer self.allocator.free(url);
        const auth = try buildAuthHeader(self.allocator, token);
        defer self.allocator.free(auth);
        return self.transport.post(self.allocator, url, null, auth);
    }
};

// ============================================================
// Credential Persistence
// ============================================================

pub const Credentials = struct {
    identity: []const u8,
    token: []const u8,
};

/// Save credentials to a file. Format: identity\ntoken
pub fn saveCredentials(allocator: std.mem.Allocator, dir_path: []const u8, database: []const u8, identity: []const u8, token: []const u8) !void {
    // Ensure directory exists
    std.fs.cwd().makePath(dir_path) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}.creds", .{ dir_path, database });
    defer allocator.free(file_path);

    const file = try std.fs.cwd().createFile(file_path, .{});
    defer file.close();

    try file.writeAll(identity);
    try file.writeAll("\n");
    try file.writeAll(token);
}

/// Load credentials from a file. Caller owns returned strings.
pub fn loadCredentials(allocator: std.mem.Allocator, dir_path: []const u8, database: []const u8) !?Credentials {
    const file_path = try std.fmt.allocPrint(allocator, "{s}/{s}.creds", .{ dir_path, database });
    defer allocator.free(file_path);

    const file = std.fs.cwd().openFile(file_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close();

    const content = file.readToEndAlloc(allocator, 1024 * 1024) catch return null;
    defer allocator.free(content);

    // Split on first newline
    if (std.mem.indexOfScalar(u8, content, '\n')) |nl| {
        const identity = try allocator.dupe(u8, content[0..nl]);
        errdefer allocator.free(identity);
        const token = try allocator.dupe(u8, std.mem.trimRight(u8, content[nl + 1 ..], "\n\r"));
        return .{ .identity = identity, .token = token };
    }
    return null;
}

/// Free credentials loaded by loadCredentials.
pub fn freeCredentials(allocator: std.mem.Allocator, creds: Credentials) void {
    allocator.free(creds.identity);
    allocator.free(creds.token);
}

/// Default credentials directory (relative to home).
pub const DEFAULT_CREDS_DIR = ".spacetimedb_client_credentials";

/// Get the default credentials directory path. Caller owns returned string.
pub fn defaultCredsDir(allocator: std.mem.Allocator) ![]const u8 {
    const home = std.process.getEnvVarOwned(allocator, "HOME") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => return allocator.dupe(u8, DEFAULT_CREDS_DIR),
        else => return err,
    };
    defer allocator.free(home);
    return std.fmt.allocPrint(allocator, "{s}/{s}", .{ home, DEFAULT_CREDS_DIR });
}

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

test "buildVerifyIdentityUrl" {
    const allocator = std.testing.allocator;
    const url = try buildVerifyIdentityUrl(allocator, "localhost:3000", "abc123");
    defer allocator.free(url);
    try std.testing.expectEqualStrings(
        "http://localhost:3000/v1/identity/abc123/verify",
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

test "Client verifyIdentity success" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "ok",
    };

    var client = Client.init(allocator, .{
        .host = "localhost:3000",
        .database = "test",
    }, mock.transport());

    const result = try client.verifyIdentity("abc123hex", "my-jwt");
    try std.testing.expect(result);
}

test "Client verifyIdentity unauthorized" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 401,
        .response_body = "unauthorized",
    };

    var client = Client.init(allocator, .{
        .host = "localhost:3000",
        .database = "test",
    }, mock.transport());

    const result = try client.verifyIdentity("abc123hex", "bad-token");
    try std.testing.expect(!result);
}

test "saveCredentials and loadCredentials round-trip" {
    const allocator = std.testing.allocator;
    const tmp_dir = "/tmp/spacetimedb-zig-test-creds";

    // Clean up before test
    std.fs.cwd().deleteTree(tmp_dir) catch {};

    try saveCredentials(allocator, tmp_dir, "my_db", "identity-hex-abc", "jwt-token-xyz");

    const creds = (try loadCredentials(allocator, tmp_dir, "my_db")).?;
    defer freeCredentials(allocator, creds);

    try std.testing.expectEqualStrings("identity-hex-abc", creds.identity);
    try std.testing.expectEqualStrings("jwt-token-xyz", creds.token);

    // Clean up
    std.fs.cwd().deleteTree(tmp_dir) catch {};
}

test "loadCredentials returns null for missing file" {
    const allocator = std.testing.allocator;
    const result = try loadCredentials(allocator, "/tmp/nonexistent-dir-12345", "nope");
    try std.testing.expect(result == null);
}

test "defaultCredsDir" {
    const allocator = std.testing.allocator;
    const dir = try defaultCredsDir(allocator);
    defer allocator.free(dir);
    // Should contain the default dir name
    try std.testing.expect(std.mem.endsWith(u8, dir, DEFAULT_CREDS_DIR));
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

test "Client getDatabase" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "{\"identity\":\"abc\"}",
    };
    var client = Client.init(allocator, .{ .host = "localhost:3000", .database = "test" }, mock.transport());
    const resp = try client.getDatabase("my_db");
    defer resp.deinit();
    try std.testing.expect(resp.isSuccess());
    try std.testing.expectEqualStrings("{\"identity\":\"abc\"}", resp.body);
}

test "Client getDatabaseIdentity" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "deadbeef",
    };
    var client = Client.init(allocator, .{ .host = "localhost:3000", .database = "test" }, mock.transport());
    const resp = try client.getDatabaseIdentity("my_db");
    defer resp.deinit();
    try std.testing.expect(resp.isSuccess());
}

test "Client publishDatabase" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "ok",
    };
    var client = Client.init(allocator, .{ .host = "localhost:3000", .database = "test" }, mock.transport());
    const resp = try client.publishDatabase("my_db", "fake-wasm", "my-token");
    defer resp.deinit();
    try std.testing.expect(resp.isSuccess());
}

test "Client getDatabaseNames" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "[\"name1\",\"name2\"]",
    };
    var client = Client.init(allocator, .{ .host = "localhost:3000", .database = "test" }, mock.transport());
    const resp = try client.getDatabaseNames("my_db");
    defer resp.deinit();
    try std.testing.expectEqualStrings("[\"name1\",\"name2\"]", resp.body);
}

test "Client addDatabaseName" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "ok",
    };
    var client = Client.init(allocator, .{ .host = "localhost:3000", .database = "test" }, mock.transport());
    const resp = try client.addDatabaseName("my_db", "new_name", "my-token");
    defer resp.deinit();
    try std.testing.expect(resp.isSuccess());
}

test "Client getLogs" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "log line 1\nlog line 2",
    };
    var client = Client.init(allocator, .{ .host = "localhost:3000", .database = "test" }, mock.transport());
    const resp = try client.getLogs("my_db", "my-token", 100);
    defer resp.deinit();
    try std.testing.expectEqualStrings("log line 1\nlog line 2", resp.body);
}

test "Client getLogs without num_lines" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "all logs",
    };
    var client = Client.init(allocator, .{ .host = "localhost:3000", .database = "test" }, mock.transport());
    const resp = try client.getLogs("my_db", "my-token", null);
    defer resp.deinit();
    try std.testing.expect(resp.isSuccess());
}

test "Client getPublicKey" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "-----BEGIN PUBLIC KEY-----\nMIIB...\n-----END PUBLIC KEY-----",
    };
    var client = Client.init(allocator, .{ .host = "localhost:3000", .database = "test" }, mock.transport());
    const resp = try client.getPublicKey();
    defer resp.deinit();
    try std.testing.expect(std.mem.startsWith(u8, resp.body, "-----BEGIN PUBLIC KEY-----"));
}

test "Client getDatabases" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "[{\"name\":\"db1\"}]",
    };
    var client = Client.init(allocator, .{ .host = "localhost:3000", .database = "test" }, mock.transport());
    const resp = try client.getDatabases("abc123hex");
    defer resp.deinit();
    try std.testing.expect(resp.isSuccess());
}

test "Client getWebSocketToken" {
    const allocator = std.testing.allocator;
    var mock = MockHttpTransport{
        .response_status = 200,
        .response_body = "{\"token\":\"ws-token-xyz\"}",
    };
    var client = Client.init(allocator, .{ .host = "localhost:3000", .database = "test" }, mock.transport());
    const resp = try client.getWebSocketToken("my-jwt");
    defer resp.deinit();
    try std.testing.expectEqualStrings("{\"token\":\"ws-token-xyz\"}", resp.body);
}
