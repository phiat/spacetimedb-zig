// SpacetimeDB Codegen CLI
//
// Fetches a schema from a running SpacetimeDB instance and generates
// typed Zig source code.
//
// Usage:
//   zig build codegen -- --host localhost:3000 --database mydb --output src/generated.zig

const std = @import("std");
const root = @import("spacetimedb");
const codegen = root.codegen;
const schema_mod = root.schema;
const http_client = root.http_client;

const stdout_file = std.fs.File{ .handle = std.posix.STDOUT_FILENO };
const stderr_file = std.fs.File{ .handle = std.posix.STDERR_FILENO };
const stdin_file = std.fs.File{ .handle = std.posix.STDIN_FILENO };

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var host: []const u8 = "localhost:3000";
    var database: ?[]const u8 = null;
    var output: []const u8 = "src/generated.zig";
    var from_stdin = false;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--host") and i + 1 < args.len) {
            i += 1;
            host = args[i];
        } else if (std.mem.eql(u8, args[i], "--database") and i + 1 < args.len) {
            i += 1;
            database = args[i];
        } else if (std.mem.eql(u8, args[i], "--output") and i + 1 < args.len) {
            i += 1;
            output = args[i];
        } else if (std.mem.eql(u8, args[i], "--stdin")) {
            from_stdin = true;
        } else if (std.mem.eql(u8, args[i], "--help")) {
            printUsage();
            return;
        }
    }

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_alloc = arena.allocator();

    var s: schema_mod.Schema = undefined;

    if (from_stdin) {
        // Read JSON schema from stdin
        const json = try stdin_file.readToEndAlloc(arena_alloc, 10 * 1024 * 1024);
        s = try schema_mod.parse(arena_alloc, json);
    } else {
        // Fetch from HTTP
        const db = database orelse {
            std.debug.print("Error: --database is required (or use --stdin)\n", .{});
            printUsage();
            std.process.exit(1);
        };

        var http_transport = http_client.StdHttpTransport{};
        var client = http_client.Client.init(arena_alloc, .{
            .host = host,
            .database = db,
        }, http_transport.transport());
        s = try client.fetchSchema();
    }

    const source = try codegen.generate(arena_alloc, s);

    // Write output
    if (std.mem.eql(u8, output, "-")) {
        try stdout_file.writeAll(source);
    } else {
        // Ensure parent directory exists
        if (std.fs.path.dirname(output)) |dir| {
            std.fs.cwd().makePath(dir) catch {};
        }
        const file = try std.fs.cwd().createFile(output, .{});
        defer file.close();
        try file.writeAll(source);
        std.debug.print("Generated {s} ({d} tables, {d} reducers)\n", .{
            output,
            s.tables.len,
            s.reducers.len,
        });
    }
}

fn printUsage() void {
    stderr_file.writeAll(
        \\Usage: codegen [options]
        \\
        \\Options:
        \\  --host <host:port>    SpacetimeDB host (default: localhost:3000)
        \\  --database <name>     Database name (required unless --stdin)
        \\  --output <path>       Output file path (default: src/generated.zig)
        \\  --stdin               Read JSON schema from stdin instead of fetching
        \\  --help                Show this help
        \\
        \\Examples:
        \\  codegen --host localhost:3000 --database mydb --output src/db.zig
        \\  cat schema.json | codegen --stdin --output src/db.zig
        \\
    ) catch {};
}
