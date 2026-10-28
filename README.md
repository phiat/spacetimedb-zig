# spacetimedb-zig

[![Zig](https://img.shields.io/badge/zig-0.17.0-orange)](https://ziglang.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![zigcheck](https://img.shields.io/badge/tested_with-zigcheck-blue)](https://github.com/phiat/zigcheck)

[SpacetimeDB](https://spacetimedb.com) client library for Zig 0.17.0. It speaks the v2 BSATN WebSocket protocol and gives you real-time subscriptions, reducer calls, a local cache of subscribed tables, an HTTP client, and code generation. Tested against SpacetimeDB 2.13.0.

## Install

```zig
// build.zig.zon
.spacetimedb_zig = .{
    .url = "https://github.com/phiat/spacetimedb-zig/archive/refs/tags/v0.2.0.tar.gz",
    // run `zig build` once; the compiler prints the .hash to add
},
```

```zig
// build.zig
const spacetimedb = b.dependency("spacetimedb_zig", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("spacetimedb", spacetimedb.module("spacetimedb"));
```

It depends on [websocket.zig](https://github.com/karlseguin/websocket.zig). Gzip uses `std.compress.flate`; brotli is optional (`-Denable-brotli=true`, needs `libbrotlidec`). [zigcheck](https://github.com/phiat/zigcheck) is used for tests only.

## Quick start

```zig
const stdb = @import("spacetimedb");

const handler = stdb.client.EventHandler{
    .ptr = @ptrCast(my_state),
    .vtable = &.{
        .onConnect = myOnConnect,
        .onInsert = myOnInsert,
        .onDelete = myOnDelete,
    },
};

var client = stdb.client.SpacetimeClient.init(allocator, .{
    .host = "localhost:3000",
    .database = "my_db",
    .token = "my-jwt-token", // optional
    .subscriptions = &.{"SELECT * FROM users"},
}, handler);
defer client.deinit();

// Network access goes through std.Io, e.g. `init.io` from `pub fn main(init: std.process.Init)`.
try client.connectReal(io);

// Process incoming frames: poll from your own loop...
while (running) _ = try client.frameTick();
// ...or run a background receive thread.
const thread = try client.runThreaded();

const qs_id = try client.subscribe(&.{"SELECT * FROM users"});
const req_id = try client.callReducerRaw("create_user", bsatn_args);
try client.unsubscribe(qs_id);

// Local cache
const n = client.count("users");
const rows = try client.getAll("users"); // untyped
const users = try client.getTyped(User, "users"); // typed
defer allocator.free(users);
```

`client.connect(transport)` takes any `websocket.Transport` instead, which is how the tests use a mock.

### Callbacks

All fields of `EventHandler.VTable` are optional.

| Callback | When it fires |
|----------|---------------|
| `onConnect(identity, conn_id, token)` | Connection established |
| `onDisconnect(reason)` | Disconnected |
| `onSubscribeApplied(table_name, row_count)` | Subscription data arrives |
| `onUnsubscribeApplied(query_set_id, rows)` | Unsubscription confirmed |
| `onInsert` / `onDelete(table_name, row)` | Row inserted / deleted |
| `onUpdate(table_name, old_row, new_row)` | Same primary key deleted and inserted |
| `onTransaction(changes)` | Whole transaction; return `true` to suppress per-row callbacks |
| `onReducerResult(request_id, result)` | Reducer finished |
| `onQueryResult(request_id, result)` | One-off SQL query finished |
| `onProcedureResult(request_id, status)` | Procedure finished |
| `onError(message)` | Protocol or transport error |

## Typed rows

`table` decodes BSATN straight into your structs at comptime:

```zig
const User = struct { id: u64, name: []const u8, email: ?[]const u8 };

var user = try stdb.table.decodeRow(User, allocator, bsatn_bytes);
defer stdb.table.freeTypedRow(User, allocator, &user);

const bytes = try stdb.table.encodeRow(User, allocator, user);
defer allocator.free(bytes);
```

## Code generation

```bash
zig build codegen -- --host localhost:3000 --database mydb --output src/generated.zig
cat schema.json | zig build codegen -- --stdin --output src/generated.zig
```

The output has one `pub const TableName = struct { ... }` per table (with `decode`, `encode`, `free`) and a `Reducers` struct with typed call functions.

## HTTP client

For identity management, database admin, SQL and reducer calls without a WebSocket:

```zig
var http_transport = stdb.http_client.StdHttpTransport{ .io = io };
var http = stdb.http_client.Client.init(allocator, .{
    .host = "localhost:3000",
    .database = "my_db",
    .token = "my-jwt",
}, http_transport.transport());

const identity_json = try http.createIdentity();
const sql = try http.executeSql("SELECT * FROM users");
const resp = try http.callReducer("create_user", args_json);
const info = try http.getDatabase("my_db");
```

Credential helpers take the same `io`: `saveCredentials(io, ...)`, `loadCredentials(io, ...)`; `defaultCredsDir(allocator, environ)` takes a `std.process.Environ` (`init.minimal.environ`).

## Modules

| Module | Description |
|--------|-------------|
| `client.zig` | `SpacetimeClient`: callbacks, subscriptions, reducers, cache queries |
| `websocket.zig` | Connection state machine and WebSocket transport |
| `http_client.zig` | HTTP client (`/v1` API) and credential storage |
| `client_cache.zig` | In-memory mirror of subscribed tables |
| `table.zig`, `codegen.zig` | Typed BSATN access; code generation (CLI in `codegen_cli.zig`) |
| `protocol.zig`, `schema.zig` | v2 message codec; schema fetch and parse |
| `row_decoder.zig`, `value_encoder.zig` | Schema-driven row decoding and value encoding |
| `bsatn.zig`, `types.zig` | BSATN codec (little-endian; `u32` length prefixes) and algebraic types |

## Development

```bash
zig build test              # unit tests, no server needed
zig build prop-test         # property tests (zigcheck)
zig build integration-test  # needs SpacetimeDB with the test module published
zig build check             # type-check only
```

Integration tests expect a server at `localhost:3000` with the `testmodule` database published; set `SPACETIMEDB_HOST=host:port` to use another. Add `-Denable-brotli=true` to build or test with brotli. [`just`](https://github.com/casey/just) recipes for these are in the `justfile`.

## License

MIT
