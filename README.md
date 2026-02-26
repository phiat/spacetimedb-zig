# spacetimedb-zig

[![Zig](https://img.shields.io/badge/zig-0.15.2-orange)](https://ziglang.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

SpacetimeDB client library for Zig.

Connects to [SpacetimeDB](https://spacetimedb.com) via the v2 BSATN binary WebSocket protocol, providing real-time subscriptions, reducer calls, a local in-memory client cache, an HTTP REST client, and code generation.

## Features

| Module | Description |
|--------|-------------|
| `bsatn.zig` | Binary codec — encoder, decoder |
| `protocol.zig` | v2 client/server message encoding and decoding |
| `websocket.zig` | WebSocket connection state machine |
| `schema.zig` | Schema fetcher and parser (tables, reducers, typespace) |
| `client_cache.zig` | In-memory local mirror of subscribed tables |
| `client.zig` | High-level client with callbacks and event dispatch |
| `http_client.zig` | HTTP REST client for all v1 API endpoints |
| `table.zig` | Comptime typed table access (BSATN ↔ Zig structs) |
| `codegen.zig` | Code generation from schema |
| `codegen_cli.zig` | CLI entry point (`zig build codegen`) |

## Dependencies

| Dependency | Type | Purpose |
|------------|------|---------|
| [websocket.zig](https://github.com/karlseguin/websocket.zig) | Zig package (required) | WebSocket client transport |
| [libbrotlidec](https://github.com/google/brotli) | System library (optional) | Brotli decompression, enabled with `-Denable-brotli=true` |

Everything else uses the Zig standard library: `std.http.Client` for HTTP REST, `std.compress.flate` for gzip, `std.json` for schema parsing.

## Installation

Add to your `build.zig.zon` dependencies:

```zig
.spacetimedb_zig = .{
    .url = "https://github.com/phiat/spacetimedb-zig/archive/refs/heads/main.tar.gz",
    // Run `zig build` once; the compiler will provide the correct .hash value
},
```

Then in `build.zig`:

```zig
const spacetimedb = b.dependency("spacetimedb_zig", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("spacetimedb", spacetimedb.module("spacetimedb"));
```

[GitHub](https://github.com/phiat/spacetimedb-zig)

## Quick Start

### High-Level Client (recommended)

Create a client with event callbacks:

```zig
const stdb = @import("spacetimedb");

const handler = stdb.client.EventHandler{
    .ptr = @ptrCast(my_state),
    .vtable = &.{
        .onConnect = myOnConnect,
        .onInsert = myOnInsert,
        .onDelete = myOnDelete,
        .onUpdate = myOnUpdate,
        .onTransaction = myOnTransaction,
        .onReducerResult = myOnReducerResult,
    },
};

var client = stdb.client.SpacetimeClient.init(allocator, .{
    .host = "localhost:3000",
    .database = "my_db",
    .token = "my-jwt-token",
    .subscriptions = &.{"SELECT * FROM users"},
}, handler);
defer client.deinit();
```

Connect and interact:

```zig
// Connect via WebSocket transport
client.connect(transport);

// Subscribe to tables
const qs_id = try client.subscribe(&.{"SELECT * FROM users"});

// Call a reducer (BSATN-encoded args)
const req_id = try client.callReducerRaw("create_user", bsatn_args);

// Query the local cache (untyped)
const row_count = client.count("users");
const rows = try client.getAll("users");

// Query the local cache (typed via comptime)
const users = try client.getTyped(User, "users");
defer allocator.free(users);

// Unsubscribe from a query set
try client.unsubscribe(qs_id);
```

### Client Callbacks

All callbacks are optional:

| Callback | When it fires |
|----------|---------------|
| `onConnect(identity, conn_id, token)` | Initial connection established |
| `onSubscribeApplied(table_name, row_count)` | Subscription data arrives |
| `onUnsubscribeApplied(query_set_id, rows)` | Unsubscription confirmed (rows optionally returned) |
| `onInsert(table_name, row)` | Row inserted |
| `onDelete(table_name, row)` | Row deleted |
| `onUpdate(table_name, old_row, new_row)` | Row replaced (same PK deleted + inserted) |
| `onTransaction(changes)` | Full transaction — return `true` to suppress per-row callbacks |
| `onReducerResult(request_id, result)` | Reducer completes |
| `onQueryResult(request_id, result)` | One-off SQL query completes |
| `onProcedureResult(request_id, status)` | Procedure call completes |
| `onError(message)` | Protocol or transport error |
| `onDisconnect(reason)` | Disconnected |

### Typed Table Access

The `table` module provides comptime-powered direct BSATN-to-struct decoding, bypassing the intermediate `AlgebraicValue` layer:

```zig
const stdb = @import("spacetimedb");

const User = struct {
    id: u64,
    name: []const u8,
    email: ?[]const u8,
};

// Decode a single row from BSATN bytes (allocates — caller must free)
var user = try stdb.table.decodeRow(User, allocator, bsatn_bytes);
defer stdb.table.freeTypedRow(User, allocator, &user);

// Encode back to BSATN
const bytes = try stdb.table.encodeRow(User, allocator, user);
defer allocator.free(bytes);
```

### Code Generation

Generate typed structs, reducer functions, and decode/encode methods from a live database:

```bash
# From a live server
zig build codegen -- --host localhost:3000 --database mydb --output src/generated.zig

# From a JSON schema file
cat schema.json | zig build codegen -- --stdin --output src/generated.zig
```

Produces:
- `pub const TableName = struct { ... }` with `decode`, `encode`, `free` methods
- `pub const Reducers = struct { ... }` with typed call functions

### HTTP REST Client

For operations that don't need a persistent WebSocket (identity management, database admin, ad-hoc SQL):

```zig
const stdb = @import("spacetimedb");

var http_transport = stdb.http_client.StdHttpTransport{};
var client = stdb.http_client.Client.init(allocator, .{
    .host = "localhost:3000",
    .database = "my_db",
    .token = "my-jwt",
}, http_transport.transport());

// Identity
const identity_json = try client.createIdentity();

// SQL query
const sql_resp = try client.executeSql("SELECT * FROM users");

// Call a reducer over HTTP
const resp = try client.callReducer("create_user", args_json);

// Database management
const db_info = try client.getDatabase("my_db");
const names = try client.getDatabaseNames("my_db");
```

## Architecture

### BSATN Codec

Binary SpacetimeDB Algebraic Type Notation — a compact little-endian binary format:

- Integers: `u8`..`u256`, `i8`..`i256` (little-endian)
- Floats: `f32`, `f64` (IEEE 754, little-endian)
- Strings/Bytes: `u32` length prefix + raw data
- Arrays: `u32` count prefix + concatenated elements
- Products (structs): fields concatenated in order
- Sums (enums): `u8` variant tag + payload

### Protocol (v2)

Client sends: `Subscribe`, `Unsubscribe`, `OneOffQuery`, `CallReducer`, `CallProcedure`.

Server sends (with 1-byte compression envelope): `InitialConnection`, `SubscribeApplied`, `UnsubscribeApplied`, `SubscriptionError`, `TransactionUpdate`, `OneOffQueryResult`, `ReducerResult`, `ProcedureResult`.

### Layer Diagram

```
┌─────────────────────────────────────────┐
│  client.zig — SpacetimeClient           │
│  (connect, subscribe, call, query)      │
├────────────┬────────────┬───────────────┤
│ websocket  │ client     │ http_client   │
│ .zig       │ _cache.zig │ .zig          │
├────────────┴──────┬─────┴───────────────┤
│ table.zig         │ codegen.zig         │
├───────────────────┴─────────────────────┤
│ protocol.zig  │ schema.zig              │
├───────────────┴─────────────────────────┤
│ row_decoder.zig │ value_encoder.zig     │
├─────────────────┴───────────────────────┤
│         bsatn.zig  │  types.zig         │
└─────────────────────────────────────────┘
```

## Development

```bash
zig build              # Build the library
zig build test         # Unit tests (no server needed)
zig build integration-test  # All tests (requires SpacetimeDB on :3000)
zig build check        # Type-check only (fast)
```

With optional brotli decompression:

```bash
zig build -Denable-brotli=true        # Build with brotli
zig build test -Denable-brotli=true   # Test with brotli
```

Or using [just](https://github.com/casey/just):

```bash
just build             # Build
just test              # Unit tests
just integration-test  # Integration tests
just check             # Type-check only
just fmt               # Format source
```

See `justfile` for all available commands.

Issue tracking uses [beads](https://github.com/cosmikwolf/beads) — a git-backed tracker with dependency management:

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd list --status=open # All open issues
```

## License

MIT
