# spacetimedb-zig

A Zig client SDK for [SpacetimeDB](https://spacetimedb.com) — the database that runs WebAssembly modules and pushes real-time updates to clients.

Ported from the [Elixir SpacetimeDB SDK](https://github.com/clockworklabs/spacetimedb), with API design influenced by the [Rust SDK](https://spacetimedb.com/docs/clients/rust).

## Status

**All core layers complete.** Ready for integration testing against a live SpacetimeDB instance.

- [x] Algebraic type system (`types.zig`)
- [x] BSATN encoder/decoder (`bsatn.zig`)
- [x] Protocol messages — v2 binary (`protocol.zig`)
- [x] Schema fetcher and parser (`schema.zig`)
- [x] Row decoder (`row_decoder.zig`)
- [x] Value encoder — schema-aware (`value_encoder.zig`)
- [x] Client cache — in-memory table store (`client_cache.zig`)
- [x] WebSocket connection manager (`websocket.zig`)
- [x] HTTP REST client (`http_client.zig`)
- [x] High-level client API (`client.zig`)
- [x] Comptime typed table access (`table.zig`)
- [x] Code generation from schema (`codegen.zig`)
- [x] Optional brotli decompression

## Requirements

- **Zig 0.15.2** or later
- **libbrotlidec** (optional — for brotli-compressed server messages)

## Building

```bash
zig build          # build the library
zig build test     # run all tests
```

With brotli decompression support:

```bash
zig build -Denable-brotli=true       # build with brotli
zig build test -Denable-brotli=true  # test with brotli
```

Or using [just](https://github.com/casey/just):

```bash
just build         # build
just test          # run tests
just fmt           # format source
just check         # type-check only (fast)
```

## Quick Start

```zig
const stdb = @import("spacetimedb_zig");

// 1. Create a client
var client = stdb.client.SpacetimeClient.init(allocator, .{
    .host = "localhost:3000",
    .database = "my_db",
    .token = "my-jwt-token",
    .subscriptions = &.{"SELECT * FROM users"},
}, my_event_handler);
defer client.deinit();

// 2. Connect via WebSocket transport
client.connect(transport);

// 3. Subscribe to tables
const qs_id = try client.subscribe(&.{"SELECT * FROM users"});

// 4. Call a reducer
const req_id = try client.callReducerRaw("create_user", bsatn_args);

// 5. Query the local cache (untyped)
const row_count = client.count("users");
const rows = try client.getAll("users");

// 6. Query the local cache (typed via comptime)
const users = try client.getTyped(User, "users");
defer allocator.free(users);
```

## Typed Table Access

The `table` module provides comptime-powered direct BSATN-to-struct decoding, bypassing the intermediate `AlgebraicValue` layer:

```zig
const stdb = @import("spacetimedb_zig");

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

## Code Generation

Generate typed Zig source from a running SpacetimeDB instance:

```bash
# From a live server
zig build codegen -- --host localhost:3000 --database mydb --output src/generated.zig

# From a JSON schema file
cat schema.json | zig build codegen -- --stdin --output src/generated.zig
```

The generated code includes struct definitions with `decode`, `encode`, and `free` methods, plus typed reducer call functions.

## HTTP Client

The HTTP client covers all SpacetimeDB REST endpoints:

```zig
const stdb = @import("spacetimedb_zig");

var http_transport = stdb.http_client.StdHttpTransport{};
var client = stdb.http_client.Client.init(allocator, .{
    .host = "localhost:3000",
    .database = "my_db",
    .token = "my-jwt",
}, http_transport.transport());

// Core operations
const schema = try client.fetchSchema();
const identity_json = try client.createIdentity();
const ok = try client.ping();

// Reducer and SQL
const resp = try client.callReducer("say_hello", args_json);
const sql_resp = try client.executeSql("SELECT * FROM users");

// Database management
const db_info = try client.getDatabase("my_db");
const names = try client.getDatabaseNames("my_db");

// Logs and identity
const logs = try client.getLogs("my_db", token, 100);
const pub_key = try client.getPublicKey();
const ws_token = try client.getWebSocketToken(token);
```

## Architecture

```
src/
  root.zig           Library root — re-exports all public modules
  types.zig          Algebraic type system (AlgebraicType, AlgebraicValue)
  bsatn.zig          BSATN binary codec (Encoder, Decoder)
  protocol.zig       v2 binary protocol messages (Client + Server)
  schema.zig         JSON schema parser with ref resolution
  row_decoder.zig    BSATN row data → decoded Row structs
  value_encoder.zig  AlgebraicValue → BSATN binary (schema-aware)
  client_cache.zig   In-memory table store with PK-based identity
  websocket.zig      WebSocket connection state machine
  http_client.zig    HTTP REST client for schema/identity/reducers
  table.zig          Comptime typed table access (BSATN ↔ structs)
  codegen.zig        Source code generation from SpacetimeDB schema
  codegen_cli.zig    CLI entry point for codegen
  client.zig         High-level client API tying everything together
```

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

### BSATN Format

BSATN (Binary SpacetimeDB Algebraic Type Notation) is the wire format for all protocol messages and row data:

- All integers: little-endian
- Strings/bytes: `u32` length prefix + data
- Arrays: `u32` count + concatenated elements
- Options: `u8` tag (`0` = Some + payload, `1` = None)
- Products (structs): concatenated fields, no separators
- Sums (enums): `u8` tag + variant payload

### WebSocket Protocol

- URL: `ws://{host}/v1/database/{name}/subscribe?compression=None`
- Subprotocol: `v2.bsatn.spacetimedb`
- Client messages: raw BSATN (no compression envelope)
- Server messages: 1-byte compression prefix + BSATN payload
- Compression: None (0), Brotli (1), Gzip (2) — Brotli requires `-Denable-brotli=true`

## License

See [LICENSE](LICENSE).
