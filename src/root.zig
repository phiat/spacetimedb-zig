// SpacetimeDB Zig Client SDK
//
// A Zig client for SpacetimeDB, providing connection management,
// BSATN serialization, and real-time table subscriptions.

pub const types = @import("types.zig");
pub const bsatn = @import("bsatn.zig");
pub const protocol = @import("protocol.zig");
pub const schema = @import("schema.zig");
pub const row_decoder = @import("row_decoder.zig");
pub const value_encoder = @import("value_encoder.zig");
pub const client_cache = @import("client_cache.zig");
pub const websocket = @import("websocket.zig");
pub const http_client = @import("http_client.zig");
pub const table = @import("table.zig");
pub const codegen = @import("codegen.zig");
pub const client = @import("client.zig");

test {
    _ = types;
    _ = bsatn;
    _ = protocol;
    _ = schema;
    _ = row_decoder;
    _ = value_encoder;
    _ = client_cache;
    _ = table;
    _ = codegen;
    _ = websocket;
    _ = http_client;
    _ = client;
}
