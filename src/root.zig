// SpacetimeDB Zig Client SDK
//
// A Zig client for SpacetimeDB, providing connection management,
// BSATN serialization, and real-time table subscriptions.

pub const types = @import("types.zig");
pub const bsatn = @import("bsatn.zig");
pub const protocol = @import("protocol.zig");
pub const schema = @import("schema.zig");

test {
    _ = types;
    _ = bsatn;
    _ = protocol;
    _ = schema;
}
