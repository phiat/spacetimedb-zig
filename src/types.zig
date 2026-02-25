// SpacetimeDB Algebraic Type System
//
// Defines the type algebra used throughout SpacetimeDB: primitives,
// containers (array, option), and composites (product, sum).
// Maps directly to BSATN wire format types.

const std = @import("std");

/// A named field within a product type or variant within a sum type.
pub const Column = struct {
    name: ?[]const u8,
    type: AlgebraicType,
};

/// The complete SpacetimeDB algebraic type system.
/// Every value in BSATN is typed by one of these variants.
pub const AlgebraicType = union(enum) {
    // Primitive types
    bool,
    u8,
    i8,
    u16,
    i16,
    u32,
    i32,
    u64,
    i64,
    u128,
    i128,
    u256,
    i256,
    f32,
    f64,
    string,
    bytes,

    // Container types
    array: *const AlgebraicType,
    option: *const AlgebraicType,

    // Composite types
    product: []const Column,
    sum: []const Column,

    // Type indirection into typespace (resolved at schema load time)
    ref: u32,

    /// Returns the fixed byte size of a primitive type, or null for variable-length types.
    pub fn fixedSize(self: AlgebraicType) ?usize {
        return switch (self) {
            .bool, .u8, .i8 => 1,
            .u16, .i16 => 2,
            .u32, .i32, .f32 => 4,
            .u64, .i64, .f64 => 8,
            .u128, .i128 => 16,
            .u256, .i256 => 32,
            .string, .bytes, .array, .option, .product, .sum, .ref => null,
        };
    }

    /// Returns true if this is a primitive (non-container, non-composite) type.
    pub fn isPrimitive(self: AlgebraicType) bool {
        return switch (self) {
            .bool, .u8, .i8, .u16, .i16, .u32, .i32, .u64, .i64,
            .u128, .i128, .u256, .i256, .f32, .f64, .string, .bytes,
            => true,
            .array, .option, .product, .sum, .ref => false,
        };
    }
};

/// A decoded BSATN value with its runtime representation.
/// Used when decoding rows without compile-time type knowledge.
pub const AlgebraicValue = union(enum) {
    bool: bool,
    u8: u8,
    i8: i8,
    u16: u16,
    i16: i16,
    u32: u32,
    i32: i32,
    u64: u64,
    i64: i64,
    u128: u128,
    i128: i128,
    u256: [32]u8,
    i256: [32]u8,
    f32: f32,
    f64: f64,
    string: []const u8,
    bytes: []const u8,
    array: []const AlgebraicValue,
    /// none = null, some = pointer to inner value
    option: ?*const AlgebraicValue,
    /// Named fields
    product: []const FieldValue,
    /// Tag index + payload
    sum: SumValue,

    pub const FieldValue = struct {
        name: ?[]const u8,
        value: AlgebraicValue,
    };

    pub const SumValue = struct {
        tag: u8,
        value: *const AlgebraicValue,
    };

    /// Free all heap-allocated memory in this value tree.
    pub fn deinit(self: *const AlgebraicValue, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |s| allocator.free(s),
            .bytes => |b| allocator.free(b),
            .array => |items| {
                for (items) |*item| item.deinit(allocator);
                allocator.free(items);
            },
            .option => |opt| {
                if (opt) |val| {
                    val.deinit(allocator);
                    allocator.destroy(val);
                }
            },
            .product => |fields| {
                for (fields) |*field| field.value.deinit(allocator);
                allocator.free(fields);
            },
            .sum => |s| {
                s.value.deinit(allocator);
                allocator.destroy(s.value);
            },
            // Primitives: nothing to free
            .bool, .u8, .i8, .u16, .i16, .u32, .i32, .u64, .i64,
            .u128, .i128, .u256, .i256, .f32, .f64,
            => {},
        }
    }
};

/// Identity: 256-bit public identifier for a SpacetimeDB client.
pub const Identity = [32]u8;

/// ConnectionId: 128-bit opaque connection discriminator.
pub const ConnectionId = [16]u8;

// -- Tests --

test "AlgebraicType.fixedSize" {
    const bool_t: AlgebraicType = .bool;
    const u32_t: AlgebraicType = .u32;
    const f64_t: AlgebraicType = .f64;
    const u256_t: AlgebraicType = .u256;
    const string_t: AlgebraicType = .string;
    try std.testing.expectEqual(@as(?usize, 1), bool_t.fixedSize());
    try std.testing.expectEqual(@as(?usize, 4), u32_t.fixedSize());
    try std.testing.expectEqual(@as(?usize, 8), f64_t.fixedSize());
    try std.testing.expectEqual(@as(?usize, 32), u256_t.fixedSize());
    try std.testing.expectEqual(@as(?usize, null), string_t.fixedSize());
}

test "AlgebraicType.isPrimitive" {
    const u64_t: AlgebraicType = .u64;
    const string_t: AlgebraicType = .string;
    const ref_t: AlgebraicType = .{ .ref = 0 };
    try std.testing.expect(u64_t.isPrimitive());
    try std.testing.expect(string_t.isPrimitive());
    try std.testing.expect(!ref_t.isPrimitive());
}
