// Value Encoder (Schema-Aware)
//
// Encodes Zig values into BSATN binary using algebraic type information
// from the schema. Inverse of row_decoder/bsatn.Decoder.decodeValue.
//
// Used for encoding reducer arguments and any client-to-server data.

const std = @import("std");
const types = @import("types.zig");
const bsatn = @import("bsatn.zig");

const AlgebraicType = types.AlgebraicType;
const AlgebraicValue = types.AlgebraicValue;
const Column = types.Column;
const Encoder = bsatn.Encoder;
const Decoder = bsatn.Decoder;

pub const EncodeError = error{
    TypeMismatch,
    MissingField,
} || std.mem.Allocator.Error;

/// Encode an AlgebraicValue to BSATN binary given its type.
/// Returns owned slice — caller must free.
pub fn encodeValue(allocator: std.mem.Allocator, value: AlgebraicValue, typ: AlgebraicType) EncodeError![]u8 {
    var enc = Encoder.init();
    defer enc.deinit(allocator);
    try encodeInto(&enc, allocator, value, typ);
    return enc.toOwnedSlice(allocator);
}

/// Encode a value into an existing encoder buffer.
fn encodeInto(enc: *Encoder, allocator: std.mem.Allocator, value: AlgebraicValue, typ: AlgebraicType) EncodeError!void {
    switch (typ) {
        .bool => try enc.encodeBool(allocator, value.bool),
        .u8 => try enc.encodeU8(allocator, value.u8),
        .i8 => try enc.encodeI8(allocator, value.i8),
        .u16 => try enc.encodeU16(allocator, value.u16),
        .i16 => try enc.encodeI16(allocator, value.i16),
        .u32 => try enc.encodeU32(allocator, value.u32),
        .i32 => try enc.encodeI32(allocator, value.i32),
        .u64 => try enc.encodeU64(allocator, value.u64),
        .i64 => try enc.encodeI64(allocator, value.i64),
        .u128 => try enc.encodeU128(allocator, value.u128),
        .i128 => try enc.encodeI128(allocator, value.i128),
        .u256 => try enc.encodeU256(allocator, value.u256),
        .i256 => try enc.encodeI256(allocator, value.i256),
        .f32 => try enc.encodeF32(allocator, value.f32),
        .f64 => try enc.encodeF64(allocator, value.f64),
        .string => try enc.encodeString(allocator, value.string),
        .bytes => try enc.encodeBytes(allocator, value.bytes),
        .array => |inner_type| {
            const items = value.array;
            try enc.encodeArrayHeader(allocator, @intCast(items.len));
            for (items) |item| {
                try encodeInto(enc, allocator, item, inner_type.*);
            }
        },
        .option => |inner_type| {
            if (value.option) |val| {
                try enc.encodeU8(allocator, 0); // Some tag
                try encodeInto(enc, allocator, val.*, inner_type.*);
            } else {
                try enc.encodeU8(allocator, 1); // None tag
            }
        },
        .product => |columns| {
            const fields = value.product;
            for (columns) |col| {
                // Find matching field by name
                const field_val = findField(fields, col.name) orelse return EncodeError.MissingField;
                try encodeInto(enc, allocator, field_val, col.type);
            }
        },
        .sum => {
            const s = value.sum;
            try enc.encodeU8(allocator, s.tag);
            // For sum encoding, we'd need the variant's type from the sum definition
            // For now, encode the value using encodeValue which dispatches by runtime tag
            try enc.encodeValue(allocator, s.value.*);
        },
        .ref => unreachable, // Must be resolved before encoding
    }
}

/// Find a field value by name in a field list.
fn findField(fields: []const AlgebraicValue.FieldValue, name: ?[]const u8) ?AlgebraicValue {
    const target = name orelse return null;
    for (fields) |f| {
        if (f.name) |n| {
            if (std.mem.eql(u8, n, target)) return f.value;
        }
    }
    return null;
}

/// Encode reducer arguments as a BSATN product.
/// Takes an array of field values and param definitions (from schema).
/// Returns owned slice.
pub fn encodeReducerArgs(
    allocator: std.mem.Allocator,
    fields: []const AlgebraicValue.FieldValue,
    params: []const Column,
) EncodeError![]u8 {
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    for (params) |param| {
        const val = findField(fields, param.name) orelse return EncodeError.MissingField;
        try encodeInto(&enc, allocator, val, param.type);
    }

    return enc.toOwnedSlice(allocator);
}

// ============================================================
// Tests — mirroring Elixir value_encoder_test.exs
// ============================================================

/// Helper: encode then decode, verify roundtrip.
fn assertRoundtrip(allocator: std.mem.Allocator, value: AlgebraicValue, typ: AlgebraicType) !void {
    const encoded = try encodeValue(allocator, value, typ);
    defer allocator.free(encoded);

    var dec = Decoder.init(encoded);
    const decoded = try dec.decodeValue(allocator, typ);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), dec.bytesRemaining());
    try expectAlgebraicEqual(value, decoded);
}

/// Compare two AlgebraicValues for equality (deep).
fn expectAlgebraicEqual(expected: AlgebraicValue, actual: AlgebraicValue) !void {
    const expected_tag = std.meta.activeTag(expected);
    const actual_tag = std.meta.activeTag(actual);
    try std.testing.expect(expected_tag == actual_tag);

    switch (expected) {
        .bool => try std.testing.expectEqual(expected.bool, actual.bool),
        .u8 => try std.testing.expectEqual(expected.u8, actual.u8),
        .i8 => try std.testing.expectEqual(expected.i8, actual.i8),
        .u16 => try std.testing.expectEqual(expected.u16, actual.u16),
        .i16 => try std.testing.expectEqual(expected.i16, actual.i16),
        .u32 => try std.testing.expectEqual(expected.u32, actual.u32),
        .i32 => try std.testing.expectEqual(expected.i32, actual.i32),
        .u64 => try std.testing.expectEqual(expected.u64, actual.u64),
        .i64 => try std.testing.expectEqual(expected.i64, actual.i64),
        .u128 => try std.testing.expectEqual(expected.u128, actual.u128),
        .i128 => try std.testing.expectEqual(expected.i128, actual.i128),
        .u256 => try std.testing.expectEqualSlices(u8, &expected.u256, &actual.u256),
        .i256 => try std.testing.expectEqualSlices(u8, &expected.i256, &actual.i256),
        .f32 => try std.testing.expectApproxEqAbs(expected.f32, actual.f32, 0.0001),
        .f64 => try std.testing.expectApproxEqAbs(expected.f64, actual.f64, 0.0000001),
        .string => try std.testing.expectEqualStrings(expected.string, actual.string),
        .bytes => try std.testing.expectEqualSlices(u8, expected.bytes, actual.bytes),
        .array => |items| {
            try std.testing.expectEqual(items.len, actual.array.len);
            for (items, actual.array) |e, a| {
                try expectAlgebraicEqual(e, a);
            }
        },
        .option => |opt| {
            if (opt) |e_val| {
                try std.testing.expect(actual.option != null);
                try expectAlgebraicEqual(e_val.*, actual.option.?.*);
            } else {
                try std.testing.expect(actual.option == null);
            }
        },
        .product => |fields| {
            try std.testing.expectEqual(fields.len, actual.product.len);
            for (fields, actual.product) |e, a| {
                try expectAlgebraicEqual(e.value, a.value);
            }
        },
        .sum => |s| {
            try std.testing.expectEqual(s.tag, actual.sum.tag);
            try expectAlgebraicEqual(s.value.*, actual.sum.value.*);
        },
    }
}

test "roundtrip bool" {
    const allocator = std.testing.allocator;
    try assertRoundtrip(allocator, .{ .bool = true }, .bool);
    try assertRoundtrip(allocator, .{ .bool = false }, .bool);
}

test "roundtrip u8" {
    const allocator = std.testing.allocator;
    try assertRoundtrip(allocator, .{ .u8 = 0 }, .u8);
    try assertRoundtrip(allocator, .{ .u8 = 255 }, .u8);
}

test "roundtrip i8" {
    const allocator = std.testing.allocator;
    try assertRoundtrip(allocator, .{ .i8 = -128 }, .i8);
    try assertRoundtrip(allocator, .{ .i8 = 127 }, .i8);
}

test "roundtrip u32" {
    const allocator = std.testing.allocator;
    try assertRoundtrip(allocator, .{ .u32 = 0 }, .u32);
    try assertRoundtrip(allocator, .{ .u32 = 4_294_967_295 }, .u32);
}

test "roundtrip i64" {
    const allocator = std.testing.allocator;
    try assertRoundtrip(allocator, .{ .i64 = -9_223_372_036_854_775_808 }, .i64);
    try assertRoundtrip(allocator, .{ .i64 = 9_223_372_036_854_775_807 }, .i64);
}

test "roundtrip u128" {
    const allocator = std.testing.allocator;
    try assertRoundtrip(allocator, .{ .u128 = 0 }, .u128);
    try assertRoundtrip(allocator, .{ .u128 = 340_282_366_920_938_463_463_374_607_431_768_211_455 }, .u128);
}

test "roundtrip f32" {
    const allocator = std.testing.allocator;
    try assertRoundtrip(allocator, .{ .f32 = 3.14 }, .f32);
    try assertRoundtrip(allocator, .{ .f32 = 0.0 }, .f32);
}

test "roundtrip f64" {
    const allocator = std.testing.allocator;
    try assertRoundtrip(allocator, .{ .f64 = 3.141592653589793 }, .f64);
    try assertRoundtrip(allocator, .{ .f64 = 0.0 }, .f64);
}

test "roundtrip string" {
    const allocator = std.testing.allocator;
    try assertRoundtrip(allocator, .{ .string = "hello" }, .string);
    try assertRoundtrip(allocator, .{ .string = "" }, .string);
    try assertRoundtrip(allocator, .{ .string = "unicode: 日本語" }, .string);
}

test "roundtrip bytes" {
    const allocator = std.testing.allocator;
    try assertRoundtrip(allocator, .{ .bytes = &[_]u8{ 1, 2, 3 } }, .bytes);
    try assertRoundtrip(allocator, .{ .bytes = &[_]u8{} }, .bytes);
}

test "roundtrip array of u32" {
    const allocator = std.testing.allocator;
    const inner: AlgebraicType = .u32;
    const arr_type: AlgebraicType = .{ .array = &inner };

    const items = [_]AlgebraicValue{ .{ .u32 = 1 }, .{ .u32 = 2 }, .{ .u32 = 3 } };
    try assertRoundtrip(allocator, .{ .array = &items }, arr_type);

    // Empty array
    try assertRoundtrip(allocator, .{ .array = &[_]AlgebraicValue{} }, arr_type);
}

test "roundtrip array of strings" {
    const allocator = std.testing.allocator;
    const inner: AlgebraicType = .string;
    const arr_type: AlgebraicType = .{ .array = &inner };

    const items = [_]AlgebraicValue{ .{ .string = "hello" }, .{ .string = "world" } };
    try assertRoundtrip(allocator, .{ .array = &items }, arr_type);
}

test "roundtrip option some" {
    const allocator = std.testing.allocator;
    const inner: AlgebraicType = .u32;
    const opt_type: AlgebraicType = .{ .option = &inner };

    const val: AlgebraicValue = .{ .option = &AlgebraicValue{ .u32 = 42 } };
    try assertRoundtrip(allocator, val, opt_type);
}

test "roundtrip option none" {
    const allocator = std.testing.allocator;
    const inner: AlgebraicType = .u32;
    const opt_type: AlgebraicType = .{ .option = &inner };

    try assertRoundtrip(allocator, .{ .option = null }, opt_type);
}

test "roundtrip product" {
    const allocator = std.testing.allocator;
    const columns = [_]Column{
        .{ .name = "name", .type = .string },
        .{ .name = "age", .type = .u32 },
    };
    const product_type: AlgebraicType = .{ .product = &columns };

    const fields = [_]AlgebraicValue.FieldValue{
        .{ .name = "name", .value = .{ .string = "Alice" } },
        .{ .name = "age", .value = .{ .u32 = 30 } },
    };
    try assertRoundtrip(allocator, .{ .product = &fields }, product_type);
}

test "roundtrip nested product" {
    const allocator = std.testing.allocator;
    const inner_cols = [_]Column{
        .{ .name = "x", .type = .i32 },
        .{ .name = "y", .type = .i32 },
    };
    const columns = [_]Column{
        .{ .name = "label", .type = .string },
        .{ .name = "point", .type = .{ .product = &inner_cols } },
    };
    const product_type: AlgebraicType = .{ .product = &columns };

    const point_fields = [_]AlgebraicValue.FieldValue{
        .{ .name = "x", .value = .{ .i32 = 0 } },
        .{ .name = "y", .value = .{ .i32 = 0 } },
    };
    const fields = [_]AlgebraicValue.FieldValue{
        .{ .name = "label", .value = .{ .string = "origin" } },
        .{ .name = "point", .value = .{ .product = &point_fields } },
    };
    try assertRoundtrip(allocator, .{ .product = &fields }, product_type);
}

test "encodeReducerArgs roundtrip" {
    const allocator = std.testing.allocator;
    const params = [_]Column{
        .{ .name = "name", .type = .string },
        .{ .name = "age", .type = .u32 },
    };

    const fields = [_]AlgebraicValue.FieldValue{
        .{ .name = "name", .value = .{ .string = "Bob" } },
        .{ .name = "age", .value = .{ .u32 = 25 } },
    };

    const encoded = try encodeReducerArgs(allocator, &fields, &params);
    defer allocator.free(encoded);

    // Decode as product to verify
    const product_type: AlgebraicType = .{ .product = &params };
    var dec = Decoder.init(encoded);
    const decoded = try dec.decodeValue(allocator, product_type);
    defer decoded.deinit(allocator);

    try std.testing.expectEqualStrings("Bob", decoded.product[0].value.string);
    try std.testing.expectEqual(@as(u32, 25), decoded.product[1].value.u32);
}

test "encodeReducerArgs missing field returns error" {
    const allocator = std.testing.allocator;
    const params = [_]Column{
        .{ .name = "x", .type = .u32 },
        .{ .name = "y", .type = .u32 },
    };

    const fields = [_]AlgebraicValue.FieldValue{
        .{ .name = "x", .value = .{ .u32 = 1 } },
        // "y" is missing
    };

    const result = encodeReducerArgs(allocator, &fields, &params);
    try std.testing.expectError(EncodeError.MissingField, result);
}
