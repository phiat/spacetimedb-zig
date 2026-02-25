// Typed Table Access (Comptime)
//
// Provides compile-time reflection to decode BSATN row data directly
// into user-defined Zig structs, bypassing the intermediate AlgebraicValue
// representation. This is the pre-codegen typed access layer.
//
// Usage:
//   const Person = struct { id: u64, name: []const u8, age: u32 };
//   const rows = try table.decodeTyped(Person, allocator, bsatn_data, columns);

const std = @import("std");
const bsatn = @import("bsatn.zig");
const types = @import("types.zig");
const protocol = @import("protocol.zig");
const row_decoder = @import("row_decoder.zig");

const Decoder = bsatn.Decoder;
const Encoder = bsatn.Encoder;
const AlgebraicValue = types.AlgebraicValue;
const Column = types.Column;
const Row = row_decoder.Row;
const BsatnRowList = protocol.BsatnRowList;

pub const DecodeError = row_decoder.DecodeError || error{TypeMismatch};

/// Decode a single BSATN row binary directly into a typed struct.
pub fn decodeRow(comptime T: type, allocator: std.mem.Allocator, data: []const u8) DecodeError!T {
    var dec = Decoder.init(data);
    return decodeStruct(T, allocator, &dec);
}

/// Decode a BSATN row list directly into a typed slice.
/// Caller owns the returned slice and must call `freeTypedRows` to clean up.
pub fn decodeRowList(
    comptime T: type,
    allocator: std.mem.Allocator,
    row_list: BsatnRowList,
) DecodeError![]T {
    const row_binaries = try row_decoder.splitRows(allocator, row_list.size_hint, row_list.rows_data);
    defer allocator.free(row_binaries);

    const rows = try allocator.alloc(T, row_binaries.len);
    var decoded_count: usize = 0;
    errdefer {
        for (rows[0..decoded_count]) |*r| freeTypedRow(T, allocator, r);
        allocator.free(rows);
    }
    for (row_binaries, 0..) |row_bin, i| {
        rows[i] = try decodeRow(T, allocator, row_bin);
        decoded_count += 1;
    }
    return rows;
}

/// Convert a dynamic Row into a typed struct.
pub fn fromRow(comptime T: type, row: *const Row) DecodeError!T {
    const info = @typeInfo(T).@"struct";
    var result: T = undefined;

    inline for (info.fields) |field| {
        const av = row.get(field.name) orelse return error.TypeMismatch;
        @field(result, field.name) = try extractField(field.type, av);
    }

    return result;
}

/// Encode a typed struct into BSATN bytes.
pub fn encodeRow(comptime T: type, allocator: std.mem.Allocator, value: T) ![]u8 {
    var enc = Encoder.init();
    defer enc.deinit(allocator);
    encodeStruct(T, allocator, &enc, value) catch |e| return e;
    return enc.toOwnedSlice(allocator);
}

/// Free heap-allocated fields in a typed row (strings, slices).
pub fn freeTypedRow(comptime T: type, allocator: std.mem.Allocator, row: *T) void {
    const info = @typeInfo(T).@"struct";
    inline for (info.fields) |field| {
        freeField(field.type, allocator, &@field(row, field.name));
    }
}

/// Free a slice of typed rows and their heap-allocated fields.
pub fn freeTypedRows(comptime T: type, allocator: std.mem.Allocator, rows: []T) void {
    for (rows) |*r| freeTypedRow(T, allocator, r);
    allocator.free(rows);
}

// ============================================================
// Comptime decoding internals
// ============================================================

fn decodeStruct(comptime T: type, allocator: std.mem.Allocator, dec: *Decoder) DecodeError!T {
    const info = @typeInfo(T).@"struct";
    var result: T = undefined;
    comptime var decoded_idx: usize = 0;

    inline for (info.fields) |field| {
        errdefer {
            // Clean up previously decoded fields on error
            comptime var j: usize = 0;
            inline for (info.fields) |prev_field| {
                if (j >= decoded_idx) break;
                freeField(prev_field.type, allocator, &@field(result, prev_field.name));
                j += 1;
            }
        }
        @field(result, field.name) = try decodeField(field.type, allocator, dec);
        decoded_idx += 1;
    }

    return result;
}

fn decodeField(comptime T: type, allocator: std.mem.Allocator, dec: *Decoder) DecodeError!T {
    // Primitives
    if (T == bool) return dec.decodeBool();
    if (T == u8) return dec.decodeU8();
    if (T == i8) return dec.decodeI8();
    if (T == u16) return dec.decodeU16();
    if (T == i16) return dec.decodeI16();
    if (T == u32) return dec.decodeU32();
    if (T == i32) return dec.decodeI32();
    if (T == u64) return dec.decodeU64();
    if (T == i64) return dec.decodeI64();
    if (T == u128) return dec.decodeU128();
    if (T == i128) return dec.decodeI128();
    if (T == f32) return dec.decodeF32();
    if (T == f64) return dec.decodeF64();

    // Strings — []const u8
    if (T == []const u8) {
        const raw = try dec.decodeString();
        return try allocator.dupe(u8, raw);
    }

    // Fixed byte arrays [N]u8
    if (comptime isFixedByteArray(T)) {
        const N = @typeInfo(T).array.len;
        const slice = try dec.readBytes(N);
        return slice[0..N].*;
    }

    // Optional
    if (comptime isOptional(T)) {
        const Child = @typeInfo(T).optional.child;
        const is_some = try dec.decodeOptionTag();
        if (is_some) {
            return try decodeField(Child, allocator, dec);
        } else {
            return null;
        }
    }

    // Slices (arrays of non-u8)
    if (comptime isSlice(T)) {
        const Child = @typeInfo(T).pointer.child;
        const count = try dec.decodeArrayLen();
        const items = try allocator.alloc(Child, count);
        var decoded: usize = 0;
        errdefer {
            for (items[0..decoded]) |*item| freeField(Child, allocator, item);
            allocator.free(items);
        }
        for (items, 0..) |*item, i| {
            _ = i;
            item.* = try decodeField(Child, allocator, dec);
            decoded += 1;
        }
        return items;
    }

    // Nested structs (product types)
    if (@typeInfo(T) == .@"struct") {
        return decodeStruct(T, allocator, dec);
    }

    @compileError("Unsupported type for BSATN decoding: " ++ @typeName(T));
}

// ============================================================
// Comptime encoding internals
// ============================================================

fn encodeStruct(comptime T: type, allocator: std.mem.Allocator, enc: *Encoder, value: T) !void {
    const info = @typeInfo(T).@"struct";
    inline for (info.fields) |field| {
        try encodeField(field.type, allocator, enc, @field(value, field.name));
    }
}

fn encodeField(comptime T: type, allocator: std.mem.Allocator, enc: *Encoder, value: T) !void {
    if (T == bool) return enc.encodeBool(allocator, value);
    if (T == u8) return enc.encodeU8(allocator, value);
    if (T == i8) return enc.encodeI8(allocator, value);
    if (T == u16) return enc.encodeU16(allocator, value);
    if (T == i16) return enc.encodeI16(allocator, value);
    if (T == u32) return enc.encodeU32(allocator, value);
    if (T == i32) return enc.encodeI32(allocator, value);
    if (T == u64) return enc.encodeU64(allocator, value);
    if (T == i64) return enc.encodeI64(allocator, value);
    if (T == u128) return enc.encodeU128(allocator, value);
    if (T == i128) return enc.encodeI128(allocator, value);
    if (T == f32) return enc.encodeF32(allocator, value);
    if (T == f64) return enc.encodeF64(allocator, value);

    if (T == []const u8) return enc.encodeString(allocator, value);

    if (comptime isFixedByteArray(T)) {
        return enc.appendRaw(allocator, &value);
    }

    if (comptime isOptional(T)) {
        const Child = @typeInfo(T).optional.child;
        if (value) |v| {
            try enc.encodeU8(allocator, 0); // Some tag
            try encodeField(Child, allocator, enc, v);
        } else {
            try enc.encodeU8(allocator, 1); // None tag
        }
        return;
    }

    if (comptime isSlice(T)) {
        const Child = @typeInfo(T).pointer.child;
        try enc.encodeArrayHeader(allocator, @intCast(value.len));
        for (value) |item| {
            try encodeField(Child, allocator, enc, item);
        }
        return;
    }

    if (@typeInfo(T) == .@"struct") {
        return encodeStruct(T, allocator, enc, value);
    }

    @compileError("Unsupported type for BSATN encoding: " ++ @typeName(T));
}

// ============================================================
// Field extraction from AlgebraicValue (for fromRow)
// ============================================================

fn extractField(comptime T: type, av: AlgebraicValue) DecodeError!T {
    if (T == bool) return av.bool;
    if (T == u8) return av.u8;
    if (T == i8) return av.i8;
    if (T == u16) return av.u16;
    if (T == i16) return av.i16;
    if (T == u32) return av.u32;
    if (T == i32) return av.i32;
    if (T == u64) return av.u64;
    if (T == i64) return av.i64;
    if (T == u128) return av.u128;
    if (T == i128) return av.i128;
    if (T == f32) return av.f32;
    if (T == f64) return av.f64;
    if (T == []const u8) return av.string;

    if (comptime isOptional(T)) {
        const Child = @typeInfo(T).optional.child;
        if (av.option) |inner| {
            return try extractField(Child, inner.*);
        } else {
            return null;
        }
    }

    return error.TypeMismatch;
}

// ============================================================
// Comptime type helpers
// ============================================================

fn isOptional(comptime T: type) bool {
    return @typeInfo(T) == .optional;
}

fn isSlice(comptime T: type) bool {
    if (@typeInfo(T) != .pointer) return false;
    const info = @typeInfo(T).pointer;
    return info.size == .slice and !(info.child == u8 and info.is_const);
}

fn isFixedByteArray(comptime T: type) bool {
    if (@typeInfo(T) != .array) return false;
    return @typeInfo(T).array.child == u8;
}

fn freeField(comptime T: type, allocator: std.mem.Allocator, ptr: *T) void {
    if (T == []const u8) {
        allocator.free(ptr.*);
        return;
    }
    if (comptime isSlice(T)) {
        const Child = @typeInfo(T).pointer.child;
        for (ptr.*) |*item| freeField(Child, allocator, item);
        allocator.free(ptr.*);
        return;
    }
    if (comptime isOptional(T)) {
        const Child = @typeInfo(T).optional.child;
        if (ptr.*) |*inner| {
            freeField(Child, allocator, inner);
        }
        return;
    }
    if (@typeInfo(T) == .@"struct") {
        const info = @typeInfo(T).@"struct";
        inline for (info.fields) |field| {
            freeField(field.type, allocator, &@field(ptr.*, field.name));
        }
        return;
    }
    // Primitives and fixed arrays: nothing to free
}

// ============================================================
// Tests
// ============================================================

const Person = struct {
    id: u64,
    name: []const u8,
    age: u32,
};

fn encodePerson(allocator: std.mem.Allocator, id: u64, name: []const u8, age: u32) ![]u8 {
    var enc = Encoder.init();
    defer enc.deinit(allocator);
    try enc.encodeU64(allocator, id);
    try enc.encodeString(allocator, name);
    try enc.encodeU32(allocator, age);
    return enc.toOwnedSlice(allocator);
}

test "decode typed struct from BSATN" {
    const allocator = std.testing.allocator;
    const data = try encodePerson(allocator, 1, "Alice", 30);
    defer allocator.free(data);

    var person = try decodeRow(Person, allocator, data);
    defer freeTypedRow(Person, allocator, &person);

    try std.testing.expectEqual(@as(u64, 1), person.id);
    try std.testing.expectEqualStrings("Alice", person.name);
    try std.testing.expectEqual(@as(u32, 30), person.age);
}

test "encode typed struct to BSATN" {
    const allocator = std.testing.allocator;

    const person = Person{ .id = 42, .name = "Bob", .age = 25 };
    const encoded = try encodeRow(Person, allocator, person);
    defer allocator.free(encoded);

    // Decode back and verify roundtrip
    var decoded = try decodeRow(Person, allocator, encoded);
    defer freeTypedRow(Person, allocator, &decoded);

    try std.testing.expectEqual(@as(u64, 42), decoded.id);
    try std.testing.expectEqualStrings("Bob", decoded.name);
    try std.testing.expectEqual(@as(u32, 25), decoded.age);
}

test "decode typed struct with optional field" {
    const allocator = std.testing.allocator;

    const WithOpt = struct {
        id: u32,
        email: ?[]const u8,
    };

    // Encode: id=1, email=Some("test@x.com")
    var enc = Encoder.init();
    defer enc.deinit(allocator);
    try enc.encodeU32(allocator, 1);
    try enc.encodeU8(allocator, 0); // Some tag
    try enc.encodeString(allocator, "test@x.com");

    var row = try decodeRow(WithOpt, allocator, enc.writtenSlice());
    defer freeTypedRow(WithOpt, allocator, &row);

    try std.testing.expectEqual(@as(u32, 1), row.id);
    try std.testing.expect(row.email != null);
    try std.testing.expectEqualStrings("test@x.com", row.email.?);

    // Encode: id=2, email=None
    var enc2 = Encoder.init();
    defer enc2.deinit(allocator);
    try enc2.encodeU32(allocator, 2);
    try enc2.encodeU8(allocator, 1); // None tag

    var row2 = try decodeRow(WithOpt, allocator, enc2.writtenSlice());
    defer freeTypedRow(WithOpt, allocator, &row2);

    try std.testing.expectEqual(@as(u32, 2), row2.id);
    try std.testing.expect(row2.email == null);
}

test "decode typed struct with nested struct" {
    const allocator = std.testing.allocator;

    const Point = struct { x: u32, y: u32 };
    const WithPoint = struct { id: u64, pos: Point };

    var enc = Encoder.init();
    defer enc.deinit(allocator);
    try enc.encodeU64(allocator, 1);
    try enc.encodeU32(allocator, 100);
    try enc.encodeU32(allocator, 200);

    var row = try decodeRow(WithPoint, allocator, enc.writtenSlice());
    defer freeTypedRow(WithPoint, allocator, &row);

    try std.testing.expectEqual(@as(u64, 1), row.id);
    try std.testing.expectEqual(@as(u32, 100), row.pos.x);
    try std.testing.expectEqual(@as(u32, 200), row.pos.y);
}

test "fromRow converts dynamic Row to typed struct" {
    const allocator = std.testing.allocator;
    const data = try encodePerson(allocator, 7, "Charlie", 40);
    defer allocator.free(data);

    const columns = [_]Column{
        .{ .name = "id", .type = .u64 },
        .{ .name = "name", .type = .string },
        .{ .name = "age", .type = .u32 },
    };

    const row = try row_decoder.decodeRow(allocator, data, &columns);
    defer row.deinit(allocator);

    const person = try fromRow(Person, &row);
    // Note: fromRow borrows from Row — no separate free needed for person fields

    try std.testing.expectEqual(@as(u64, 7), person.id);
    try std.testing.expectEqualStrings("Charlie", person.name);
    try std.testing.expectEqual(@as(u32, 40), person.age);
}

test "typed row list decode" {
    const allocator = std.testing.allocator;

    const row1 = try encodePerson(allocator, 1, "Al", 20);
    defer allocator.free(row1);
    const row2 = try encodePerson(allocator, 2, "Bo", 30);
    defer allocator.free(row2);

    const all_data = try std.mem.concat(allocator, u8, &.{ row1, row2 });
    defer allocator.free(all_data);

    const offset_vals = [_]u64{ 0, row1.len };
    var offset_bytes: [2 * 8]u8 = undefined;
    for (offset_vals, 0..) |val, i| {
        std.mem.writeInt(u64, offset_bytes[i * 8 ..][0..8], val, .little);
    }

    const row_list = BsatnRowList{
        .size_hint = .{ .row_offsets = .{
            .count = 2,
            .raw_bytes = &offset_bytes,
        } },
        .rows_data = all_data,
    };

    const rows = try decodeRowList(Person, allocator, row_list);
    defer freeTypedRows(Person, allocator, rows);

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqualStrings("Al", rows[0].name);
    try std.testing.expectEqualStrings("Bo", rows[1].name);
    try std.testing.expectEqual(@as(u64, 1), rows[0].id);
    try std.testing.expectEqual(@as(u64, 2), rows[1].id);
}

test "decodeRow errdefer frees already-decoded fields on failure" {
    // Verifies that comptime var errdefer cleanup works correctly:
    // encode a valid string field, then truncate so the next field fails.
    // The string from field 1 must be freed (no leak).
    const allocator = std.testing.allocator;

    const TwoStrings = struct {
        first: []const u8,
        second: []const u8,
    };

    // Encode only one string, truncating before the second
    var enc = Encoder.init();
    defer enc.deinit(allocator);
    try enc.encodeString(allocator, "hello");
    // Don't encode second string — decodeRow should fail with BufferTooShort

    const result = decodeRow(TwoStrings, allocator, enc.writtenSlice());
    // Should fail, and the allocator leak detector verifies "hello" was freed
    try std.testing.expectError(error.BufferTooShort, result);
}
