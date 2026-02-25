// BSATN (Binary SpacetimeDB Algebraic Type Notation) codec
//
// Little-endian binary format used as the wire protocol for all
// SpacetimeDB messages and row data.
//
// Encoding rules:
// - All integers: little-endian
// - Booleans: 1 byte (0x00 = false, 0x01 = true)
// - Floats: IEEE 754, little-endian
// - Strings/bytes: u32 length prefix + data
// - Arrays: u32 count prefix + concatenated elements
// - Options: u8 tag (0 = Some + payload, 1 = None)
// - Products: concatenated fields (no separators)
// - Sums: u8 tag + payload

const std = @import("std");
const types = @import("types.zig");

const AlgebraicType = types.AlgebraicType;
const AlgebraicValue = types.AlgebraicValue;

pub const Error = error{
    BufferTooShort,
    InvalidBool,
    InvalidOptionTag,
    InvalidSumTag,
    Overflow,
    InvalidUtf8,
};

/// Write a little-endian integer to the buffer.
fn appendLittle(buf: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator, comptime T: type, val: T) !void {
    const le = std.mem.nativeToLittle(T, val);
    try buf.appendSlice(allocator, &std.mem.toBytes(le));
}

/// Read a little-endian integer from a byte slice.
fn readLittle(comptime T: type, slice: []const u8) T {
    const Bytes = [@sizeOf(T)]u8;
    return std.mem.littleToNative(T, @bitCast(@as(Bytes, slice[0..@sizeOf(T)].*)));
}

// ============================================================
// Encoder
// ============================================================

pub const Encoder = struct {
    buf: std.ArrayListUnmanaged(u8),

    pub fn init() Encoder {
        return .{ .buf = .{} };
    }

    pub fn deinit(self: *Encoder, allocator: std.mem.Allocator) void {
        self.buf.deinit(allocator);
    }

    pub fn toOwnedSlice(self: *Encoder, allocator: std.mem.Allocator) std.mem.Allocator.Error![]u8 {
        return self.buf.toOwnedSlice(allocator);
    }

    pub fn writtenSlice(self: *const Encoder) []const u8 {
        return self.buf.items;
    }

    // -- Primitive encoders --

    pub fn encodeBool(self: *Encoder, allocator: std.mem.Allocator, val: bool) !void {
        try self.buf.append(allocator, if (val) @as(u8, 1) else @as(u8, 0));
    }

    pub fn encodeU8(self: *Encoder, allocator: std.mem.Allocator, val: u8) !void {
        try self.buf.append(allocator, val);
    }

    pub fn encodeI8(self: *Encoder, allocator: std.mem.Allocator, val: i8) !void {
        try self.buf.append(allocator, @bitCast(val));
    }

    pub fn encodeU16(self: *Encoder, allocator: std.mem.Allocator, val: u16) !void {
        try appendLittle(&self.buf, allocator, u16, val);
    }

    pub fn encodeI16(self: *Encoder, allocator: std.mem.Allocator, val: i16) !void {
        try appendLittle(&self.buf, allocator, i16, val);
    }

    pub fn encodeU32(self: *Encoder, allocator: std.mem.Allocator, val: u32) !void {
        try appendLittle(&self.buf, allocator, u32, val);
    }

    pub fn encodeI32(self: *Encoder, allocator: std.mem.Allocator, val: i32) !void {
        try appendLittle(&self.buf, allocator, i32, val);
    }

    pub fn encodeU64(self: *Encoder, allocator: std.mem.Allocator, val: u64) !void {
        try appendLittle(&self.buf, allocator, u64, val);
    }

    pub fn encodeI64(self: *Encoder, allocator: std.mem.Allocator, val: i64) !void {
        try appendLittle(&self.buf, allocator, i64, val);
    }

    pub fn encodeU128(self: *Encoder, allocator: std.mem.Allocator, val: u128) !void {
        try appendLittle(&self.buf, allocator, u128, val);
    }

    pub fn encodeI128(self: *Encoder, allocator: std.mem.Allocator, val: i128) !void {
        try appendLittle(&self.buf, allocator, i128, val);
    }

    pub fn encodeU256(self: *Encoder, allocator: std.mem.Allocator, val: [32]u8) !void {
        try self.buf.appendSlice(allocator, &val);
    }

    pub fn encodeI256(self: *Encoder, allocator: std.mem.Allocator, val: [32]u8) !void {
        try self.buf.appendSlice(allocator, &val);
    }

    pub fn encodeF32(self: *Encoder, allocator: std.mem.Allocator, val: f32) !void {
        try appendLittle(&self.buf, allocator, u32, @bitCast(val));
    }

    pub fn encodeF64(self: *Encoder, allocator: std.mem.Allocator, val: f64) !void {
        try appendLittle(&self.buf, allocator, u64, @bitCast(val));
    }

    pub fn encodeString(self: *Encoder, allocator: std.mem.Allocator, val: []const u8) !void {
        const len: u32 = @intCast(val.len);
        try self.encodeU32(allocator, len);
        try self.buf.appendSlice(allocator, val);
    }

    pub fn encodeBytes(self: *Encoder, allocator: std.mem.Allocator, val: []const u8) !void {
        try self.encodeString(allocator, val);
    }

    // -- Composite encoders --

    /// Encode a sum type: u8 tag + payload bytes.
    pub fn encodeSum(self: *Encoder, allocator: std.mem.Allocator, tag: u8, payload: []const u8) !void {
        try self.buf.append(allocator, tag);
        try self.buf.appendSlice(allocator, payload);
    }

    /// Encode an option None (tag 1).
    pub fn encodeOptionNone(self: *Encoder, allocator: std.mem.Allocator) !void {
        try self.buf.append(allocator, 1);
    }

    /// Encode an option Some (tag 0 + payload).
    pub fn encodeOptionSome(self: *Encoder, allocator: std.mem.Allocator, payload: []const u8) !void {
        try self.buf.append(allocator, 0);
        try self.buf.appendSlice(allocator, payload);
    }

    /// Encode an array header (u32 count).
    pub fn encodeArrayHeader(self: *Encoder, allocator: std.mem.Allocator, count: u32) !void {
        try self.encodeU32(allocator, count);
    }

    /// Append raw pre-encoded bytes (for product fields or array elements).
    pub fn appendRaw(self: *Encoder, allocator: std.mem.Allocator, data: []const u8) !void {
        try self.buf.appendSlice(allocator, data);
    }

    /// Encode an AlgebraicValue according to its runtime type tag.
    pub fn encodeValue(self: *Encoder, allocator: std.mem.Allocator, value: AlgebraicValue) !void {
        switch (value) {
            .bool => |v| try self.encodeBool(allocator, v),
            .u8 => |v| try self.encodeU8(allocator, v),
            .i8 => |v| try self.encodeI8(allocator, v),
            .u16 => |v| try self.encodeU16(allocator, v),
            .i16 => |v| try self.encodeI16(allocator, v),
            .u32 => |v| try self.encodeU32(allocator, v),
            .i32 => |v| try self.encodeI32(allocator, v),
            .u64 => |v| try self.encodeU64(allocator, v),
            .i64 => |v| try self.encodeI64(allocator, v),
            .u128 => |v| try self.encodeU128(allocator, v),
            .i128 => |v| try self.encodeI128(allocator, v),
            .u256 => |v| try self.encodeU256(allocator, v),
            .i256 => |v| try self.encodeI256(allocator, v),
            .f32 => |v| try self.encodeF32(allocator, v),
            .f64 => |v| try self.encodeF64(allocator, v),
            .string => |v| try self.encodeString(allocator, v),
            .bytes => |v| try self.encodeBytes(allocator, v),
            .array => |items| {
                try self.encodeArrayHeader(allocator, @intCast(items.len));
                for (items) |item| {
                    try self.encodeValue(allocator, item);
                }
            },
            .option => |opt| {
                if (opt) |val| {
                    try self.buf.append(allocator, 0); // Some tag
                    try self.encodeValue(allocator, val.*);
                } else {
                    try self.buf.append(allocator, 1); // None tag
                }
            },
            .product => |fields| {
                for (fields) |field| {
                    try self.encodeValue(allocator, field.value);
                }
            },
            .sum => |s| {
                try self.buf.append(allocator, s.tag);
                try self.encodeValue(allocator, s.value.*);
            },
        }
    }
};

// ============================================================
// Decoder
// ============================================================

pub const Decoder = struct {
    data: []const u8,
    pos: usize,

    pub fn init(data: []const u8) Decoder {
        return .{ .data = data, .pos = 0 };
    }

    pub fn remaining(self: *const Decoder) []const u8 {
        return self.data[self.pos..];
    }

    pub fn bytesRemaining(self: *const Decoder) usize {
        return self.data.len - self.pos;
    }

    pub fn readBytes(self: *Decoder, n: usize) Error![]const u8 {
        if (self.pos + n > self.data.len) return Error.BufferTooShort;
        const slice = self.data[self.pos .. self.pos + n];
        self.pos += n;
        return slice;
    }

    fn readByte(self: *Decoder) Error!u8 {
        if (self.pos >= self.data.len) return Error.BufferTooShort;
        const b = self.data[self.pos];
        self.pos += 1;
        return b;
    }

    // -- Primitive decoders --

    pub fn decodeBool(self: *Decoder) Error!bool {
        const b = try self.readByte();
        return switch (b) {
            0 => false,
            1 => true,
            else => Error.InvalidBool,
        };
    }

    pub fn decodeU8(self: *Decoder) Error!u8 {
        return self.readByte();
    }

    pub fn decodeI8(self: *Decoder) Error!i8 {
        return @bitCast(try self.readByte());
    }

    pub fn decodeU16(self: *Decoder) Error!u16 {
        const slice = try self.readBytes(2);
        return readLittle(u16, slice);
    }

    pub fn decodeI16(self: *Decoder) Error!i16 {
        const slice = try self.readBytes(2);
        return readLittle(i16, slice);
    }

    pub fn decodeU32(self: *Decoder) Error!u32 {
        const slice = try self.readBytes(4);
        return readLittle(u32, slice);
    }

    pub fn decodeI32(self: *Decoder) Error!i32 {
        const slice = try self.readBytes(4);
        return readLittle(i32, slice);
    }

    pub fn decodeU64(self: *Decoder) Error!u64 {
        const slice = try self.readBytes(8);
        return readLittle(u64, slice);
    }

    pub fn decodeI64(self: *Decoder) Error!i64 {
        const slice = try self.readBytes(8);
        return readLittle(i64, slice);
    }

    pub fn decodeU128(self: *Decoder) Error!u128 {
        const slice = try self.readBytes(16);
        return readLittle(u128, slice);
    }

    pub fn decodeI128(self: *Decoder) Error!i128 {
        const slice = try self.readBytes(16);
        return readLittle(i128, slice);
    }

    pub fn decodeU256(self: *Decoder) Error![32]u8 {
        const slice = try self.readBytes(32);
        return slice[0..32].*;
    }

    pub fn decodeI256(self: *Decoder) Error![32]u8 {
        const slice = try self.readBytes(32);
        return slice[0..32].*;
    }

    pub fn decodeF32(self: *Decoder) Error!f32 {
        const slice = try self.readBytes(4);
        const bits = readLittle(u32, slice);
        return @bitCast(bits);
    }

    pub fn decodeF64(self: *Decoder) Error!f64 {
        const slice = try self.readBytes(8);
        const bits = readLittle(u64, slice);
        return @bitCast(bits);
    }

    /// Decode a length-prefixed string. Returns a slice into the decoder's buffer.
    pub fn decodeString(self: *Decoder) Error![]const u8 {
        const len = try self.decodeU32();
        return self.readBytes(len);
    }

    /// Decode length-prefixed bytes. Returns a slice into the decoder's buffer.
    pub fn decodeBytes(self: *Decoder) Error![]const u8 {
        return self.decodeString();
    }

    // -- Composite decoders --

    /// Decode a sum tag (u8).
    pub fn decodeSumTag(self: *Decoder) Error!u8 {
        return self.readByte();
    }

    /// Decode an option tag. Returns true for Some (tag 0), false for None (tag 1).
    pub fn decodeOptionTag(self: *Decoder) Error!bool {
        const tag = try self.readByte();
        return switch (tag) {
            0 => true, // Some
            1 => false, // None
            else => Error.InvalidOptionTag,
        };
    }

    /// Decode an array header (u32 count).
    pub fn decodeArrayLen(self: *Decoder) Error!u32 {
        return self.decodeU32();
    }

    /// Decode a complete AlgebraicValue given its type.
    /// Allocates memory for variable-size types (strings, arrays, products, sums).
    pub fn decodeValue(self: *Decoder, allocator: std.mem.Allocator, typ: AlgebraicType) (Error || std.mem.Allocator.Error)!AlgebraicValue {
        return switch (typ) {
            .bool => .{ .bool = try self.decodeBool() },
            .u8 => .{ .u8 = try self.decodeU8() },
            .i8 => .{ .i8 = try self.decodeI8() },
            .u16 => .{ .u16 = try self.decodeU16() },
            .i16 => .{ .i16 = try self.decodeI16() },
            .u32 => .{ .u32 = try self.decodeU32() },
            .i32 => .{ .i32 = try self.decodeI32() },
            .u64 => .{ .u64 = try self.decodeU64() },
            .i64 => .{ .i64 = try self.decodeI64() },
            .u128 => .{ .u128 = try self.decodeU128() },
            .i128 => .{ .i128 = try self.decodeI128() },
            .u256 => .{ .u256 = try self.decodeU256() },
            .i256 => .{ .i256 = try self.decodeI256() },
            .f32 => .{ .f32 = try self.decodeF32() },
            .f64 => .{ .f64 = try self.decodeF64() },
            .string => {
                const raw = try self.decodeString();
                const owned = try allocator.dupe(u8, raw);
                return .{ .string = owned };
            },
            .bytes => {
                const raw = try self.decodeBytes();
                const owned = try allocator.dupe(u8, raw);
                return .{ .bytes = owned };
            },
            .array => |inner| {
                const count = try self.decodeArrayLen();
                const items = try allocator.alloc(AlgebraicValue, count);
                errdefer allocator.free(items);
                for (items, 0..) |*item, i| {
                    errdefer for (items[0..i]) |*prev| prev.deinit(allocator);
                    item.* = try self.decodeValue(allocator, inner.*);
                }
                return .{ .array = items };
            },
            .option => |inner| {
                const is_some = try self.decodeOptionTag();
                if (is_some) {
                    const val = try allocator.create(AlgebraicValue);
                    errdefer allocator.destroy(val);
                    val.* = try self.decodeValue(allocator, inner.*);
                    return .{ .option = val };
                } else {
                    return .{ .option = null };
                }
            },
            .product => |columns| {
                const fields = try allocator.alloc(AlgebraicValue.FieldValue, columns.len);
                errdefer allocator.free(fields);
                for (columns, 0..) |col, i| {
                    errdefer for (fields[0..i]) |*prev| prev.value.deinit(allocator);
                    fields[i] = .{
                        .name = col.name,
                        .value = try self.decodeValue(allocator, col.type),
                    };
                }
                return .{ .product = fields };
            },
            .sum => |variants| {
                const tag = try self.decodeSumTag();
                if (tag >= variants.len) return Error.InvalidSumTag;
                const val = try allocator.create(AlgebraicValue);
                errdefer allocator.destroy(val);
                val.* = try self.decodeValue(allocator, variants[tag].type);
                return .{ .sum = .{ .tag = tag, .value = val } };
            },
            .ref => unreachable, // Refs must be resolved before decoding
        };
    }
};

// ============================================================
// Tests
// ============================================================

test "encode and decode bool" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    try enc.encodeBool(allocator, true);
    try enc.encodeBool(allocator, false);

    var dec = Decoder.init(enc.writtenSlice());
    try std.testing.expect(try dec.decodeBool() == true);
    try std.testing.expect(try dec.decodeBool() == false);
    try std.testing.expectEqual(@as(usize, 0), dec.bytesRemaining());
}

test "encode and decode integers" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    try enc.encodeU8(allocator, 255);
    try enc.encodeI8(allocator, -128);
    try enc.encodeU16(allocator, 0xBEEF);
    try enc.encodeI16(allocator, -1234);
    try enc.encodeU32(allocator, 0xDEADBEEF);
    try enc.encodeI32(allocator, -100_000);
    try enc.encodeU64(allocator, 0xCAFEBABE_DEADBEEF);
    try enc.encodeI64(allocator, -9_000_000_000);

    var dec = Decoder.init(enc.writtenSlice());
    try std.testing.expectEqual(@as(u8, 255), try dec.decodeU8());
    try std.testing.expectEqual(@as(i8, -128), try dec.decodeI8());
    try std.testing.expectEqual(@as(u16, 0xBEEF), try dec.decodeU16());
    try std.testing.expectEqual(@as(i16, -1234), try dec.decodeI16());
    try std.testing.expectEqual(@as(u32, 0xDEADBEEF), try dec.decodeU32());
    try std.testing.expectEqual(@as(i32, -100_000), try dec.decodeI32());
    try std.testing.expectEqual(@as(u64, 0xCAFEBABE_DEADBEEF), try dec.decodeU64());
    try std.testing.expectEqual(@as(i64, -9_000_000_000), try dec.decodeI64());
    try std.testing.expectEqual(@as(usize, 0), dec.bytesRemaining());
}

test "encode and decode u128/i128" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    const big: u128 = 340_282_366_920_938_463_463_374_607_431_768_211_455;
    const neg: i128 = -170_141_183_460_469_231_731_687_303_715_884_105_728;
    try enc.encodeU128(allocator, big);
    try enc.encodeI128(allocator, neg);

    var dec = Decoder.init(enc.writtenSlice());
    try std.testing.expectEqual(big, try dec.decodeU128());
    try std.testing.expectEqual(neg, try dec.decodeI128());
}

test "encode and decode floats" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    try enc.encodeF32(allocator, 3.14);
    try enc.encodeF64(allocator, 2.718281828459045);

    var dec = Decoder.init(enc.writtenSlice());
    try std.testing.expectApproxEqAbs(@as(f32, 3.14), try dec.decodeF32(), 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 2.718281828459045), try dec.decodeF64(), 0.0000001);
}

test "encode and decode string" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    try enc.encodeString(allocator, "hello world");
    try enc.encodeString(allocator, "");

    var dec = Decoder.init(enc.writtenSlice());
    try std.testing.expectEqualStrings("hello world", try dec.decodeString());
    try std.testing.expectEqualStrings("", try dec.decodeString());
    try std.testing.expectEqual(@as(usize, 0), dec.bytesRemaining());
}

test "encode and decode option (Some and None)" {
    const allocator = std.testing.allocator;

    // Encode Some(42u32) and None using raw encoder
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    // Some(42u32): tag 0 + u32
    try enc.encodeU8(allocator, 0); // Some tag
    try enc.encodeU32(allocator, 42);
    // None: tag 1
    try enc.encodeU8(allocator, 1); // None tag

    var dec = Decoder.init(enc.writtenSlice());
    // Some
    try std.testing.expect(try dec.decodeOptionTag());
    try std.testing.expectEqual(@as(u32, 42), try dec.decodeU32());
    // None
    try std.testing.expect(!try dec.decodeOptionTag());
    try std.testing.expectEqual(@as(usize, 0), dec.bytesRemaining());
}

test "encode and decode array" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    // Array of 3 u16s
    try enc.encodeArrayHeader(allocator, 3);
    try enc.encodeU16(allocator, 10);
    try enc.encodeU16(allocator, 20);
    try enc.encodeU16(allocator, 30);

    var dec = Decoder.init(enc.writtenSlice());
    const len = try dec.decodeArrayLen();
    try std.testing.expectEqual(@as(u32, 3), len);
    try std.testing.expectEqual(@as(u16, 10), try dec.decodeU16());
    try std.testing.expectEqual(@as(u16, 20), try dec.decodeU16());
    try std.testing.expectEqual(@as(u16, 30), try dec.decodeU16());
    try std.testing.expectEqual(@as(usize, 0), dec.bytesRemaining());
}

test "encode and decode sum" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    // Sum tag 2, payload "variant data"
    try enc.encodeSum(allocator, 2, "variant data");

    var dec = Decoder.init(enc.writtenSlice());
    try std.testing.expectEqual(@as(u8, 2), try dec.decodeSumTag());
    const payload = try dec.readBytes(12);
    try std.testing.expectEqualStrings("variant data", payload);
}

test "buffer too short returns error" {
    var dec = Decoder.init(&[_]u8{0x01});
    _ = try dec.decodeBool(); // OK, consumes 1 byte
    const result = dec.decodeBool();
    try std.testing.expectError(Error.BufferTooShort, result);
}

test "invalid bool value returns error" {
    var dec = Decoder.init(&[_]u8{0x02});
    try std.testing.expectError(Error.InvalidBool, dec.decodeBool());
}

test "decodeValue roundtrip for product type" {
    const allocator = std.testing.allocator;

    // Define a product type: { name: string, age: u32 }
    const columns = [_]types.Column{
        .{ .name = "name", .type = .string },
        .{ .name = "age", .type = .u32 },
    };
    const product_type: AlgebraicType = .{ .product = &columns };

    // Encode a product value
    var enc = Encoder.init();
    defer enc.deinit(allocator);
    try enc.encodeString(allocator, "Alice");
    try enc.encodeU32(allocator, 30);

    // Decode it
    var dec = Decoder.init(enc.writtenSlice());
    const val = try dec.decodeValue(allocator, product_type);
    defer val.deinit(allocator);

    const fields = val.product;
    try std.testing.expectEqual(@as(usize, 2), fields.len);
    try std.testing.expectEqualStrings("Alice", fields[0].value.string);
    try std.testing.expectEqual(@as(u32, 30), fields[1].value.u32);
}

test "decodeValue roundtrip for option type" {
    const allocator = std.testing.allocator;

    const inner_type: AlgebraicType = .u64;
    const option_type: AlgebraicType = .{ .option = &inner_type };

    // Encode Some(42) using encodeValue
    var enc = Encoder.init();
    defer enc.deinit(allocator);
    const some_val: AlgebraicValue = .{ .option = &AlgebraicValue{ .u64 = 42 } };
    try enc.encodeValue(allocator, some_val);

    var dec = Decoder.init(enc.writtenSlice());
    const decoded = try dec.decodeValue(allocator, option_type);
    defer decoded.deinit(allocator);

    try std.testing.expect(decoded.option != null);
    try std.testing.expectEqual(@as(u64, 42), decoded.option.?.u64);

    // Encode None
    var enc2 = Encoder.init();
    defer enc2.deinit(allocator);
    const none_val: AlgebraicValue = .{ .option = null };
    try enc2.encodeValue(allocator, none_val);

    var dec2 = Decoder.init(enc2.writtenSlice());
    const decoded_none = try dec2.decodeValue(allocator, option_type);
    defer decoded_none.deinit(allocator);

    try std.testing.expect(decoded_none.option == null);
}

test "encodeValue and decodeValue roundtrip for array" {
    const allocator = std.testing.allocator;

    // array of u32
    const inner: AlgebraicType = .u32;
    const array_type: AlgebraicType = .{ .array = &inner };

    const items = [_]AlgebraicValue{
        .{ .u32 = 100 },
        .{ .u32 = 200 },
        .{ .u32 = 300 },
    };
    const val: AlgebraicValue = .{ .array = &items };

    var enc = Encoder.init();
    defer enc.deinit(allocator);
    try enc.encodeValue(allocator, val);

    var dec = Decoder.init(enc.writtenSlice());
    const decoded = try dec.decodeValue(allocator, array_type);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), decoded.array.len);
    try std.testing.expectEqual(@as(u32, 100), decoded.array[0].u32);
    try std.testing.expectEqual(@as(u32, 200), decoded.array[1].u32);
    try std.testing.expectEqual(@as(u32, 300), decoded.array[2].u32);
}

test "encodeValue and decodeValue roundtrip for sum" {
    const allocator = std.testing.allocator;

    // Sum with 2 variants: tag 0 = u32, tag 1 = string
    const variants = [_]types.Column{
        .{ .name = "num", .type = .u32 },
        .{ .name = "text", .type = .string },
    };
    const sum_type: AlgebraicType = .{ .sum = &variants };

    // Encode variant 1 (string)
    var enc = Encoder.init();
    defer enc.deinit(allocator);
    const inner_val: AlgebraicValue = .{ .string = "hello" };
    const sum_val: AlgebraicValue = .{ .sum = .{ .tag = 1, .value = &inner_val } };
    try enc.encodeValue(allocator, sum_val);

    var dec = Decoder.init(enc.writtenSlice());
    const decoded = try dec.decodeValue(allocator, sum_type);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(@as(u8, 1), decoded.sum.tag);
    try std.testing.expectEqualStrings("hello", decoded.sum.value.string);
}
