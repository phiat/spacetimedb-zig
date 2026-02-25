// Row Decoder
//
// Decodes BSATN row data into AlgebraicValues using table schema.
// Takes a BsatnRowList (from SubscribeApplied / TransactionUpdate)
// and column definitions, produces decoded row values.

const std = @import("std");
const types = @import("types.zig");
const bsatn = @import("bsatn.zig");
const protocol = @import("protocol.zig");

const AlgebraicType = types.AlgebraicType;
const AlgebraicValue = types.AlgebraicValue;
const Column = types.Column;
const Decoder = bsatn.Decoder;
const BsatnRowList = protocol.BsatnRowList;
const RowSizeHint = protocol.RowSizeHint;

pub const DecodeError = bsatn.Error || std.mem.Allocator.Error || error{UnknownType};

/// A decoded row: an array of named field values.
pub const Row = struct {
    fields: []const AlgebraicValue.FieldValue,

    /// Get a field value by name.
    pub fn get(self: *const Row, name: []const u8) ?AlgebraicValue {
        for (self.fields) |f| {
            if (f.name) |n| {
                if (std.mem.eql(u8, n, name)) return f.value;
            }
        }
        return null;
    }

    pub fn deinit(self: *const Row, allocator: std.mem.Allocator) void {
        for (self.fields) |*f| {
            f.value.deinit(allocator);
        }
        allocator.free(self.fields);
    }
};

/// Decode a BsatnRowList into a list of decoded rows.
pub fn decodeRowList(
    allocator: std.mem.Allocator,
    row_list: BsatnRowList,
    columns: []const Column,
) DecodeError![]Row {
    const row_binaries = try splitRows(allocator, row_list.size_hint, row_list.rows_data);
    defer allocator.free(row_binaries);

    const rows = try allocator.alloc(Row, row_binaries.len);
    var decoded_count: usize = 0;
    errdefer {
        for (rows[0..decoded_count]) |*r| r.deinit(allocator);
        allocator.free(rows);
    }
    for (row_binaries, 0..) |row_bin, i| {
        rows[i] = try decodeRow(allocator, row_bin, columns);
        decoded_count += 1;
    }
    return rows;
}

/// Decode a single BSATN row binary into a Row using column definitions.
pub fn decodeRow(
    allocator: std.mem.Allocator,
    data: []const u8,
    columns: []const Column,
) DecodeError!Row {
    var dec = Decoder.init(data);
    const fields = try allocator.alloc(AlgebraicValue.FieldValue, columns.len);
    var decoded_fields: usize = 0;
    errdefer {
        for (fields[0..decoded_fields]) |*f| f.value.deinit(allocator);
        allocator.free(fields);
    }

    for (columns, 0..) |col, i| {
        const value = try dec.decodeValue(allocator, col.type);
        fields[i] = .{ .name = col.name, .value = value };
        decoded_fields += 1;
    }

    return .{ .fields = fields };
}

/// Split concatenated row data into individual row slices.
pub fn splitRows(
    allocator: std.mem.Allocator,
    size_hint: RowSizeHint,
    data: []const u8,
) std.mem.Allocator.Error![]const []const u8 {
    return switch (size_hint) {
        .fixed_size => |size| splitFixed(allocator, data, size),
        .row_offsets => |offsets| splitByRowOffsets(allocator, data, offsets),
    };
}

fn splitFixed(
    allocator: std.mem.Allocator,
    data: []const u8,
    size: u16,
) std.mem.Allocator.Error![]const []const u8 {
    if (size == 0 or data.len == 0) {
        return try allocator.alloc([]const u8, 0);
    }
    const count = data.len / size;
    const result = try allocator.alloc([]const u8, count);
    for (result, 0..) |*row, i| {
        const start = i * size;
        row.* = data[start .. start + size];
    }
    return result;
}

fn splitByRowOffsets(
    allocator: std.mem.Allocator,
    data: []const u8,
    offsets: RowSizeHint.RowOffsets,
) std.mem.Allocator.Error![]const []const u8 {
    if (offsets.count == 0) {
        return try allocator.alloc([]const u8, 0);
    }
    const result = try allocator.alloc([]const u8, offsets.count);
    for (0..offsets.count) |i| {
        const start: usize = @intCast(offsets.getOffset(i));
        const end: usize = if (i + 1 < offsets.count)
            @intCast(offsets.getOffset(i + 1))
        else
            data.len;
        result[i] = data[start..end];
    }
    return result;
}

// ============================================================
// Tests — mirroring Elixir row_decoder_test.exs
// ============================================================

const person_columns = [_]Column{
    .{ .name = "id", .type = .u64 },
    .{ .name = "name", .type = .string },
    .{ .name = "age", .type = .u32 },
};

fn encodePerson(allocator: std.mem.Allocator, id: u64, name: []const u8, age: u32) ![]u8 {
    var enc = bsatn.Encoder.init();
    defer enc.deinit(allocator);
    try enc.encodeU64(allocator, id);
    try enc.encodeString(allocator, name);
    try enc.encodeU32(allocator, age);
    return enc.toOwnedSlice(allocator);
}

test "decode single person row" {
    const allocator = std.testing.allocator;
    const data = try encodePerson(allocator, 1, "Alice", 30);
    defer allocator.free(data);

    const row = try decodeRow(allocator, data, &person_columns);
    defer row.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 1), row.get("id").?.u64);
    try std.testing.expectEqualStrings("Alice", row.get("name").?.string);
    try std.testing.expectEqual(@as(u32, 30), row.get("age").?.u32);
}

test "decode row with empty string" {
    const allocator = std.testing.allocator;
    const data = try encodePerson(allocator, 0, "", 0);
    defer allocator.free(data);

    const row = try decodeRow(allocator, data, &person_columns);
    defer row.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 0), row.get("id").?.u64);
    try std.testing.expectEqualStrings("", row.get("name").?.string);
    try std.testing.expectEqual(@as(u32, 0), row.get("age").?.u32);
}

test "decode row with unicode" {
    const allocator = std.testing.allocator;
    const data = try encodePerson(allocator, 42, "日本語", 25);
    defer allocator.free(data);

    const row = try decodeRow(allocator, data, &person_columns);
    defer row.deinit(allocator);

    try std.testing.expectEqualStrings("日本語", row.get("name").?.string);
}

test "decode row_list with fixed_size" {
    const allocator = std.testing.allocator;
    const columns = [_]Column{
        .{ .name = "x", .type = .u32 },
        .{ .name = "y", .type = .u32 },
    };

    // Each row is 8 bytes (two u32s)
    var enc = bsatn.Encoder.init();
    defer enc.deinit(allocator);
    try enc.encodeU32(allocator, 1);
    try enc.encodeU32(allocator, 2);
    try enc.encodeU32(allocator, 3);
    try enc.encodeU32(allocator, 4);

    const row_list = BsatnRowList{
        .size_hint = .{ .fixed_size = 8 },
        .rows_data = enc.writtenSlice(),
    };

    const rows = try decodeRowList(allocator, row_list, &columns);
    defer {
        for (rows) |*r| r.deinit(allocator);
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 2), rows.len);
    try std.testing.expectEqual(@as(u32, 1), rows[0].get("x").?.u32);
    try std.testing.expectEqual(@as(u32, 2), rows[0].get("y").?.u32);
    try std.testing.expectEqual(@as(u32, 3), rows[1].get("x").?.u32);
    try std.testing.expectEqual(@as(u32, 4), rows[1].get("y").?.u32);
}

test "decode row_list with empty data" {
    const allocator = std.testing.allocator;
    const row_list = BsatnRowList{
        .size_hint = .{ .fixed_size = 8 },
        .rows_data = &[_]u8{},
    };

    const rows = try decodeRowList(allocator, row_list, &[_]Column{});
    defer allocator.free(rows);
    try std.testing.expectEqual(@as(usize, 0), rows.len);
}

test "decode row_list with row_offsets" {
    const allocator = std.testing.allocator;
    const row1 = try encodePerson(allocator, 1, "Al", 20);
    defer allocator.free(row1);
    const row2 = try encodePerson(allocator, 2, "Bob", 30);
    defer allocator.free(row2);
    const row3 = try encodePerson(allocator, 3, "Charlie", 40);
    defer allocator.free(row3);

    // Concatenate rows
    const all_data = try std.mem.concat(allocator, u8, &.{ row1, row2, row3 });
    defer allocator.free(all_data);

    // Build raw offset bytes (little-endian u64s)
    const offset_vals = [_]u64{ 0, row1.len, row1.len + row2.len };
    var offset_bytes: [3 * 8]u8 = undefined;
    for (offset_vals, 0..) |val, i| {
        std.mem.writeInt(u64, offset_bytes[i * 8 ..][0..8], val, .little);
    }
    const row_list = BsatnRowList{
        .size_hint = .{ .row_offsets = .{
            .count = 3,
            .raw_bytes = &offset_bytes,
        } },
        .rows_data = all_data,
    };

    const rows = try decodeRowList(allocator, row_list, &person_columns);
    defer {
        for (rows) |*r| r.deinit(allocator);
        allocator.free(rows);
    }

    try std.testing.expectEqual(@as(usize, 3), rows.len);
    try std.testing.expectEqualStrings("Al", rows[0].get("name").?.string);
    try std.testing.expectEqualStrings("Bob", rows[1].get("name").?.string);
    try std.testing.expectEqualStrings("Charlie", rows[2].get("name").?.string);
}

test "decode option some and none" {
    const allocator = std.testing.allocator;

    // Some(42u32)
    var enc1 = bsatn.Encoder.init();
    defer enc1.deinit(allocator);
    try enc1.encodeU8(allocator, 0); // Some tag
    try enc1.encodeU32(allocator, 42);

    const inner_type: AlgebraicType = .u32;
    const opt_col = [_]Column{.{ .name = "val", .type = .{ .option = &inner_type } }};
    const row1 = try decodeRow(allocator, enc1.writtenSlice(), &opt_col);
    defer row1.deinit(allocator);
    try std.testing.expect(row1.get("val").?.option != null);
    try std.testing.expectEqual(@as(u32, 42), row1.get("val").?.option.?.u32);

    // None
    var enc2 = bsatn.Encoder.init();
    defer enc2.deinit(allocator);
    try enc2.encodeU8(allocator, 1); // None tag

    const row2 = try decodeRow(allocator, enc2.writtenSlice(), &opt_col);
    defer row2.deinit(allocator);
    try std.testing.expect(row2.get("val").?.option == null);
}

test "decode array" {
    const allocator = std.testing.allocator;

    var enc = bsatn.Encoder.init();
    defer enc.deinit(allocator);
    try enc.encodeU32(allocator, 3); // count
    try enc.encodeU32(allocator, 10);
    try enc.encodeU32(allocator, 20);
    try enc.encodeU32(allocator, 30);

    const inner_type: AlgebraicType = .u32;
    const col = [_]Column{.{ .name = "vals", .type = .{ .array = &inner_type } }};
    const row = try decodeRow(allocator, enc.writtenSlice(), &col);
    defer row.deinit(allocator);

    const arr = row.get("vals").?.array;
    try std.testing.expectEqual(@as(usize, 3), arr.len);
    try std.testing.expectEqual(@as(u32, 10), arr[0].u32);
    try std.testing.expectEqual(@as(u32, 20), arr[1].u32);
    try std.testing.expectEqual(@as(u32, 30), arr[2].u32);
}

test "decode nested product" {
    const allocator = std.testing.allocator;

    const inner_columns = [_]Column{
        .{ .name = "a", .type = .u8 },
        .{ .name = "b", .type = .u8 },
    };
    const cols = [_]Column{
        .{ .name = "pair", .type = .{ .product = &inner_columns } },
    };

    const data = [_]u8{ 5, 10 };
    const row = try decodeRow(allocator, &data, &cols);
    defer row.deinit(allocator);

    const pair = row.get("pair").?.product;
    try std.testing.expectEqual(@as(usize, 2), pair.len);
    try std.testing.expectEqual(@as(u8, 5), pair[0].value.u8);
    try std.testing.expectEqual(@as(u8, 10), pair[1].value.u8);
}
