// SpacetimeDB Schema Fetcher and Parser
//
// Parses the JSON schema returned by GET /v1/database/{name}/schema?version=9
// into typed Zig structs. Resolves type references eagerly so downstream
// decoders never encounter unresolved refs.

const std = @import("std");
const types = @import("types.zig");
const AlgebraicType = types.AlgebraicType;

pub const SchemaError = error{
    InvalidJson,
    UnknownType,
    InvalidTypeRef,
    MissingField,
} || std.mem.Allocator.Error;

pub const Column = types.Column;

/// A table definition from the schema.
pub const TableDef = struct {
    name: []const u8,
    columns: []const Column,
    primary_key: []const u32,
};

/// A reducer definition from the schema.
pub const ReducerDef = struct {
    name: []const u8,
    params: []const Column,
};

/// Parsed SpacetimeDB module schema.
pub const Schema = struct {
    tables: []const TableDef,
    reducers: []const ReducerDef,
    typespace: []const *AlgebraicType,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *const Schema) void {
        // All memory was allocated from the arena, which the caller manages.
        // This is a no-op if using an arena allocator (recommended).
        _ = self;
    }

    /// Find a table by name.
    pub fn getTable(self: *const Schema, name: []const u8) ?*const TableDef {
        for (self.tables) |*t| {
            if (std.mem.eql(u8, t.name, name)) return t;
        }
        return null;
    }

    /// Get columns for a table by name.
    pub fn columnsFor(self: *const Schema, table_name: []const u8) ?[]const Column {
        const t = self.getTable(table_name) orelse return null;
        return t.columns;
    }

    /// Get primary key indices for a table by name.
    pub fn primaryKeyFor(self: *const Schema, table_name: []const u8) ?[]const u32 {
        const t = self.getTable(table_name) orelse return null;
        return t.primary_key;
    }

    /// Find a reducer by name.
    pub fn getReducer(self: *const Schema, name: []const u8) ?*const ReducerDef {
        for (self.reducers) |*r| {
            if (std.mem.eql(u8, r.name, name)) return r;
        }
        return null;
    }
};

/// Parse a raw JSON schema map into a Schema.
/// All memory is allocated from `allocator` (use an arena for easy cleanup).
pub fn parse(allocator: std.mem.Allocator, json_str: []const u8) SchemaError!Schema {
    const parsed = std.json.parseFromSlice(std.json.Value, allocator, json_str, .{}) catch
        return SchemaError.InvalidJson;

    return parseValue(allocator, parsed.value);
}

/// Parse from an already-parsed JSON value.
pub fn parseValue(allocator: std.mem.Allocator, root: std.json.Value) SchemaError!Schema {
    // Parse typespace
    const typespace = try parseTypespace(allocator, root);

    // Resolve all refs in typespace eagerly
    for (typespace) |t| {
        try resolveRefsInPlace(t, typespace);
    }

    // Parse tables
    const tables = try parseTables(allocator, root, typespace);

    // Parse reducers
    const reducers = try parseReducers(allocator, root);

    return .{
        .tables = tables,
        .reducers = reducers,
        .typespace = typespace,
        .allocator = allocator,
    };
}

// ============================================================
// Typespace parsing
// ============================================================

fn parseTypespace(allocator: std.mem.Allocator, root: std.json.Value) SchemaError![]const *AlgebraicType {
    const ts_val = jsonGet(root, "typespace") orelse return &[_]*AlgebraicType{};
    const types_val = jsonGet(ts_val.*, "types") orelse return &[_]*AlgebraicType{};
    const types_arr = switch (types_val.*) {
        .array => |a| a.items,
        else => return &[_]*AlgebraicType{},
    };

    const result = try allocator.alloc(*AlgebraicType, types_arr.len);
    for (types_arr, 0..) |item, i| {
        result[i] = try parseTypeDef(allocator, item);
    }
    return result;
}

fn parseTypeDef(allocator: std.mem.Allocator, val: std.json.Value) SchemaError!*AlgebraicType {
    if (jsonGet(val, "Product")) |product| {
        if (jsonGet(product.*, "elements")) |elements| {
            const cols = try parseElements(allocator, elements.*);
            const t = try allocator.create(AlgebraicType);
            t.* = .{ .product = cols };
            return t;
        }
    }
    if (jsonGet(val, "Sum")) |sum| {
        if (jsonGet(sum.*, "variants")) |variants| {
            const cols = try parseElements(allocator, variants.*);
            const t = try allocator.create(AlgebraicType);
            t.* = .{ .sum = cols };
            return t;
        }
    }
    const t = try allocator.create(AlgebraicType);
    t.* = .bool; // fallback
    return t;
}

fn parseElements(allocator: std.mem.Allocator, val: std.json.Value) SchemaError![]const types.Column {
    const arr = switch (val) {
        .array => |a| a.items,
        else => return &[_]types.Column{},
    };
    const cols = try allocator.alloc(types.Column, arr.len);
    for (arr, 0..) |item, i| {
        cols[i] = try parseElement(allocator, item);
    }
    return cols;
}

fn parseElement(allocator: std.mem.Allocator, val: std.json.Value) SchemaError!types.Column {
    const name_val = jsonGet(val, "name");
    const name: ?[]const u8 = if (name_val) |nv| unwrapOption(nv.*) else null;

    const at_val = jsonGet(val, "algebraic_type") orelse return SchemaError.MissingField;
    const typ = try parseAlgebraicType(allocator, at_val.*);

    return .{ .name = name, .type = typ };
}

// ============================================================
// Algebraic type parsing
// ============================================================

fn parseAlgebraicType(allocator: std.mem.Allocator, val: std.json.Value) SchemaError!AlgebraicType {
    // Primitives
    if (jsonGet(val, "Bool") != null) return .bool;
    if (jsonGet(val, "U8") != null) return .u8;
    if (jsonGet(val, "I8") != null) return .i8;
    if (jsonGet(val, "U16") != null) return .u16;
    if (jsonGet(val, "I16") != null) return .i16;
    if (jsonGet(val, "U32") != null) return .u32;
    if (jsonGet(val, "I32") != null) return .i32;
    if (jsonGet(val, "U64") != null) return .u64;
    if (jsonGet(val, "I64") != null) return .i64;
    if (jsonGet(val, "U128") != null) return .u128;
    if (jsonGet(val, "I128") != null) return .i128;
    if (jsonGet(val, "U256") != null) return .u256;
    if (jsonGet(val, "I256") != null) return .i256;
    if (jsonGet(val, "F32") != null) return .f32;
    if (jsonGet(val, "F64") != null) return .f64;
    if (jsonGet(val, "String") != null) return .string;
    if (jsonGet(val, "Bytes") != null) return .bytes;

    // Array
    if (jsonGet(val, "Array")) |inner| {
        const inner_type = try allocator.create(AlgebraicType);
        inner_type.* = try parseAlgebraicType(allocator, inner.*);
        return .{ .array = inner_type };
    }

    // Ref
    if (jsonGet(val, "Ref")) |ref_val| {
        const idx = jsonToU32(ref_val.*) orelse return SchemaError.InvalidTypeRef;
        return .{ .ref = idx };
    }

    // Sum — check for Option pattern (2 variants: some + none)
    if (jsonGet(val, "Sum")) |sum| {
        if (jsonGet(sum.*, "variants")) |variants_val| {
            const variants = switch (variants_val.*) {
                .array => |a| a.items,
                else => return SchemaError.InvalidJson,
            };
            if (variants.len == 2) {
                if (isOptionPattern(variants[0], variants[1])) |inner_at| {
                    const inner_type = try allocator.create(AlgebraicType);
                    inner_type.* = try parseAlgebraicType(allocator, inner_at);
                    return .{ .option = inner_type };
                }
            }
            // Regular sum
            const cols = try parseElements(allocator, variants_val.*);
            return .{ .sum = cols };
        }
    }

    // Product
    if (jsonGet(val, "Product")) |product| {
        if (jsonGet(product.*, "elements")) |elements| {
            const cols = try parseElements(allocator, elements.*);
            return .{ .product = cols };
        }
    }

    return SchemaError.UnknownType;
}

/// Check if two variants form the Option pattern: some(T) + none.
fn isOptionPattern(v0: std.json.Value, v1: std.json.Value) ?std.json.Value {
    const name0 = jsonGet(v0, "name") orelse return null;
    const name1 = jsonGet(v1, "name") orelse return null;

    const n0 = unwrapOption(name0.*);
    const n1 = unwrapOption(name1.*);

    if (n0 != null and n1 != null) {
        if (std.mem.eql(u8, n0.?, "some") and std.mem.eql(u8, n1.?, "none")) {
            const at = jsonGet(v0, "algebraic_type") orelse return null;
            return at.*;
        }
    }
    return null;
}

// ============================================================
// Table and reducer parsing
// ============================================================

fn parseTables(allocator: std.mem.Allocator, root: std.json.Value, typespace: []const *AlgebraicType) SchemaError![]const TableDef {
    const tables_val = jsonGet(root, "tables") orelse return &[_]TableDef{};
    const tables_arr = switch (tables_val.*) {
        .array => |a| a.items,
        else => return &[_]TableDef{},
    };

    const result = try allocator.alloc(TableDef, tables_arr.len);
    for (tables_arr, 0..) |item, i| {
        result[i] = try parseTable(allocator, item, typespace);
    }
    return result;
}

fn parseTable(allocator: std.mem.Allocator, val: std.json.Value, typespace: []const *AlgebraicType) SchemaError!TableDef {
    const name = jsonGetString(val, "name") orelse return SchemaError.MissingField;

    // Resolve columns from product_type_ref
    const type_ref = if (jsonGet(val, "product_type_ref")) |ref| jsonToU32(ref.*) else null;
    var columns: []const types.Column = &[_]types.Column{};
    if (type_ref) |ref| {
        if (ref < typespace.len) {
            switch (typespace[ref].*) {
                .product => |cols| {
                    // Deep copy columns and resolve refs in their types
                    const resolved = try allocator.alloc(types.Column, cols.len);
                    for (cols, 0..) |col, ci| {
                        resolved[ci] = .{ .name = col.name, .type = col.type };
                    }
                    // Resolve refs in the copied column types
                    for (resolved) |*col| {
                        try resolveRefsValue(&col.type, typespace);
                    }
                    columns = resolved;
                },
                else => {},
            }
        }
    }

    // Parse primary_key array
    const pk = if (jsonGet(val, "primary_key")) |pk_val| blk: {
        const arr = switch (pk_val.*) {
            .array => |a| a.items,
            else => break :blk &[_]u32{},
        };
        const pks = try allocator.alloc(u32, arr.len);
        for (arr, 0..) |pk_item, pi| {
            pks[pi] = jsonToU32(pk_item) orelse 0;
        }
        break :blk @as([]const u32, pks);
    } else &[_]u32{};

    return .{
        .name = name,
        .columns = columns,
        .primary_key = pk,
    };
}

fn parseReducers(allocator: std.mem.Allocator, root: std.json.Value) SchemaError![]const ReducerDef {
    const reducers_val = jsonGet(root, "reducers") orelse return &[_]ReducerDef{};
    const reducers_arr = switch (reducers_val.*) {
        .array => |a| a.items,
        else => return &[_]ReducerDef{},
    };

    const result = try allocator.alloc(ReducerDef, reducers_arr.len);
    for (reducers_arr, 0..) |item, i| {
        result[i] = try parseReducer(allocator, item);
    }
    return result;
}

fn parseReducer(allocator: std.mem.Allocator, val: std.json.Value) SchemaError!ReducerDef {
    const name = jsonGetString(val, "name") orelse return SchemaError.MissingField;

    var params: []const types.Column = &[_]types.Column{};
    if (jsonGet(val, "params")) |params_val| {
        if (jsonGet(params_val.*, "elements")) |elements| {
            params = try parseElements(allocator, elements.*);
        }
    }

    return .{ .name = name, .params = params };
}

// ============================================================
// Ref resolution
// ============================================================

/// Resolve refs in a heap-allocated AlgebraicType (used for typespace entries).
fn resolveRefsInPlace(t: *AlgebraicType, typespace: []const *AlgebraicType) SchemaError!void {
    switch (t.*) {
        .ref => |idx| {
            if (idx >= typespace.len) return SchemaError.InvalidTypeRef;
            t.* = typespace[idx].*;
            try resolveRefsInPlace(t, typespace);
        },
        .array => |inner| {
            try resolveRefsInPlace(@constCast(inner), typespace);
        },
        .option => |inner| {
            try resolveRefsInPlace(@constCast(inner), typespace);
        },
        .product => |cols| {
            for (@constCast(cols)) |*col| {
                try resolveRefsValue(&col.type, typespace);
            }
        },
        .sum => |cols| {
            for (@constCast(cols)) |*col| {
                try resolveRefsValue(&col.type, typespace);
            }
        },
        else => {},
    }
}

/// Resolve refs in a value-typed AlgebraicType (used for table columns).
fn resolveRefsValue(t: *AlgebraicType, typespace: []const *AlgebraicType) SchemaError!void {
    switch (t.*) {
        .ref => |idx| {
            if (idx >= typespace.len) return SchemaError.InvalidTypeRef;
            t.* = typespace[idx].*;
            try resolveRefsValue(t, typespace);
        },
        .array => |inner| {
            try resolveRefsInPlace(@constCast(inner), typespace);
        },
        .option => |inner| {
            try resolveRefsInPlace(@constCast(inner), typespace);
        },
        .product => |cols| {
            for (@constCast(cols)) |*col| {
                try resolveRefsValue(&col.type, typespace);
            }
        },
        .sum => |cols| {
            for (@constCast(cols)) |*col| {
                try resolveRefsValue(&col.type, typespace);
            }
        },
        else => {},
    }
}

// ============================================================
// JSON helpers
// ============================================================

fn jsonGet(val: std.json.Value, key: []const u8) ?*std.json.Value {
    return switch (val) {
        .object => |obj| obj.getPtr(key),
        else => null,
    };
}

fn jsonGetString(val: std.json.Value, key: []const u8) ?[]const u8 {
    const v = jsonGet(val, key) orelse return null;
    return switch (v.*) {
        .string => |s| s,
        else => null,
    };
}

fn jsonToU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| if (i >= 0 and i <= std.math.maxInt(u32)) @intCast(i) else null,
        else => null,
    };
}

/// Unwrap SpacetimeDB's option encoding: {"some": "val"} or {"none": ...}
fn unwrapOption(val: std.json.Value) ?[]const u8 {
    if (jsonGet(val, "some")) |v| {
        return switch (v.*) {
            .string => |s| s,
            else => null,
        };
    }
    return null;
}

// ============================================================
// Tests — mirroring Elixir schema_test.exs
// ============================================================

const raw_schema =
    \\{
    \\  "typespace": {
    \\    "types": [
    \\      {
    \\        "Product": {
    \\          "elements": [
    \\            {"name": {"some": "id"}, "algebraic_type": {"U64": []}},
    \\            {"name": {"some": "name"}, "algebraic_type": {"String": []}},
    \\            {"name": {"some": "age"}, "algebraic_type": {"U32": []}}
    \\          ]
    \\        }
    \\      }
    \\    ]
    \\  },
    \\  "tables": [
    \\    {
    \\      "name": "person",
    \\      "product_type_ref": 0,
    \\      "primary_key": [0]
    \\    }
    \\  ],
    \\  "reducers": [
    \\    {
    \\      "name": "add_person",
    \\      "params": {
    \\        "elements": [
    \\          {"name": {"some": "name"}, "algebraic_type": {"String": []}},
    \\          {"name": {"some": "age"}, "algebraic_type": {"U32": []}}
    \\        ]
    \\      }
    \\    },
    \\    {
    \\      "name": "say_hello",
    \\      "params": {"elements": []}
    \\    }
    \\  ]
    \\}
;

test "parse tables with columns and primary key" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const schema = try parse(allocator, raw_schema);

    const person = schema.getTable("person") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("person", person.name);
    try std.testing.expectEqual(@as(usize, 3), person.columns.len);
    try std.testing.expectEqual(@as(usize, 1), person.primary_key.len);
    try std.testing.expectEqual(@as(u32, 0), person.primary_key[0]);

    try std.testing.expectEqualStrings("id", person.columns[0].name.?);
    try std.testing.expect(person.columns[0].type == .u64);
    try std.testing.expectEqualStrings("name", person.columns[1].name.?);
    try std.testing.expect(person.columns[1].type == .string);
    try std.testing.expectEqualStrings("age", person.columns[2].name.?);
    try std.testing.expect(person.columns[2].type == .u32);
}

test "parse reducers with params" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const schema = try parse(allocator, raw_schema);

    const add_person = schema.getReducer("add_person") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 2), add_person.params.len);
    try std.testing.expectEqualStrings("name", add_person.params[0].name.?);
    try std.testing.expect(add_person.params[0].type == .string);
    try std.testing.expectEqualStrings("age", add_person.params[1].name.?);
    try std.testing.expect(add_person.params[1].type == .u32);

    const say_hello = schema.getReducer("say_hello") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 0), say_hello.params.len);
}

test "columnsFor and primaryKeyFor" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const schema = try parse(allocator, raw_schema);

    const cols = schema.columnsFor("person") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 3), cols.len);

    const pk = schema.primaryKeyFor("person") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), pk.len);
    try std.testing.expectEqual(@as(u32, 0), pk[0]);

    // Unknown table
    try std.testing.expect(schema.columnsFor("nope") == null);
}

test "ref type resolution" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const json =
        \\{
        \\  "typespace": {
        \\    "types": [
        \\      {
        \\        "Product": {
        \\          "elements": [
        \\            {"name": {"some": "x"}, "algebraic_type": {"U32": {}}},
        \\            {"name": {"some": "y"}, "algebraic_type": {"U32": {}}}
        \\          ]
        \\        }
        \\      },
        \\      {
        \\        "Product": {
        \\          "elements": [
        \\            {"name": {"some": "id"}, "algebraic_type": {"U64": {}}},
        \\            {"name": {"some": "coords"}, "algebraic_type": {"Ref": 0}}
        \\          ]
        \\        }
        \\      }
        \\    ]
        \\  },
        \\  "tables": [
        \\    {"name": "points", "product_type_ref": 0, "primary_key": [0]},
        \\    {"name": "objects", "product_type_ref": 1, "primary_key": [0]}
        \\  ],
        \\  "reducers": []
        \\}
    ;

    const schema = try parse(allocator, json);

    // Points table — plain u32 columns
    const point_cols = schema.columnsFor("points") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 2), point_cols.len);
    try std.testing.expect(point_cols[0].type == .u32);
    try std.testing.expect(point_cols[1].type == .u32);

    // Objects table — coords column should be resolved product, not ref
    const obj_cols = schema.columnsFor("objects") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("id", obj_cols[0].name.?);
    try std.testing.expect(obj_cols[0].type == .u64);

    try std.testing.expectEqualStrings("coords", obj_cols[1].name.?);
    // Should be a product type (resolved from ref 0), not a ref
    switch (obj_cols[1].type) {
        .product => |cols| {
            try std.testing.expectEqual(@as(usize, 2), cols.len);
            try std.testing.expectEqualStrings("x", cols[0].name.?);
            try std.testing.expect(cols[0].type == .u32);
            try std.testing.expectEqualStrings("y", cols[1].name.?);
            try std.testing.expect(cols[1].type == .u32);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "parse all primitive types" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const primitives = [_]struct { json_key: []const u8, expected: std.meta.Tag(AlgebraicType) }{
        .{ .json_key = "Bool", .expected = .bool },
        .{ .json_key = "U8", .expected = .u8 },
        .{ .json_key = "I8", .expected = .i8 },
        .{ .json_key = "U16", .expected = .u16 },
        .{ .json_key = "I16", .expected = .i16 },
        .{ .json_key = "U32", .expected = .u32 },
        .{ .json_key = "I32", .expected = .i32 },
        .{ .json_key = "U64", .expected = .u64 },
        .{ .json_key = "I64", .expected = .i64 },
        .{ .json_key = "U128", .expected = .u128 },
        .{ .json_key = "I128", .expected = .i128 },
        .{ .json_key = "U256", .expected = .u256 },
        .{ .json_key = "I256", .expected = .i256 },
        .{ .json_key = "F32", .expected = .f32 },
        .{ .json_key = "F64", .expected = .f64 },
        .{ .json_key = "String", .expected = .string },
        .{ .json_key = "Bytes", .expected = .bytes },
    };

    for (primitives) |p| {
        const json = try std.fmt.allocPrint(allocator,
            \\{{"typespace":{{"types":[{{"Product":{{"elements":[{{"name":{{"some":"x"}},"algebraic_type":{{"{s}":[]}}}}]}}}}]}},"tables":[{{"name":"t","product_type_ref":0,"primary_key":[0]}}],"reducers":[]}}
        , .{p.json_key});

        const s = try parse(allocator, json);
        const cols = s.columnsFor("t") orelse return error.TestUnexpectedResult;
        try std.testing.expect(cols[0].type == p.expected);
    }
}
