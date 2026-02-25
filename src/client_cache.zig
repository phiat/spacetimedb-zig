// Client Cache (In-Memory Table Store)
//
// Stores decoded rows per table, keyed by primary key.
// Applies inserts/deletes from SubscribeApplied and TransactionUpdate messages.
// Detects updates (delete+insert with same PK) and fires callbacks.

const std = @import("std");
const types = @import("types.zig");
const protocol = @import("protocol.zig");
const schema_mod = @import("schema.zig");
const row_decoder = @import("row_decoder.zig");

const AlgebraicValue = types.AlgebraicValue;
const Column = types.Column;
const Row = row_decoder.Row;
const Schema = schema_mod.Schema;
const TableRows = protocol.TableRows;
const TableUpdate = protocol.TableUpdate;
const TableUpdateRows = protocol.TableUpdateRows;
const QuerySetUpdate = protocol.QuerySetUpdate;
const BsatnRowList = protocol.BsatnRowList;

/// A primary key: either a single value hash or composite key hash.
/// We store rows keyed by the BSATN bytes of their PK columns for identity.
pub const PrimaryKey = struct {
    /// Raw BSATN bytes of the primary key column(s), owned by the cache.
    bytes: []const u8,

    pub fn eql(a: PrimaryKey, b: PrimaryKey) bool {
        return std.mem.eql(u8, a.bytes, b.bytes);
    }

    pub fn hash(self: PrimaryKey) u64 {
        return std.hash.Wyhash.hash(0, self.bytes);
    }
};

const PkContext = struct {
    pub fn hash(_: PkContext, key: PrimaryKey) u64 {
        return key.hash();
    }

    pub fn eql(_: PkContext, a: PrimaryKey, b: PrimaryKey) bool {
        return a.eql(b);
    }
};

const RowMap = std.HashMapUnmanaged(PrimaryKey, Row, PkContext, 80);

/// Change type for row-level callbacks.
pub const RowChange = union(enum) {
    insert: struct {
        table_name: []const u8,
        row: *const Row,
    },
    delete: struct {
        table_name: []const u8,
        /// Heap-allocated copy — owned by the changes list, freed by freeChanges.
        row: *const Row,
    },
    update: struct {
        table_name: []const u8,
        /// Heap-allocated copy — owned by the changes list, freed by freeChanges.
        old_row: *const Row,
        new_row: *const Row,
    },

    /// Free a changes list returned by applySubscribeApplied/applyTransactionUpdate.
    /// This frees heap-allocated old_row (updates) and row (deletes) copies.
    /// Insert rows point into the cache and are NOT freed here.
    pub fn freeChanges(allocator: std.mem.Allocator, changes: []const RowChange) void {
        for (changes) |change| {
            switch (change) {
                .delete => |d| {
                    @constCast(d.row).deinit(allocator);
                    allocator.destroy(@constCast(d.row));
                },
                .update => |u| {
                    @constCast(u.old_row).deinit(allocator);
                    allocator.destroy(@constCast(u.old_row));
                },
                .insert => {},
            }
        }
        allocator.free(changes);
    }
};

/// Per-table storage.
const TableStore = struct {
    rows: RowMap,
    pk_col_indices: []const u32,

    fn init() TableStore {
        return .{
            .rows = .{},
            .pk_col_indices = &.{},
        };
    }

    fn deinit(self: *TableStore, allocator: std.mem.Allocator) void {
        var it = self.rows.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.bytes);
            entry.value_ptr.deinit(allocator);
        }
        self.rows.deinit(allocator);
    }

    fn count(self: *const TableStore) usize {
        return self.rows.count();
    }
};

pub const ClientCache = struct {
    allocator: std.mem.Allocator,
    tables: std.StringHashMapUnmanaged(TableStore),
    table_schema: schema_mod.Schema,

    pub fn init(allocator: std.mem.Allocator, s: Schema) ClientCache {
        return .{
            .allocator = allocator,
            .tables = .{},
            .table_schema = s,
        };
    }

    pub fn deinit(self: *ClientCache) void {
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
        }
        self.tables.deinit(self.allocator);
    }

    /// Get a table store, creating it if needed.
    fn getOrCreateTable(self: *ClientCache, table_name: []const u8) !*TableStore {
        const gop = try self.tables.getOrPut(self.allocator, table_name);
        if (!gop.found_existing) {
            gop.value_ptr.* = TableStore.init();
            // Resolve PK indices from schema
            if (self.table_schema.primaryKeyFor(table_name)) |pk_indices| {
                gop.value_ptr.pk_col_indices = pk_indices;
            }
        }
        return gop.value_ptr;
    }

    /// Get a row by table name and raw primary key bytes.
    pub fn getRow(self: *ClientCache, table_name: []const u8, pk_bytes: []const u8) ?*const Row {
        const store = self.tables.get(table_name) orelse return null;
        const key = PrimaryKey{ .bytes = pk_bytes };
        return store.rows.getPtr(key);
    }

    /// Find a row by primary key value(s).
    /// For single-column PK: pass the value directly (e.g., .{ .u64 = 42 }).
    /// For composite PK: pass a product with the PK fields in order.
    /// Encodes the value to BSATN internally and looks it up.
    pub fn find(self: *ClientCache, table_name: []const u8, pk_value: AlgebraicValue) !?*const Row {
        const bsatn_mod = @import("bsatn.zig");
        var enc = bsatn_mod.Encoder.init();
        defer enc.deinit(self.allocator);
        try enc.encodeValue(self.allocator, pk_value);
        const pk_bytes = enc.writtenSlice();

        const store = self.tables.get(table_name) orelse return null;
        const key = PrimaryKey{ .bytes = pk_bytes };
        return store.rows.getPtr(key);
    }

    /// Get all rows in a table. Caller must free the returned slice.
    pub fn getTableRows(self: *ClientCache, table_name: []const u8) ![]const Row {
        const store = self.tables.get(table_name) orelse return &.{};
        const result = try self.allocator.alloc(Row, store.rows.count());
        var it = store.rows.valueIterator();
        var i: usize = 0;
        while (it.next()) |row| {
            result[i] = row.*;
            i += 1;
        }
        return result;
    }

    /// Get all rows in a table as typed structs.
    /// String fields borrow from cache — valid until next cache mutation.
    /// Caller must free the returned slice with `allocator.free(result)`.
    pub fn getTyped(self: *ClientCache, comptime T: type, table_name: []const u8) ![]T {
        const table_mod = @import("table.zig");
        const store = self.tables.get(table_name) orelse return try self.allocator.alloc(T, 0);
        const result = try self.allocator.alloc(T, store.rows.count());
        var decoded: usize = 0;
        errdefer self.allocator.free(result);
        var it = store.rows.valueIterator();
        while (it.next()) |row| {
            result[decoded] = try table_mod.fromRow(T, row);
            decoded += 1;
        }
        return result;
    }

    /// Find a row by primary key value and return it as a typed struct.
    /// String fields borrow from cache — valid until next cache mutation.
    pub fn findTyped(self: *ClientCache, comptime T: type, table_name: []const u8, pk_value: AlgebraicValue) !?T {
        const table_mod = @import("table.zig");
        const row = try self.find(table_name, pk_value) orelse return null;
        return try table_mod.fromRow(T, row);
    }

    /// Get number of rows in a table.
    pub fn tableRowCount(self: *ClientCache, table_name: []const u8) usize {
        const store = self.tables.get(table_name) orelse return 0;
        return store.count();
    }

    /// Apply initial rows from SubscribeApplied.
    /// Returns list of changes for callbacks.
    pub fn applySubscribeApplied(
        self: *ClientCache,
        table_rows_list: []const TableRows,
    ) ![]RowChange {
        var changes = std.ArrayListUnmanaged(RowChange){};
        errdefer changes.deinit(self.allocator);

        for (table_rows_list) |tr| {
            const columns = self.table_schema.columnsFor(tr.table_name) orelse continue;
            const decoded = try row_decoder.decodeRowList(self.allocator, tr.rows, columns);
            defer self.allocator.free(decoded);

            const store = try self.getOrCreateTable(tr.table_name);
            var stored_count: usize = 0;
            errdefer {
                // Free rows that weren't stored in the cache
                for (decoded[stored_count..]) |*r| {
                    @constCast(r).deinit(self.allocator);
                }
            }
            for (decoded) |row| {
                const pk = try self.extractPk(row, store.pk_col_indices);
                try store.rows.put(self.allocator, pk, row);
                stored_count += 1;
                // Get pointer to stored row for callback
                const stored = store.rows.getPtr(pk).?;
                try changes.append(self.allocator, .{ .insert = .{
                    .table_name = tr.table_name,
                    .row = stored,
                } });
            }
        }

        return changes.toOwnedSlice(self.allocator);
    }

    /// Apply a TransactionUpdate. Detects updates (delete+insert same PK).
    /// Returns list of changes for callbacks.
    pub fn applyTransactionUpdate(
        self: *ClientCache,
        query_sets: []const QuerySetUpdate,
    ) ![]RowChange {
        var changes = std.ArrayListUnmanaged(RowChange){};
        errdefer {
            // Free heap-allocated row copies in delete/update changes
            for (changes.items) |change| {
                switch (change) {
                    .delete => |d| {
                        @constCast(d.row).deinit(self.allocator);
                        self.allocator.destroy(@constCast(d.row));
                    },
                    .update => |u| {
                        @constCast(u.old_row).deinit(self.allocator);
                        self.allocator.destroy(@constCast(u.old_row));
                    },
                    .insert => {},
                }
            }
            changes.deinit(self.allocator);
        }

        for (query_sets) |qs| {
            for (qs.tables) |table_update| {
                try self.applyTableUpdate(table_update, &changes);
            }
        }

        return changes.toOwnedSlice(self.allocator);
    }

    fn applyTableUpdate(
        self: *ClientCache,
        update: TableUpdate,
        changes: *std.ArrayListUnmanaged(RowChange),
    ) !void {
        const columns = self.table_schema.columnsFor(update.table_name) orelse return;
        const store = try self.getOrCreateTable(update.table_name);

        for (update.rows) |row_update| {
            switch (row_update) {
                .persistent => |p| {
                    // Process deletes first, collecting deleted rows for O(1) update detection
                    var deleted_map = RowMap{};
                    defer {
                        // Free any remaining (unmatched) entries
                        var dit = deleted_map.iterator();
                        while (dit.next()) |entry| {
                            self.allocator.free(entry.key_ptr.bytes);
                            entry.value_ptr.deinit(self.allocator);
                        }
                        deleted_map.deinit(self.allocator);
                    }

                    const del_decoded = try row_decoder.decodeRowList(self.allocator, p.deletes, columns);
                    defer self.allocator.free(del_decoded);
                    var del_processed: usize = 0;
                    errdefer for (del_decoded[del_processed..]) |*r| r.deinit(self.allocator);
                    for (del_decoded) |*row_ptr| {
                        const row = row_ptr.*;
                        const pk = try self.extractPk(row, store.pk_col_indices);
                        del_processed += 1;
                        if (store.rows.fetchRemove(pk)) |kv| {
                            // Free the original PK from the store — we use our new pk for deleted_map
                            self.allocator.free(kv.key.bytes);
                            try deleted_map.put(self.allocator, pk, kv.value);
                        } else {
                            self.allocator.free(pk.bytes);
                        }
                        // The decoded delete row was only needed for PK extraction.
                        // Free it now — the actual row data lives in the cache/deleted_map.
                        @constCast(row_ptr).deinit(self.allocator);
                    }

                    // Process inserts, checking against deleted map for updates (O(1) per lookup)
                    const ins_decoded = try row_decoder.decodeRowList(self.allocator, p.inserts, columns);
                    defer self.allocator.free(ins_decoded);
                    var ins_processed: usize = 0;
                    errdefer for (ins_decoded[ins_processed..]) |*r| r.deinit(self.allocator);
                    for (ins_decoded) |row| {
                        const pk = try self.extractPk(row, store.pk_col_indices);
                        ins_processed += 1;

                        if (deleted_map.fetchRemove(pk)) |old_entry| {
                            // Update: same PK was deleted then re-inserted
                            try store.rows.put(self.allocator, pk, row);
                            const stored = store.rows.getPtr(pk).?;
                            // Store old row for callback, then append change
                            // Old row ownership: we need it alive for the callback.
                            // Allocate a copy since deleted_map entry is now gone.
                            const old_row_ptr = try self.allocator.create(Row);
                            old_row_ptr.* = old_entry.value;
                            try changes.append(self.allocator, .{ .update = .{
                                .table_name = update.table_name,
                                .old_row = old_row_ptr,
                                .new_row = stored,
                            } });
                            self.allocator.free(old_entry.key.bytes);
                        } else {
                            try store.rows.put(self.allocator, pk, row);
                            const stored = store.rows.getPtr(pk).?;
                            try changes.append(self.allocator, .{ .insert = .{
                                .table_name = update.table_name,
                                .row = stored,
                            } });
                        }
                    }

                    // Remaining deletes (not matched by inserts) are pure deletes.
                    // Move ownership from deleted_map to heap-allocated Row copies
                    // so they survive past the defer cleanup. We collect keys to
                    // remove after iteration.
                    {
                        var rem_it = deleted_map.iterator();
                        var keys_to_remove = std.ArrayListUnmanaged(PrimaryKey){};
                        defer keys_to_remove.deinit(self.allocator);
                        while (rem_it.next()) |entry| {
                            const del_row_ptr = try self.allocator.create(Row);
                            del_row_ptr.* = entry.value_ptr.*;
                            try keys_to_remove.append(self.allocator, entry.key_ptr.*);
                            try changes.append(self.allocator, .{ .delete = .{
                                .table_name = update.table_name,
                                .row = del_row_ptr,
                            } });
                        }
                        // Remove transferred entries so defer doesn't double-free.
                        // Free the PK bytes since ownership didn't transfer.
                        for (keys_to_remove.items) |pk| {
                            _ = deleted_map.fetchRemove(pk);
                            self.allocator.free(pk.bytes);
                        }
                    }
                },
                .event => {
                    // Event tables are transient — no cache storage
                },
            }
        }
    }

    /// Extract primary key BSATN bytes from a decoded row.
    fn extractPk(self: *ClientCache, row: Row, pk_indices: []const u32) !PrimaryKey {
        if (pk_indices.len == 0) {
            // No PK defined — use all fields as key
            return self.hashAllFields(row);
        }

        const bsatn_mod = @import("bsatn.zig");
        var enc = bsatn_mod.Encoder.init();
        defer enc.deinit(self.allocator);

        for (pk_indices) |idx| {
            const i: usize = @intCast(idx);
            if (i < row.fields.len) {
                try enc.encodeValue(self.allocator, row.fields[i].value);
            }
        }

        return .{ .bytes = try enc.toOwnedSlice(self.allocator) };
    }

    /// Hash all fields when no PK is defined.
    fn hashAllFields(self: *ClientCache, row: Row) !PrimaryKey {
        const bsatn_mod = @import("bsatn.zig");
        var enc = bsatn_mod.Encoder.init();
        defer enc.deinit(self.allocator);

        for (row.fields) |f| {
            try enc.encodeValue(self.allocator, f.value);
        }

        return .{ .bytes = try enc.toOwnedSlice(self.allocator) };
    }
};

// ============================================================
// Tests
// ============================================================

const bsatn = @import("bsatn.zig");
const Encoder = bsatn.Encoder;

fn makeTestSchema(allocator: std.mem.Allocator) Schema {
    return .{
        .tables = &.{},
        .reducers = &.{},
        .typespace = &.{},
        .allocator = allocator,
    };
}

fn encodeTestRow(allocator: std.mem.Allocator, id: u64, name: []const u8) ![]u8 {
    var enc = Encoder.init();
    defer enc.deinit(allocator);
    try enc.encodeU64(allocator, id);
    try enc.encodeString(allocator, name);
    return enc.toOwnedSlice(allocator);
}

const test_columns = [_]Column{
    .{ .name = "id", .type = .u64 },
    .{ .name = "name", .type = .string },
};

test "cache init and deinit" {
    const allocator = std.testing.allocator;
    var cache = ClientCache.init(allocator, makeTestSchema(allocator));
    defer cache.deinit();
    try std.testing.expectEqual(@as(usize, 0), cache.tables.count());
}

test "cache insert and retrieve" {
    const allocator = std.testing.allocator;
    var cache = ClientCache.init(allocator, makeTestSchema(allocator));
    defer cache.deinit();

    // Encode a row
    const row_data = try encodeTestRow(allocator, 1, "Alice");
    defer allocator.free(row_data);

    // Decode it
    const row = try row_decoder.decodeRow(allocator, row_data, &test_columns);

    // Insert manually into cache
    const store = try cache.getOrCreateTable("users");
    const pk = try cache.extractPk(row, store.pk_col_indices);
    try store.rows.put(allocator, pk, row);

    try std.testing.expectEqual(@as(usize, 1), cache.tableRowCount("users"));
}

test "cache apply inserts via subscribe" {
    const allocator = std.testing.allocator;

    // Build raw row data: two rows concatenated
    const row1 = try encodeTestRow(allocator, 1, "Alice");
    defer allocator.free(row1);
    const row2 = try encodeTestRow(allocator, 2, "Bob");
    defer allocator.free(row2);

    const all_data = try std.mem.concat(allocator, u8, &.{ row1, row2 });
    defer allocator.free(all_data);

    const offset_vals = [_]u64{ 0, row1.len };
    var offset_bytes: [2 * 8]u8 = undefined;
    for (offset_vals, 0..) |val, oi| {
        std.mem.writeInt(u64, offset_bytes[oi * 8 ..][0..8], val, .little);
    }
    const table_rows = [_]TableRows{.{
        .table_name = "users",
        .rows = .{
            .size_hint = .{ .row_offsets = .{
                .count = 2,
                .raw_bytes = &offset_bytes,
            } },
            .rows_data = all_data,
        },
    }};

    // We need a schema that knows about "users" columns
    // For this test, use a minimal schema with columnsFor override
    // Since Schema.columnsFor relies on schema.tables, we construct one
    const tables = [_]schema_mod.TableDef{.{
        .name = "users",
        .columns = &test_columns,
        .primary_key = &[_]u32{0},
    }};
    const test_schema = Schema{
        .tables = &tables,
        .reducers = &.{},
        .typespace = &.{},
        .allocator = allocator,
    };

    var cache = ClientCache.init(allocator, test_schema);
    defer cache.deinit();

    const changes = try cache.applySubscribeApplied(&table_rows);
    defer RowChange.freeChanges(allocator, changes);

    try std.testing.expectEqual(@as(usize, 2), changes.len);
    try std.testing.expectEqual(@as(usize, 2), cache.tableRowCount("users"));

    // Verify both are inserts
    try std.testing.expect(changes[0] == .insert);
    try std.testing.expect(changes[1] == .insert);
}

test "cache table row count for missing table" {
    const allocator = std.testing.allocator;
    var cache = ClientCache.init(allocator, makeTestSchema(allocator));
    defer cache.deinit();
    try std.testing.expectEqual(@as(usize, 0), cache.tableRowCount("nonexistent"));
}

test "PrimaryKey equality" {
    const pk1 = PrimaryKey{ .bytes = &[_]u8{ 1, 0, 0, 0 } };
    const pk2 = PrimaryKey{ .bytes = &[_]u8{ 1, 0, 0, 0 } };
    const pk3 = PrimaryKey{ .bytes = &[_]u8{ 2, 0, 0, 0 } };
    try std.testing.expect(pk1.eql(pk2));
    try std.testing.expect(!pk1.eql(pk3));
}

test "find by primary key" {
    const allocator = std.testing.allocator;

    // Schema with PK on column 0 (id)
    const tables = [_]schema_mod.TableDef{.{
        .name = "users",
        .columns = &test_columns,
        .primary_key = &[_]u32{0},
    }};
    const test_schema = Schema{
        .tables = &tables,
        .reducers = &.{},
        .typespace = &.{},
        .allocator = allocator,
    };

    var cache = ClientCache.init(allocator, test_schema);
    defer cache.deinit();

    // Insert two rows via subscribe
    const row1 = try encodeTestRow(allocator, 42, "Alice");
    defer allocator.free(row1);
    const row2 = try encodeTestRow(allocator, 99, "Bob");
    defer allocator.free(row2);

    const all_data = try std.mem.concat(allocator, u8, &.{ row1, row2 });
    defer allocator.free(all_data);

    const offset_vals = [_]u64{ 0, row1.len };
    var offset_bytes: [2 * 8]u8 = undefined;
    for (offset_vals, 0..) |val, oi| {
        std.mem.writeInt(u64, offset_bytes[oi * 8 ..][0..8], val, .little);
    }
    const table_rows = [_]TableRows{.{
        .table_name = "users",
        .rows = .{
            .size_hint = .{ .row_offsets = .{
                .count = 2,
                .raw_bytes = &offset_bytes,
            } },
            .rows_data = all_data,
        },
    }};

    const changes = try cache.applySubscribeApplied(&table_rows);
    defer RowChange.freeChanges(allocator, changes);

    // Find by PK value
    const found = try cache.find("users", .{ .u64 = 42 });
    try std.testing.expect(found != null);
    try std.testing.expectEqual(@as(u64, 42), found.?.fields[0].value.u64);

    const found2 = try cache.find("users", .{ .u64 = 99 });
    try std.testing.expect(found2 != null);
    try std.testing.expectEqualStrings("Bob", found2.?.fields[1].value.string);

    // Not found
    const missing = try cache.find("users", .{ .u64 = 999 });
    try std.testing.expect(missing == null);
}

test "find returns null for missing table" {
    const allocator = std.testing.allocator;
    var cache = ClientCache.init(allocator, makeTestSchema(allocator));
    defer cache.deinit();

    const result = try cache.find("nonexistent", .{ .u64 = 1 });
    try std.testing.expect(result == null);
}

test "getTyped returns typed structs" {
    const allocator = std.testing.allocator;

    const User = struct { id: u64, name: []const u8 };

    const tables = [_]schema_mod.TableDef{.{
        .name = "users",
        .columns = &test_columns,
        .primary_key = &[_]u32{0},
    }};
    const test_schema = Schema{
        .tables = &tables,
        .reducers = &.{},
        .typespace = &.{},
        .allocator = allocator,
    };

    var cache = ClientCache.init(allocator, test_schema);
    defer cache.deinit();

    // Insert rows
    const row1 = try encodeTestRow(allocator, 1, "Alice");
    defer allocator.free(row1);
    const row2 = try encodeTestRow(allocator, 2, "Bob");
    defer allocator.free(row2);

    const all_data = try std.mem.concat(allocator, u8, &.{ row1, row2 });
    defer allocator.free(all_data);

    const offset_vals = [_]u64{ 0, row1.len };
    var offset_bytes: [2 * 8]u8 = undefined;
    for (offset_vals, 0..) |val, oi| {
        std.mem.writeInt(u64, offset_bytes[oi * 8 ..][0..8], val, .little);
    }
    const table_rows = [_]TableRows{.{
        .table_name = "users",
        .rows = .{
            .size_hint = .{ .row_offsets = .{ .count = 2, .raw_bytes = &offset_bytes } },
            .rows_data = all_data,
        },
    }};

    const changes = try cache.applySubscribeApplied(&table_rows);
    defer RowChange.freeChanges(allocator, changes);

    // Get typed — borrows from cache, just free the slice
    const users = try cache.getTyped(User, "users");
    defer allocator.free(users);

    try std.testing.expectEqual(@as(usize, 2), users.len);

    // Find typed
    const alice = try cache.findTyped(User, "users", .{ .u64 = 1 });
    try std.testing.expect(alice != null);
    try std.testing.expectEqual(@as(u64, 1), alice.?.id);
}

test "cache transaction update with delete and update changes" {
    const allocator = std.testing.allocator;

    const tables = [_]schema_mod.TableDef{.{
        .name = "users",
        .columns = &test_columns,
        .primary_key = &[_]u32{0},
    }};
    const test_schema = Schema{
        .tables = &tables,
        .reducers = &.{},
        .typespace = &.{},
        .allocator = allocator,
    };

    var cache = ClientCache.init(allocator, test_schema);
    defer cache.deinit();

    // First, insert two rows via subscribe
    const row1 = try encodeTestRow(allocator, 1, "Alice");
    defer allocator.free(row1);
    const row2 = try encodeTestRow(allocator, 2, "Bob");
    defer allocator.free(row2);

    const all_data = try std.mem.concat(allocator, u8, &.{ row1, row2 });
    defer allocator.free(all_data);

    const offset_vals = [_]u64{ 0, row1.len };
    var offset_bytes: [2 * 8]u8 = undefined;
    for (offset_vals, 0..) |val, oi| {
        std.mem.writeInt(u64, offset_bytes[oi * 8 ..][0..8], val, .little);
    }
    const init_rows = [_]TableRows{.{
        .table_name = "users",
        .rows = .{
            .size_hint = .{ .row_offsets = .{ .count = 2, .raw_bytes = &offset_bytes } },
            .rows_data = all_data,
        },
    }};

    const init_changes = try cache.applySubscribeApplied(&init_rows);
    defer RowChange.freeChanges(allocator, init_changes);

    try std.testing.expectEqual(@as(usize, 2), cache.tableRowCount("users"));

    // Now apply a transaction that:
    // - Deletes Bob (id=2)
    // - Updates Alice (id=1 -> name changes to "Alicia")
    const del_row2 = try encodeTestRow(allocator, 2, "Bob");
    defer allocator.free(del_row2);
    const del_row1 = try encodeTestRow(allocator, 1, "Alice");
    defer allocator.free(del_row1);
    const ins_row1 = try encodeTestRow(allocator, 1, "Alicia");
    defer allocator.free(ins_row1);

    // Deletes: row2 (Bob) + row1 (Alice)
    const del_data = try std.mem.concat(allocator, u8, &.{ del_row2, del_row1 });
    defer allocator.free(del_data);
    const del_offsets = [_]u64{ 0, del_row2.len };
    var del_off_bytes: [2 * 8]u8 = undefined;
    for (del_offsets, 0..) |val, oi| {
        std.mem.writeInt(u64, del_off_bytes[oi * 8 ..][0..8], val, .little);
    }

    // Inserts: row1 with new name (Alicia) — only Alice gets re-inserted
    const ins_data = ins_row1;
    const ins_offsets = [_]u64{0};
    var ins_off_bytes: [1 * 8]u8 = undefined;
    std.mem.writeInt(u64, ins_off_bytes[0..8], ins_offsets[0], .little);

    const update_rows = [_]TableUpdateRows{.{ .persistent = .{
        .deletes = .{
            .size_hint = .{ .row_offsets = .{ .count = 2, .raw_bytes = &del_off_bytes } },
            .rows_data = del_data,
        },
        .inserts = .{
            .size_hint = .{ .row_offsets = .{ .count = 1, .raw_bytes = &ins_off_bytes } },
            .rows_data = ins_data,
        },
    } }};
    const table_updates = [_]TableUpdate{.{
        .table_name = "users",
        .rows = &update_rows,
    }};
    const query_set_updates = [_]QuerySetUpdate{.{
        .query_set_id = 1,
        .tables = &table_updates,
    }};

    const tx_changes = try cache.applyTransactionUpdate(&query_set_updates);
    defer RowChange.freeChanges(allocator, tx_changes);

    // Should have 2 changes: one update (Alice -> Alicia) and one delete (Bob)
    try std.testing.expectEqual(@as(usize, 2), tx_changes.len);

    // Verify we have one update and one delete
    var updates_count: usize = 0;
    var deletes_count: usize = 0;
    for (tx_changes) |change| {
        switch (change) {
            .update => updates_count += 1,
            .delete => deletes_count += 1,
            .insert => {},
        }
    }
    try std.testing.expectEqual(@as(usize, 1), updates_count);
    try std.testing.expectEqual(@as(usize, 1), deletes_count);

    // Cache should now have 1 row (Alicia)
    try std.testing.expectEqual(@as(usize, 1), cache.tableRowCount("users"));
}
