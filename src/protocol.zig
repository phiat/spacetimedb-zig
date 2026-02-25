// SpacetimeDB v2 Binary Protocol Messages
//
// Client messages: raw BSATN (no compression envelope)
// Server messages: 1-byte compression envelope + BSATN
//
// Client message tags: Subscribe(0), Unsubscribe(1), OneOffQuery(2),
//   CallReducer(3), CallProcedure(4)
// Server message tags: InitialConnection(0), SubscribeApplied(1),
//   UnsubscribeApplied(2), SubscriptionError(3), TransactionUpdate(4),
//   OneOffQueryResult(5), ReducerResult(6), ProcedureResult(7)

const std = @import("std");
const flate = std.compress.flate;
const build_options = @import("build_options");
const bsatn = @import("bsatn.zig");
const types = @import("types.zig");

const brotli = if (build_options.enable_brotli) @cImport(@cInclude("brotli/decode.h")) else struct {};

const Encoder = bsatn.Encoder;
const Decoder = bsatn.Decoder;

pub const Error = error{
    UnknownCompression,
    EmptyFrame,
    DecompressionFailed,
    UnknownServerMessageTag,
    UnknownReducerOutcomeTag,
    UnknownOneOffResultTag,
    UnknownProcedureStatusTag,
    UnknownRowSizeHintTag,
    UnknownTableUpdateRowsTag,
    InvalidOptionTag,
} || bsatn.Error || std.mem.Allocator.Error;

// ============================================================
// Compression
// ============================================================

pub const Compression = enum(u8) {
    none = 0x00,
    brotli = 0x01,
    gzip = 0x02,
};

/// Decompress a server frame (strip 1-byte compression envelope).
/// Returns the decompressed payload. Caller owns returned memory for gzip.
pub fn decompress(allocator: std.mem.Allocator, frame: []const u8) Error!DecompressResult {
    if (frame.len == 0) return Error.EmptyFrame;
    const tag = frame[0];
    const payload = frame[1..];
    return switch (tag) {
        0x00 => .{ .data = payload, .allocated = false },
        0x02 => {
            // Gzip decompression via std.compress.flate
            var reader: std.Io.Reader = .fixed(payload);
            var writer: std.Io.Writer.Allocating = .init(allocator);
            errdefer writer.deinit();

            var decomp: flate.Decompress = .init(&reader, .gzip, &.{});
            _ = decomp.reader.streamRemaining(&writer.writer) catch
                return Error.DecompressionFailed;

            const decompressed = writer.toOwnedSlice() catch
                return Error.DecompressionFailed;
            return .{ .data = decompressed, .allocated = true };
        },
        0x01 => {
            if (comptime build_options.enable_brotli) {
                return decompressBrotli(allocator, payload);
            } else {
                return Error.DecompressionFailed; // Brotli not enabled — build with -Denable-brotli=true
            }
        },
        else => Error.UnknownCompression,
    };
}

/// Decompress brotli data using libbrotlidec.
/// Only available when built with -Denable-brotli=true.
fn decompressBrotli(allocator: std.mem.Allocator, payload: []const u8) Error!DecompressResult {
    if (comptime !build_options.enable_brotli) unreachable;

    const state = brotli.BrotliDecoderCreateInstance(null, null, null) orelse
        return Error.DecompressionFailed;
    defer brotli.BrotliDecoderDestroyInstance(state);

    var output = std.ArrayListUnmanaged(u8){};
    errdefer output.deinit(allocator);

    var avail_in: usize = payload.len;
    var next_in: [*c]const u8 = payload.ptr;

    while (true) {
        // Grow output buffer in chunks
        const chunk_size: usize = if (output.items.len == 0)
            @max(payload.len * 4, 4096)
        else
            output.items.len;

        output.ensureUnusedCapacity(allocator, chunk_size) catch
            return Error.DecompressionFailed;

        var avail_out: usize = output.capacity - output.items.len;
        var next_out: [*c]u8 = output.items.ptr + output.items.len;

        const result = brotli.BrotliDecoderDecompressStream(
            state,
            &avail_in,
            &next_in,
            &avail_out,
            &next_out,
            null,
        );

        // Update length based on how much was written
        output.items.len = @intFromPtr(next_out) - @intFromPtr(output.items.ptr);

        switch (result) {
            brotli.BROTLI_DECODER_RESULT_SUCCESS => {
                const decompressed = output.toOwnedSlice(allocator) catch
                    return Error.DecompressionFailed;
                return .{ .data = decompressed, .allocated = true };
            },
            brotli.BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT => continue,
            else => return Error.DecompressionFailed,
        }
    }
}

pub const DecompressResult = struct {
    data: []const u8,
    allocated: bool,

    pub fn deinit(self: *const DecompressResult, allocator: std.mem.Allocator) void {
        if (self.allocated) {
            allocator.free(@constCast(self.data));
        }
    }
};

// ============================================================
// Client Messages
// ============================================================

pub const UnsubscribeFlags = enum(u8) {
    default = 0,
    send_dropped_rows = 1,
};

pub const ClientMessage = union(enum) {
    subscribe: Subscribe,
    unsubscribe: Unsubscribe,
    one_off_query: OneOffQuery,
    call_reducer: CallReducer,
    call_procedure: CallProcedure,

    pub const Subscribe = struct {
        request_id: u32,
        query_set_id: u32,
        query_strings: []const []const u8,
    };

    pub const Unsubscribe = struct {
        request_id: u32,
        query_set_id: u32,
        flags: UnsubscribeFlags = .default,
    };

    pub const OneOffQuery = struct {
        request_id: u32,
        query_string: []const u8,
    };

    pub const CallReducer = struct {
        request_id: u32,
        reducer: []const u8,
        args: []const u8, // Pre-encoded BSATN
    };

    pub const CallProcedure = struct {
        request_id: u32,
        procedure: []const u8,
        args: []const u8,
    };

    /// Encode a client message to raw BSATN binary.
    pub fn encode(self: ClientMessage, allocator: std.mem.Allocator) ![]u8 {
        var enc = Encoder.init();
        defer enc.deinit(allocator);

        switch (self) {
            .subscribe => |msg| {
                try enc.encodeU8(allocator, 0); // tag
                try enc.encodeU32(allocator, msg.request_id);
                try enc.encodeU32(allocator, msg.query_set_id);
                try enc.encodeU32(allocator, @intCast(msg.query_strings.len));
                for (msg.query_strings) |qs| {
                    try enc.encodeString(allocator, qs);
                }
            },
            .unsubscribe => |msg| {
                try enc.encodeU8(allocator, 1); // tag
                try enc.encodeU32(allocator, msg.request_id);
                try enc.encodeU32(allocator, msg.query_set_id);
                try enc.encodeU8(allocator, @intFromEnum(msg.flags));
            },
            .one_off_query => |msg| {
                try enc.encodeU8(allocator, 2); // tag
                try enc.encodeU32(allocator, msg.request_id);
                try enc.encodeString(allocator, msg.query_string);
            },
            .call_reducer => |msg| {
                try enc.encodeU8(allocator, 3); // tag
                try enc.encodeU32(allocator, msg.request_id);
                try enc.encodeU8(allocator, 0); // flags (always 0)
                try enc.encodeString(allocator, msg.reducer);
                try enc.encodeBytes(allocator, msg.args);
            },
            .call_procedure => |msg| {
                try enc.encodeU8(allocator, 4); // tag
                try enc.encodeU32(allocator, msg.request_id);
                try enc.encodeU8(allocator, 0); // flags
                try enc.encodeString(allocator, msg.procedure);
                try enc.encodeBytes(allocator, msg.args);
            },
        }

        return enc.toOwnedSlice(allocator);
    }
};

// ============================================================
// Server Messages
// ============================================================

/// Row size hint for BsatnRowList.
pub const RowSizeHint = union(enum) {
    fixed_size: u16,
    /// Raw little-endian u64 offset bytes from the frame buffer (no allocation).
    /// Use `getOffset(i)` to read individual offsets.
    row_offsets: RowOffsets,

    pub const RowOffsets = struct {
        count: u32,
        /// Raw bytes: count * 8 bytes of little-endian u64s
        raw_bytes: []const u8,

        pub fn getOffset(self: RowOffsets, i: usize) u64 {
            const start = i * 8;
            return std.mem.readInt(u64, self.raw_bytes[start..][0..8], .little);
        }
    };
};

/// A list of BSATN-encoded rows with size hint.
pub const BsatnRowList = struct {
    size_hint: RowSizeHint,
    rows_data: []const u8,
};

/// A single table's rows in a subscription or query result.
pub const TableRows = struct {
    table_name: []const u8,
    rows: BsatnRowList,
};

/// Update type for persistent or event tables in a transaction.
pub const TableUpdateRows = union(enum) {
    persistent: struct {
        inserts: BsatnRowList,
        deletes: BsatnRowList,
    },
    event: BsatnRowList,
};

/// Per-table changes in a transaction.
pub const TableUpdate = struct {
    table_name: []const u8,
    rows: []const TableUpdateRows,
};

/// Per-query-set changes in a transaction.
pub const QuerySetUpdate = struct {
    query_set_id: u32,
    tables: []const TableUpdate,
};

/// Result of a reducer invocation.
pub const ReducerOutcome = union(enum) {
    ok: struct {
        return_value: []const u8,
        transaction: []const QuerySetUpdate,
    },
    ok_empty,
    err: []const u8,
    internal_error: []const u8,
};

/// Result of a one-off query.
pub const OneOffResult = union(enum) {
    ok: []const TableRows,
    err: []const u8,
};

/// Status of a procedure call.
pub const ProcedureStatus = union(enum) {
    returned: []const u8,
    internal_error: []const u8,
};

pub const ServerMessage = union(enum) {
    initial_connection: struct {
        identity: types.Identity,
        connection_id: types.ConnectionId,
        token: []const u8,
    },
    subscribe_applied: struct {
        request_id: u32,
        query_set_id: u32,
        rows: []const TableRows,
    },
    unsubscribe_applied: struct {
        request_id: u32,
        query_set_id: u32,
        rows: ?[]const TableRows,
    },
    subscription_error: struct {
        request_id: ?u32,
        query_set_id: u32,
        @"error": []const u8,
    },
    transaction_update: struct {
        query_sets: []const QuerySetUpdate,
    },
    one_off_query_result: struct {
        request_id: u32,
        result: OneOffResult,
    },
    reducer_result: struct {
        request_id: u32,
        timestamp: i64,
        result: ReducerOutcome,
    },
    procedure_result: struct {
        status: ProcedureStatus,
        timestamp: i64,
        total_host_execution_duration: i64,
        request_id: u32,
    },

    /// Decode a decompressed BSATN server message.
    /// All returned slices point into the `data` buffer — no heap allocation
    /// except for arrays of structs.
    pub fn decode(allocator: std.mem.Allocator, data: []const u8) Error!ServerMessage {
        var dec = Decoder.init(data);
        const tag = try dec.decodeU8();

        return switch (tag) {
            0 => decodeInitialConnection(&dec),
            1 => decodeSubscribeApplied(allocator, &dec),
            2 => decodeUnsubscribeApplied(allocator, &dec),
            3 => decodeSubscriptionError(&dec),
            4 => decodeTransactionUpdate(allocator, &dec),
            5 => decodeOneOffQueryResult(allocator, &dec),
            6 => decodeReducerResult(allocator, &dec),
            7 => decodeProcedureResult(&dec),
            else => Error.UnknownServerMessageTag,
        };
    }

    fn decodeInitialConnection(dec: *Decoder) Error!ServerMessage {
        const identity_bytes = try dec.readBytes(32);
        const conn_bytes = try dec.readBytes(16);
        const token = try dec.decodeString();
        return .{ .initial_connection = .{
            .identity = identity_bytes[0..32].*,
            .connection_id = conn_bytes[0..16].*,
            .token = token,
        } };
    }

    fn decodeSubscribeApplied(allocator: std.mem.Allocator, dec: *Decoder) Error!ServerMessage {
        const request_id = try dec.decodeU32();
        const query_set_id = try dec.decodeU32();
        const rows = try decodeQueryRows(allocator, dec);
        return .{ .subscribe_applied = .{
            .request_id = request_id,
            .query_set_id = query_set_id,
            .rows = rows,
        } };
    }

    fn decodeUnsubscribeApplied(allocator: std.mem.Allocator, dec: *Decoder) Error!ServerMessage {
        const request_id = try dec.decodeU32();
        const query_set_id = try dec.decodeU32();
        // Option<QueryRows>
        const opt_tag = try dec.decodeU8();
        const rows: ?[]const TableRows = switch (opt_tag) {
            0 => try decodeQueryRows(allocator, dec),
            1 => null,
            else => return Error.InvalidOptionTag,
        };
        return .{ .unsubscribe_applied = .{
            .request_id = request_id,
            .query_set_id = query_set_id,
            .rows = rows,
        } };
    }

    fn decodeSubscriptionError(dec: *Decoder) Error!ServerMessage {
        // Option<u32> request_id
        const opt_tag = try dec.decodeU8();
        const request_id: ?u32 = switch (opt_tag) {
            0 => try dec.decodeU32(),
            1 => null,
            else => return Error.InvalidOptionTag,
        };
        const query_set_id = try dec.decodeU32();
        const err = try dec.decodeString();
        return .{ .subscription_error = .{
            .request_id = request_id,
            .query_set_id = query_set_id,
            .@"error" = err,
        } };
    }

    fn decodeTransactionUpdate(allocator: std.mem.Allocator, dec: *Decoder) Error!ServerMessage {
        const query_sets = try decodeQuerySetUpdates(allocator, dec);
        return .{ .transaction_update = .{
            .query_sets = query_sets,
        } };
    }

    fn decodeOneOffQueryResult(allocator: std.mem.Allocator, dec: *Decoder) Error!ServerMessage {
        const request_id = try dec.decodeU32();
        const result_tag = try dec.decodeU8();
        const result: OneOffResult = switch (result_tag) {
            0 => .{ .ok = try decodeQueryRows(allocator, dec) },
            1 => .{ .err = try dec.decodeString() },
            else => return Error.UnknownOneOffResultTag,
        };
        return .{ .one_off_query_result = .{
            .request_id = request_id,
            .result = result,
        } };
    }

    fn decodeReducerResult(allocator: std.mem.Allocator, dec: *Decoder) Error!ServerMessage {
        const request_id = try dec.decodeU32();
        const timestamp = try dec.decodeI64();
        const outcome_tag = try dec.decodeU8();
        const result: ReducerOutcome = switch (outcome_tag) {
            0 => blk: {
                const ret_value = try dec.decodeBytes();
                const query_sets = try decodeQuerySetUpdates(allocator, dec);
                break :blk .{ .ok = .{ .return_value = ret_value, .transaction = query_sets } };
            },
            1 => .ok_empty,
            2 => .{ .err = try dec.decodeBytes() },
            3 => .{ .internal_error = try dec.decodeString() },
            else => return Error.UnknownReducerOutcomeTag,
        };
        return .{ .reducer_result = .{
            .request_id = request_id,
            .timestamp = timestamp,
            .result = result,
        } };
    }

    fn decodeProcedureResult(dec: *Decoder) Error!ServerMessage {
        const status_tag = try dec.decodeU8();
        const status: ProcedureStatus = switch (status_tag) {
            0 => .{ .returned = try dec.decodeBytes() },
            1 => .{ .internal_error = try dec.decodeString() },
            else => return Error.UnknownProcedureStatusTag,
        };
        const timestamp = try dec.decodeI64();
        const duration = try dec.decodeI64();
        const request_id = try dec.decodeU32();
        return .{ .procedure_result = .{
            .status = status,
            .timestamp = timestamp,
            .total_host_execution_duration = duration,
            .request_id = request_id,
        } };
    }

    // -- Shared decoders --

    fn decodeQueryRows(allocator: std.mem.Allocator, dec: *Decoder) Error![]const TableRows {
        const count = try dec.decodeU32();
        const rows = try allocator.alloc(TableRows, count);
        for (rows) |*row| {
            row.table_name = try dec.decodeString();
            row.rows = try decodeBsatnRowList(allocator, dec);
        }
        return rows;
    }

    fn decodeBsatnRowList(allocator: std.mem.Allocator, dec: *Decoder) Error!BsatnRowList {
        _ = allocator;
        const hint_tag = try dec.decodeU8();
        const size_hint: RowSizeHint = switch (hint_tag) {
            0 => .{ .fixed_size = try dec.decodeU16() },
            1 => blk: {
                const offset_count = try dec.decodeU32();
                // Read raw offset bytes directly from the frame buffer (zero-copy)
                const raw_bytes = try dec.readBytes(offset_count * 8);
                break :blk .{ .row_offsets = .{
                    .count = offset_count,
                    .raw_bytes = raw_bytes,
                } };
            },
            else => return Error.UnknownRowSizeHintTag,
        };
        const rows_data = try dec.decodeBytes();
        return .{ .size_hint = size_hint, .rows_data = rows_data };
    }

    fn decodeQuerySetUpdates(allocator: std.mem.Allocator, dec: *Decoder) Error![]const QuerySetUpdate {
        const count = try dec.decodeU32();
        const updates = try allocator.alloc(QuerySetUpdate, count);
        for (updates) |*update| {
            update.query_set_id = try dec.decodeU32();
            const table_count = try dec.decodeU32();
            const tables = try allocator.alloc(TableUpdate, table_count);
            for (tables) |*table| {
                table.table_name = try dec.decodeString();
                const row_count = try dec.decodeU32();
                const rows = try allocator.alloc(TableUpdateRows, row_count);
                for (rows) |*row| {
                    const row_tag = try dec.decodeU8();
                    row.* = switch (row_tag) {
                        0 => .{ .persistent = .{
                            .inserts = try decodeBsatnRowList(allocator, dec),
                            .deletes = try decodeBsatnRowList(allocator, dec),
                        } },
                        1 => .{ .event = try decodeBsatnRowList(allocator, dec) },
                        else => return Error.UnknownTableUpdateRowsTag,
                    };
                }
                table.rows = rows;
            }
            update.tables = tables;
        }
        return updates;
    }
};

// ============================================================
// Tests — mirroring Elixir protocol_test.exs
// ============================================================

test "ClientMessage encode Subscribe" {
    const allocator = std.testing.allocator;
    const queries = [_][]const u8{"SELECT * FROM users"};
    const msg = ClientMessage{ .subscribe = .{
        .request_id = 1,
        .query_set_id = 100,
        .query_strings = &queries,
    } };

    const encoded = try msg.encode(allocator);
    defer allocator.free(encoded);

    // tag 0 + product(u32, u32, array(string))
    try std.testing.expectEqual(@as(u8, 0), encoded[0]);
}

test "ClientMessage encode Unsubscribe" {
    const allocator = std.testing.allocator;
    const msg = ClientMessage{ .unsubscribe = .{
        .request_id = 2,
        .query_set_id = 100,
        .flags = .default,
    } };

    const encoded = try msg.encode(allocator);
    defer allocator.free(encoded);

    var dec = Decoder.init(encoded);
    try std.testing.expectEqual(@as(u8, 1), try dec.decodeU8()); // tag
    try std.testing.expectEqual(@as(u32, 2), try dec.decodeU32());
    try std.testing.expectEqual(@as(u32, 100), try dec.decodeU32());
    try std.testing.expectEqual(@as(u8, 0), try dec.decodeU8()); // flags
}

test "ClientMessage encode OneOffQuery" {
    const allocator = std.testing.allocator;
    const msg = ClientMessage{ .one_off_query = .{
        .request_id = 3,
        .query_string = "SELECT * FROM users WHERE id = 1",
    } };

    const encoded = try msg.encode(allocator);
    defer allocator.free(encoded);

    var dec = Decoder.init(encoded);
    try std.testing.expectEqual(@as(u8, 2), try dec.decodeU8()); // tag
    try std.testing.expectEqual(@as(u32, 3), try dec.decodeU32());
    try std.testing.expectEqualStrings("SELECT * FROM users WHERE id = 1", try dec.decodeString());
}

test "ClientMessage encode CallReducer" {
    const allocator = std.testing.allocator;
    const args = &[_]u8{ 1, 2, 3 };
    const msg = ClientMessage{ .call_reducer = .{
        .request_id = 4,
        .reducer = "say_hello",
        .args = args,
    } };

    const encoded = try msg.encode(allocator);
    defer allocator.free(encoded);

    var dec = Decoder.init(encoded);
    try std.testing.expectEqual(@as(u8, 3), try dec.decodeU8()); // tag
    try std.testing.expectEqual(@as(u32, 4), try dec.decodeU32());
    try std.testing.expectEqual(@as(u8, 0), try dec.decodeU8()); // flags
    try std.testing.expectEqualStrings("say_hello", try dec.decodeString());
    try std.testing.expectEqualSlices(u8, args, try dec.decodeBytes());
}

test "ClientMessage Subscribe with multiple queries" {
    const allocator = std.testing.allocator;
    const queries = [_][]const u8{ "SELECT * FROM players", "SELECT * FROM scores" };
    const msg = ClientMessage{ .subscribe = .{
        .request_id = 42,
        .query_set_id = 7,
        .query_strings = &queries,
    } };

    const encoded = try msg.encode(allocator);
    defer allocator.free(encoded);

    var dec = Decoder.init(encoded);
    try std.testing.expectEqual(@as(u8, 0), try dec.decodeU8()); // tag
    try std.testing.expectEqual(@as(u32, 42), try dec.decodeU32());
    try std.testing.expectEqual(@as(u32, 7), try dec.decodeU32());
    try std.testing.expectEqual(@as(u32, 2), try dec.decodeU32()); // array count
    try std.testing.expectEqualStrings("SELECT * FROM players", try dec.decodeString());
    try std.testing.expectEqualStrings("SELECT * FROM scores", try dec.decodeString());
}

test "ServerMessage decode InitialConnection" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    // Build: tag(0) + identity(32 bytes) + connection_id(16 bytes) + string(token)
    try enc.encodeU8(allocator, 0);
    var identity: [32]u8 = undefined;
    for (&identity, 0..) |*b, i| b.* = @intCast(i);
    try enc.appendRaw(allocator, &identity);
    var conn_id: [16]u8 = undefined;
    for (&conn_id, 0..) |*b, i| b.* = @intCast(i + 100);
    try enc.appendRaw(allocator, &conn_id);
    try enc.encodeString(allocator, "eyJhbGciOiJFUzI1NiJ9.test.sig");

    const msg = try ServerMessage.decode(allocator, enc.writtenSlice());
    const ic = msg.initial_connection;
    try std.testing.expectEqualSlices(u8, &identity, &ic.identity);
    try std.testing.expectEqualSlices(u8, &conn_id, &ic.connection_id);
    try std.testing.expectEqualStrings("eyJhbGciOiJFUzI1NiJ9.test.sig", ic.token);
}

test "ServerMessage decode SubscriptionError" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    // tag 3, request_id=Some(5), query_set_id=10, error="bad query"
    try enc.encodeU8(allocator, 3); // tag
    try enc.encodeU8(allocator, 0); // Some tag
    try enc.encodeU32(allocator, 5);
    try enc.encodeU32(allocator, 10);
    try enc.encodeString(allocator, "bad query");

    const msg = try ServerMessage.decode(allocator, enc.writtenSlice());
    const se = msg.subscription_error;
    try std.testing.expectEqual(@as(?u32, 5), se.request_id);
    try std.testing.expectEqual(@as(u32, 10), se.query_set_id);
    try std.testing.expectEqualStrings("bad query", se.@"error");
}

test "ServerMessage decode ReducerResult OkEmpty" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    try enc.encodeU8(allocator, 6); // tag
    try enc.encodeU32(allocator, 99);
    try enc.encodeI64(allocator, 1_700_000_000_000_000_000);
    try enc.encodeU8(allocator, 1); // OkEmpty

    const msg = try ServerMessage.decode(allocator, enc.writtenSlice());
    const rr = msg.reducer_result;
    try std.testing.expectEqual(@as(u32, 99), rr.request_id);
    try std.testing.expectEqual(@as(i64, 1_700_000_000_000_000_000), rr.timestamp);
    try std.testing.expect(rr.result == .ok_empty);
}

test "ServerMessage decode ReducerResult InternalError" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    try enc.encodeU8(allocator, 6); // tag
    try enc.encodeU32(allocator, 7);
    try enc.encodeI64(allocator, 0);
    try enc.encodeU8(allocator, 3); // InternalError
    try enc.encodeString(allocator, "reducer panicked");

    const msg = try ServerMessage.decode(allocator, enc.writtenSlice());
    const rr = msg.reducer_result;
    try std.testing.expectEqual(@as(u32, 7), rr.request_id);
    try std.testing.expectEqualStrings("reducer panicked", rr.result.internal_error);
}

test "ServerMessage decode OneOffQueryResult Ok" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    // tag 5 + request_id + Result(Ok(QueryRows))
    try enc.encodeU8(allocator, 5);
    try enc.encodeU32(allocator, 42);
    try enc.encodeU8(allocator, 0); // Ok tag
    // QueryRows = array of 1 SingleTableRows
    try enc.encodeU32(allocator, 1); // array count
    try enc.encodeString(allocator, "users"); // table_name
    // BsatnRowList: size_hint=FixedSize(8) + rows_data
    try enc.encodeU8(allocator, 0); // FixedSize tag
    try enc.encodeU16(allocator, 8);
    const row_data = [_]u8{ 1, 0, 0, 0, 0, 0, 0, 0 };
    try enc.encodeBytes(allocator, &row_data);

    const msg = try ServerMessage.decode(allocator, enc.writtenSlice());
    defer allocator.free(msg.one_off_query_result.result.ok);
    const oqr = msg.one_off_query_result;
    try std.testing.expectEqual(@as(u32, 42), oqr.request_id);
    const table_rows = oqr.result.ok;
    try std.testing.expectEqual(@as(usize, 1), table_rows.len);
    try std.testing.expectEqualStrings("users", table_rows[0].table_name);
    try std.testing.expectEqual(@as(u16, 8), table_rows[0].rows.size_hint.fixed_size);
}

test "ServerMessage decode OneOffQueryResult Err" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    try enc.encodeU8(allocator, 5);
    try enc.encodeU32(allocator, 43);
    try enc.encodeU8(allocator, 1); // Err tag
    try enc.encodeString(allocator, "table not found");

    const msg = try ServerMessage.decode(allocator, enc.writtenSlice());
    try std.testing.expectEqual(@as(u32, 43), msg.one_off_query_result.request_id);
    try std.testing.expectEqualStrings("table not found", msg.one_off_query_result.result.err);
}

test "ServerMessage decode SubscribeApplied" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    try enc.encodeU8(allocator, 1); // tag
    try enc.encodeU32(allocator, 10);
    try enc.encodeU32(allocator, 20);
    // QueryRows: array of 1 SingleTableRows
    try enc.encodeU32(allocator, 1);
    try enc.encodeString(allocator, "players");
    // BsatnRowList: FixedSize(4) + rows_data
    try enc.encodeU8(allocator, 0);
    try enc.encodeU16(allocator, 4);
    const row_data = [_]u8{ 99, 0, 0, 0 };
    try enc.encodeBytes(allocator, &row_data);

    const msg = try ServerMessage.decode(allocator, enc.writtenSlice());
    defer allocator.free(msg.subscribe_applied.rows);
    const sa = msg.subscribe_applied;
    try std.testing.expectEqual(@as(u32, 10), sa.request_id);
    try std.testing.expectEqual(@as(u32, 20), sa.query_set_id);
    try std.testing.expectEqualStrings("players", sa.rows[0].table_name);
    try std.testing.expectEqual(@as(u16, 4), sa.rows[0].rows.size_hint.fixed_size);
    try std.testing.expectEqualSlices(u8, &row_data, sa.rows[0].rows.rows_data);
}

test "ServerMessage decode SubscribeApplied with row_offsets" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    try enc.encodeU8(allocator, 1); // SubscribeApplied tag
    try enc.encodeU32(allocator, 10); // request_id
    try enc.encodeU32(allocator, 20); // query_set_id
    // QueryRows: 1 table
    try enc.encodeU32(allocator, 1);
    try enc.encodeString(allocator, "users");
    // BsatnRowList with RowOffsets hint (tag=1)
    try enc.encodeU8(allocator, 1); // row_offsets tag
    try enc.encodeU32(allocator, 2); // 2 offsets
    try enc.encodeU64(allocator, 0); // offset 0
    try enc.encodeU64(allocator, 4); // offset 4
    // rows_data: two 4-byte "rows"
    try enc.encodeBytes(allocator, &[_]u8{ 0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44 });

    const msg = try ServerMessage.decode(allocator, enc.writtenSlice());
    defer allocator.free(msg.subscribe_applied.rows);
    const sa = msg.subscribe_applied;
    try std.testing.expectEqual(@as(u32, 10), sa.request_id);

    const hint = sa.rows[0].rows.size_hint;
    try std.testing.expect(hint == .row_offsets);
    const offsets = hint.row_offsets;
    try std.testing.expectEqual(@as(u32, 2), offsets.count);
    try std.testing.expectEqual(@as(u64, 0), offsets.getOffset(0));
    try std.testing.expectEqual(@as(u64, 4), offsets.getOffset(1));
    try std.testing.expectEqual(@as(usize, 8), sa.rows[0].rows.rows_data.len);
}

test "ServerMessage decode TransactionUpdate" {
    const allocator = std.testing.allocator;
    var enc = Encoder.init();
    defer enc.deinit(allocator);

    const insert_data = [_]u8{ 1, 2, 3, 4 };
    const delete_data = [_]u8{ 5, 6, 7, 8 };

    try enc.encodeU8(allocator, 4); // TransactionUpdate tag
    // Array of 1 QuerySetUpdate
    try enc.encodeU32(allocator, 1);
    try enc.encodeU32(allocator, 55); // query_set_id
    // Array of 1 TableUpdate
    try enc.encodeU32(allocator, 1);
    try enc.encodeString(allocator, "scores"); // table_name
    // Array of 1 TableUpdateRows
    try enc.encodeU32(allocator, 1);
    try enc.encodeU8(allocator, 0); // PersistentTable tag
    // inserts BsatnRowList
    try enc.encodeU8(allocator, 0); // FixedSize
    try enc.encodeU16(allocator, 4);
    try enc.encodeBytes(allocator, &insert_data);
    // deletes BsatnRowList
    try enc.encodeU8(allocator, 0); // FixedSize
    try enc.encodeU16(allocator, 4);
    try enc.encodeBytes(allocator, &delete_data);

    const msg = try ServerMessage.decode(allocator, enc.writtenSlice());
    const tu = msg.transaction_update;
    defer {
        for (tu.query_sets) |qs| {
            for (qs.tables) |t| allocator.free(t.rows);
            allocator.free(qs.tables);
        }
        allocator.free(tu.query_sets);
    }

    try std.testing.expectEqual(@as(usize, 1), tu.query_sets.len);
    try std.testing.expectEqual(@as(u32, 55), tu.query_sets[0].query_set_id);

    const table = tu.query_sets[0].tables[0];
    try std.testing.expectEqualStrings("scores", table.table_name);

    const persistent = table.rows[0].persistent;
    try std.testing.expectEqual(@as(u16, 4), persistent.inserts.size_hint.fixed_size);
    try std.testing.expectEqualSlices(u8, &insert_data, persistent.inserts.rows_data);
    try std.testing.expectEqual(@as(u16, 4), persistent.deletes.size_hint.fixed_size);
    try std.testing.expectEqualSlices(u8, &delete_data, persistent.deletes.rows_data);
}

test "decompress none" {
    const allocator = std.testing.allocator;
    const payload = [_]u8{ 1, 2, 3 };
    const frame = [_]u8{0x00} ++ payload;
    const result = try decompress(allocator, &frame);
    defer result.deinit(allocator);
    try std.testing.expectEqualSlices(u8, &payload, result.data);
    try std.testing.expect(!result.allocated);
}

test "decompress empty frame" {
    const allocator = std.testing.allocator;
    const result = decompress(allocator, &[_]u8{});
    try std.testing.expectError(Error.EmptyFrame, result);
}

test "decompress unknown tag" {
    const allocator = std.testing.allocator;
    const result = decompress(allocator, &[_]u8{ 0xFF, 1 });
    try std.testing.expectError(Error.UnknownCompression, result);
}

test "decompress gzip" {
    const allocator = std.testing.allocator;

    // Pre-compressed gzip of "Hello SpacetimeDB gzip test"
    const gz_data = [_]u8{
        0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xf3, 0x48,
        0xcd, 0xc9, 0xc9, 0x57, 0x08, 0x2e, 0x48, 0x4c, 0x4e, 0x2d, 0xc9, 0xcc,
        0x4d, 0x75, 0x71, 0x52, 0x48, 0xaf, 0xca, 0x2c, 0x50, 0x28, 0x49, 0x2d,
        0x2e, 0x01, 0x00, 0xcb, 0xf3, 0xf0, 0x4a, 0x1b, 0x00, 0x00, 0x00,
    };

    // Build frame: 0x02 gzip tag + compressed data
    var frame: [1 + gz_data.len]u8 = undefined;
    frame[0] = 0x02;
    @memcpy(frame[1..], &gz_data);

    const result = try decompress(allocator, &frame);
    defer result.deinit(allocator);

    try std.testing.expect(result.allocated);
    try std.testing.expectEqualStrings("Hello SpacetimeDB gzip test", result.data);
}

test "decompress brotli" {
    if (comptime !build_options.enable_brotli) return; // skip when brotli not enabled

    const allocator = std.testing.allocator;

    // Pre-compressed brotli of "hello world from brotli"
    const br_data = [_]u8{
        0x0b, 0x0b, 0x80, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72,
        0x6c, 0x64, 0x20, 0x66, 0x72, 0x6f, 0x6d, 0x20, 0x62, 0x72, 0x6f, 0x74,
        0x6c, 0x69, 0x03,
    };

    // Build frame: 0x01 brotli tag + compressed data
    var frame: [1 + br_data.len]u8 = undefined;
    frame[0] = 0x01;
    @memcpy(frame[1..], &br_data);

    const result = try decompress(allocator, &frame);
    defer result.deinit(allocator);

    try std.testing.expect(result.allocated);
    try std.testing.expectEqualStrings("hello world from brotli", result.data);
}

test "decompress brotli returns error when disabled" {
    if (comptime build_options.enable_brotli) return; // skip when brotli is enabled

    const allocator = std.testing.allocator;
    const frame = [_]u8{ 0x01, 0x00 }; // brotli tag + dummy payload
    const result = decompress(allocator, &frame);
    try std.testing.expectError(Error.DecompressionFailed, result);
}
