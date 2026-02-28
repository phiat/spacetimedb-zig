// Property-based tests for BSATN codec and typed table layer.
//
// Uses zigcheck to generate random inputs and verify encode/decode
// roundtrip invariants across all supported types.

const std = @import("std");
const zigcheck = @import("zigcheck");
const gen = zigcheck.generators;

const bsatn = @import("bsatn.zig");
const table = @import("table.zig");

const Encoder = bsatn.Encoder;
const Decoder = bsatn.Decoder;

// ============================================================
// BSATN primitive roundtrips
// ============================================================

test "property: u8 encode/decode roundtrip" {
    try zigcheck.forAll(u8, gen.int(u8), struct {
        fn prop(n: u8) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeU8(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeU8();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: i8 encode/decode roundtrip" {
    try zigcheck.forAll(i8, gen.int(i8), struct {
        fn prop(n: i8) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeI8(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeI8();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: u16 encode/decode roundtrip" {
    try zigcheck.forAll(u16, gen.int(u16), struct {
        fn prop(n: u16) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeU16(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeU16();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: i16 encode/decode roundtrip" {
    try zigcheck.forAll(i16, gen.int(i16), struct {
        fn prop(n: i16) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeI16(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeI16();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: u32 encode/decode roundtrip" {
    try zigcheck.forAll(u32, gen.int(u32), struct {
        fn prop(n: u32) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeU32(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeU32();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: i32 encode/decode roundtrip" {
    try zigcheck.forAll(i32, gen.int(i32), struct {
        fn prop(n: i32) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeI32(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeI32();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: u64 encode/decode roundtrip" {
    try zigcheck.forAll(u64, gen.int(u64), struct {
        fn prop(n: u64) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeU64(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeU64();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: i64 encode/decode roundtrip" {
    try zigcheck.forAll(i64, gen.int(i64), struct {
        fn prop(n: i64) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeI64(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeI64();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: u128 encode/decode roundtrip" {
    try zigcheck.forAll(u128, gen.int(u128), struct {
        fn prop(n: u128) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeU128(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeU128();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: i128 encode/decode roundtrip" {
    try zigcheck.forAll(i128, gen.int(i128), struct {
        fn prop(n: i128) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeI128(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeI128();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: f32 encode/decode roundtrip" {
    try zigcheck.forAll(f32, gen.float(f32), struct {
        fn prop(n: f32) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeF32(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeF32();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: f64 encode/decode roundtrip" {
    try zigcheck.forAll(f64, gen.float(f64), struct {
        fn prop(n: f64) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeF64(alloc, n);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeF64();
            if (decoded != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: bool encode/decode roundtrip" {
    try zigcheck.forAll(bool, gen.boolean(), struct {
        fn prop(b: bool) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeBool(alloc, b);
            var dec = Decoder.init(enc.writtenSlice());
            const decoded = try dec.decodeBool();
            if (decoded != b) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: string encode/decode roundtrip" {
    try zigcheck.forAll(
        []const u8,
        gen.asciiString(64),
        struct {
            fn prop(s: []const u8) !void {
                const alloc = std.testing.allocator;
                var enc = Encoder.init();
                defer enc.deinit(alloc);
                try enc.encodeString(alloc, s);
                var dec = Decoder.init(enc.writtenSlice());
                const decoded = try dec.decodeString();
                if (!std.mem.eql(u8, decoded, s)) return error.PropertyFalsified;
            }
        }.prop,
    );
}

test "property: unicode string encode/decode roundtrip" {
    try zigcheck.forAll(
        []const u8,
        gen.unicodeString(32),
        struct {
            fn prop(s: []const u8) !void {
                const alloc = std.testing.allocator;
                var enc = Encoder.init();
                defer enc.deinit(alloc);
                try enc.encodeString(alloc, s);
                var dec = Decoder.init(enc.writtenSlice());
                const decoded = try dec.decodeString();
                if (!std.mem.eql(u8, decoded, s)) return error.PropertyFalsified;
            }
        }.prop,
    );
}

// ============================================================
// BSATN byte-level invariants
// ============================================================

test "property: u32 encodes to exactly 4 bytes little-endian" {
    try zigcheck.forAll(u32, gen.int(u32), struct {
        fn prop(n: u32) !void {
            const alloc = std.testing.allocator;
            var enc = Encoder.init();
            defer enc.deinit(alloc);
            try enc.encodeU32(alloc, n);
            const bytes = enc.writtenSlice();
            if (bytes.len != 4) return error.PropertyFalsified;
            // Verify little-endian byte order
            const reconstructed = @as(u32, bytes[0]) |
                (@as(u32, bytes[1]) << 8) |
                (@as(u32, bytes[2]) << 16) |
                (@as(u32, bytes[3]) << 24);
            if (reconstructed != n) return error.PropertyFalsified;
        }
    }.prop);
}

test "property: string length prefix matches actual length" {
    try zigcheck.forAll(
        []const u8,
        gen.asciiString(128),
        struct {
            fn prop(s: []const u8) !void {
                const alloc = std.testing.allocator;
                var enc = Encoder.init();
                defer enc.deinit(alloc);
                try enc.encodeString(alloc, s);
                const bytes = enc.writtenSlice();
                // First 4 bytes are the length prefix
                var dec = Decoder.init(bytes);
                const prefix_len = try dec.decodeU32();
                if (prefix_len != s.len) return error.PropertyFalsified;
                // Total encoded size = 4 (prefix) + string length
                if (bytes.len != 4 + s.len) return error.PropertyFalsified;
            }
        }.prop,
    );
}

// ============================================================
// Typed table roundtrips (table.zig)
// ============================================================

test "property: typed struct {u64, u32} encode/decode roundtrip" {
    const Pair = struct { a: u64, b: u32 };

    try zigcheck.forAllZip(
        .{ gen.int(u64), gen.int(u32) },
        struct {
            fn prop(a: u64, b: u32) !void {
                const alloc = std.testing.allocator;
                const original = Pair{ .a = a, .b = b };

                const encoded = table.encodeRow(Pair, alloc, original) catch return;
                defer alloc.free(encoded);

                var decoded = table.decodeRow(Pair, alloc, encoded) catch
                    return error.PropertyFalsified;
                defer table.freeTypedRow(Pair, alloc, &decoded);

                if (decoded.a != original.a or decoded.b != original.b)
                    return error.PropertyFalsified;
            }
        }.prop,
    );
}

test "property: typed struct with bool and optional encode/decode roundtrip" {
    const Record = struct { flag: bool, value: u32, extra: ?u64 };

    try zigcheck.forAllZip(
        .{
            gen.boolean(),
            gen.int(u32),
            // Generate optional u64: ~50% Some, ~50% None
            gen.frequency(?u64, &.{
                .{ 1, gen.map(u64, ?u64, gen.int(u64), struct {
                    fn f(v: u64) ?u64 {
                        return v;
                    }
                }.f) },
                .{ 1, gen.constant(?u64, null) },
            }),
        },
        struct {
            fn prop(flag: bool, value: u32, extra: ?u64) !void {
                const alloc = std.testing.allocator;
                const original = Record{ .flag = flag, .value = value, .extra = extra };

                const encoded = table.encodeRow(Record, alloc, original) catch return;
                defer alloc.free(encoded);

                var decoded = table.decodeRow(Record, alloc, encoded) catch
                    return error.PropertyFalsified;
                defer table.freeTypedRow(Record, alloc, &decoded);

                if (decoded.flag != original.flag) return error.PropertyFalsified;
                if (decoded.value != original.value) return error.PropertyFalsified;
                if (decoded.extra == null and original.extra != null) return error.PropertyFalsified;
                if (decoded.extra != null and original.extra == null) return error.PropertyFalsified;
                if (decoded.extra != null and original.extra != null) {
                    if (decoded.extra.? != original.extra.?) return error.PropertyFalsified;
                }
            }
        }.prop,
    );
}

test "property: typed struct with string field roundtrip" {
    try zigcheck.forAllZip(
        .{ gen.int(u64), gen.asciiString(32), gen.int(u32) },
        struct {
            fn prop(id: u64, name: []const u8, age: u32) !void {
                const alloc = std.testing.allocator;
                const Person = struct { id: u64, name: []const u8, age: u32 };
                const original = Person{ .id = id, .name = name, .age = age };

                const encoded = table.encodeRow(Person, alloc, original) catch return;
                defer alloc.free(encoded);

                var decoded = table.decodeRow(Person, alloc, encoded) catch
                    return error.PropertyFalsified;
                defer table.freeTypedRow(Person, alloc, &decoded);

                if (decoded.id != original.id) return error.PropertyFalsified;
                if (decoded.age != original.age) return error.PropertyFalsified;
                if (!std.mem.eql(u8, decoded.name, original.name)) return error.PropertyFalsified;
            }
        }.prop,
    );
}

test "property: nested struct encode/decode roundtrip" {
    const Point = struct { x: i32, y: i32 };
    const Entity = struct { id: u64, pos: Point };

    try zigcheck.forAllZip(
        .{ gen.int(u64), gen.int(i32), gen.int(i32) },
        struct {
            fn prop(id: u64, x: i32, y: i32) !void {
                const alloc = std.testing.allocator;
                const original = Entity{
                    .id = id,
                    .pos = .{ .x = x, .y = y },
                };

                const encoded = table.encodeRow(Entity, alloc, original) catch return;
                defer alloc.free(encoded);

                var decoded = table.decodeRow(Entity, alloc, encoded) catch
                    return error.PropertyFalsified;
                defer table.freeTypedRow(Entity, alloc, &decoded);

                if (decoded.id != original.id) return error.PropertyFalsified;
                if (decoded.pos.x != original.pos.x) return error.PropertyFalsified;
                if (decoded.pos.y != original.pos.y) return error.PropertyFalsified;
            }
        }.prop,
    );
}

// ============================================================
// Cross-layer: encode with Encoder, decode with table.decodeRow
// ============================================================

test "property: manual BSATN encoding decodes correctly via table.decodeRow" {
    const Simple = struct { x: u32, y: u32 };

    try zigcheck.forAllZip(
        .{ gen.int(u32), gen.int(u32) },
        struct {
            fn prop(x: u32, y: u32) !void {
                const alloc = std.testing.allocator;

                // Encode manually with BSATN encoder
                var enc = Encoder.init();
                defer enc.deinit(alloc);
                try enc.encodeU32(alloc, x);
                try enc.encodeU32(alloc, y);

                // Decode via typed table layer
                var decoded = table.decodeRow(Simple, alloc, enc.writtenSlice()) catch
                    return error.PropertyFalsified;
                defer table.freeTypedRow(Simple, alloc, &decoded);

                if (decoded.x != x or decoded.y != y) return error.PropertyFalsified;
            }
        }.prop,
    );
}
