// Fuzz input helper for tests.
//
// `std.testing.fuzz` hands a `*Smith` to the test. Run from the corpus, its `in` field holds the
// corpus entry exactly as written, so the BSATN corpus literals keep their meaning. Under the
// fuzzer `in` is null and the bytes come from `Smith.slice`.

const std = @import("std");

pub const max_len = 4096;

pub fn bytes(smith: *std.testing.Smith, buf: *[max_len]u8) []const u8 {
    if (smith.in) |in| return in;
    return buf[0..smith.slice(buf)];
}
