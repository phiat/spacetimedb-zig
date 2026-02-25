# spacetimedb-zig

A Zig client SDK for [SpacetimeDB](https://spacetimedb.com) — the database that runs WebAssembly modules and pushes real-time updates to clients.

Ported from the [Elixir SpacetimeDB SDK](https://github.com/clockworklabs/spacetimedb), with API design influenced by the [Rust SDK](https://spacetimedb.com/docs/clients/rust).

## Status

**Work in progress.** Core layers are being built bottom-up:

- [x] Algebraic type system
- [x] BSATN encoder/decoder
- [ ] Schema fetcher and parser
- [ ] Protocol messages (v2 binary)
- [ ] WebSocket connection manager
- [ ] Row decoder
- [ ] Value encoder (schema-aware)
- [ ] Client cache (in-memory table store)
- [ ] High-level client API
- [ ] HTTP REST client

## Requirements

- **Zig 0.15.2** or later

## Building

```bash
zig build          # build the library
zig build test     # run all tests
```

Or using [just](https://github.com/casey/just):

```bash
just build         # build
just test          # run tests
just fmt           # format source
just check         # type-check only (fast)
```

## Architecture

```
src/
  root.zig         # library root — re-exports all public modules
  types.zig        # algebraic type system (AlgebraicType, AlgebraicValue)
  bsatn.zig        # BSATN binary codec (Encoder, Decoder)
```

### BSATN Format

BSATN (Binary SpacetimeDB Algebraic Type Notation) is the wire format for all SpacetimeDB protocol messages and row data:

- All integers: little-endian
- Strings/bytes: `u32` length prefix + data
- Arrays: `u32` count + concatenated elements
- Options: `u8` tag (`0` = Some + payload, `1` = None)
- Products (structs): concatenated fields, no separators
- Sums (enums): `u8` tag + variant payload

## License

See [LICENSE](LICENSE).
