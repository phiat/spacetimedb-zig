# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## Project Overview

**spacetimedb-zig**: A Zig client SDK for [SpacetimeDB](https://spacetimedb.com), ported from the [Elixir SDK](../spacetimedbex/). Reference: [Rust SDK docs](https://spacetimedb.com/docs/clients/rust).

### Tech Stack
- **Zig 0.15.2** (latest stable)
- **Target**: Library + optional CLI tools
- **Protocol**: SpacetimeDB v2 BSATN binary WebSocket protocol
- **Reference SDK**: `../spacetimedbex/` (Elixir implementation)

### Architecture Layers (bottom-up, all complete)
1. **types.zig** — Algebraic type system (AlgebraicType, AlgebraicValue, Column)
2. **bsatn.zig** — BSATN binary codec (Encoder, Decoder)
3. **protocol.zig** — v2 binary Client/Server message encoding/decoding
4. **schema.zig** — JSON schema parser with ref resolution
5. **row_decoder.zig** — BSATN row data → decoded Row structs
6. **value_encoder.zig** — AlgebraicValue → BSATN binary (schema-aware)
7. **client_cache.zig** — In-memory table store with PK-based identity
8. **websocket.zig** — WebSocket connection state machine (abstract transport)
9. **http_client.zig** — HTTP REST client (abstract transport)
10. **client.zig** — High-level client API: connect, subscribe, call reducers, query

### Key Design Decisions
- Zig 0.15 removed `async`/`await` — use thread-based concurrency (`std.Thread`)
- No `usingnamespace` in 0.15 — explicit imports everywhere
- `std.json` for JSON (schema fetching), custom BSATN for wire format
- WebSocket via `websocket.zig` (Karl Seguin) or similar dependency
- Allocator-explicit API (Zig convention): all public functions take `std.mem.Allocator`

### Zig 0.15 Gotchas
- `@typeInfo` tags are lowercase: `.int`, `.@"struct"`, `.@"enum"`
- `ArrayListUnmanaged` replaces `ArrayList` (allocator per-call)
- New I/O: `std.fs.File.stdout().writer(&buf)` pattern
- `addExecutable` requires `root_module = b.createModule(...)` in build.zig
- Lossy int-to-float coercion is a compile error

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --status in_progress  # Claim work
bd close <id>         # Complete work
bd sync               # Sync with git
just build            # Build the project
just test             # Run tests
just check            # Type-check without codegen
```

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
