# SpacetimeDB Zig SDK

default:
    @just --list

# Build the library
build:
    zig build

# Build in release mode
build-release:
    zig build -Doptimize=ReleaseSafe

# Run all tests
test:
    zig build test

# Run tests with verbose output
test-verbose:
    zig build test -- --verbose

# Run integration tests (requires live SpacetimeDB at localhost:3000)
integration-test:
    zig build integration-test

# Type-check without full codegen (fast feedback)
check:
    zig build check

# Clean build artifacts
clean:
    rm -rf zig-out .zig-cache zig-cache

# Format all zig files
fmt:
    zig fmt src/ build.zig

# Check formatting without modifying
fmt-check:
    zig fmt --check src/ build.zig

# Run a specific test by name
test-filter FILTER:
    zig build test -- --test-filter "{{FILTER}}"

# Show beads status
status:
    bd ready && bd stats
