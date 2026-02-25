const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // External dependency: websocket.zig
    const websocket_dep = b.dependency("websocket", .{
        .target = target,
        .optimize = optimize,
    });
    const websocket_mod = websocket_dep.module("websocket");

    // Main library module
    const lib_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "websocket", .module = websocket_mod },
        },
    });

    // Static library artifact
    const lib = b.addLibrary(.{
        .name = "spacetimedb",
        .root_module = lib_mod,
    });
    b.installArtifact(lib);

    // Tests
    const lib_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "websocket", .module = websocket_mod },
            },
        }),
    });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_tests.step);

    // Integration tests (require live SpacetimeDB at localhost:3000)
    const integration_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/integration_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "websocket", .module = websocket_mod },
            },
        }),
    });
    const run_integration_tests = b.addRunArtifact(integration_tests);

    const integration_step = b.step("integration-test", "Run integration tests (requires live SpacetimeDB)");
    integration_step.dependOn(&run_integration_tests.step);

    // Codegen CLI executable
    const codegen_exe = b.addExecutable(.{
        .name = "spacetimedb-codegen",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/codegen_cli.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "spacetimedb", .module = lib_mod },
                .{ .name = "websocket", .module = websocket_mod },
            },
        }),
    });
    b.installArtifact(codegen_exe);

    const run_codegen = b.addRunArtifact(codegen_exe);
    run_codegen.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_codegen.addArgs(args);
    }
    const codegen_step = b.step("codegen", "Generate Zig source from SpacetimeDB schema");
    codegen_step.dependOn(&run_codegen.step);

    // Check step (fast type-checking)
    const check = b.addLibrary(.{
        .name = "spacetimedb",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/root.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "websocket", .module = websocket_mod },
            },
        }),
    });
    const check_step = b.step("check", "Check for compilation errors");
    check_step.dependOn(&check.step);
}
