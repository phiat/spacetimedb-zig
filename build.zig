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

    // Test-only dependency: zigcheck (property-based testing)
    const zigcheck_dep = b.dependency("zigcheck", .{});
    const zigcheck_mod = zigcheck_dep.module("zigcheck");

    // Optional brotli decompression support (requires libbrotlidec)
    const enable_brotli = b.option(bool, "enable-brotli", "Enable brotli decompression (requires libbrotlidec)") orelse false;

    const build_options = b.addOptions();
    build_options.addOption(bool, "enable_brotli", enable_brotli);
    const build_options_mod = build_options.createModule();

    // Helper to create a root module with standard imports
    const mkRootMod = struct {
        fn f(
            builder: *std.Build,
            src: std.Build.LazyPath,
            tgt: std.Build.ResolvedTarget,
            opt: std.builtin.OptimizeMode,
            ws_mod: *std.Build.Module,
            bo_mod: *std.Build.Module,
            do_brotli: bool,
        ) *std.Build.Module {
            const mod = builder.createModule(.{
                .root_source_file = src,
                .target = tgt,
                .optimize = opt,
                .imports = &.{
                    .{ .name = "websocket", .module = ws_mod },
                    .{ .name = "build_options", .module = bo_mod },
                },
            });
            if (do_brotli) {
                mod.linkSystemLibrary("brotlidec", .{});
            }
            return mod;
        }
    }.f;

    // Main library module
    const lib_mod = mkRootMod(b, b.path("src/root.zig"), target, optimize, websocket_mod, build_options_mod, enable_brotli);

    // Static library artifact
    const lib = b.addLibrary(.{
        .name = "spacetimedb",
        .root_module = lib_mod,
    });
    b.installArtifact(lib);


    // Tests
    const test_mod = mkRootMod(b, b.path("src/root.zig"), target, optimize, websocket_mod, build_options_mod, enable_brotli);
    test_mod.addImport("zigcheck", zigcheck_mod);
    const lib_tests = b.addTest(.{ .root_module = test_mod });
    const run_lib_tests = b.addRunArtifact(lib_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_tests.step);

    // Integration tests (require live SpacetimeDB at localhost:3000)
    const int_mod = mkRootMod(b, b.path("src/integration_test.zig"), target, optimize, websocket_mod, build_options_mod, enable_brotli);
    int_mod.addImport("zigcheck", zigcheck_mod);
    const integration_tests = b.addTest(.{ .root_module = int_mod });
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
                .{ .name = "build_options", .module = build_options_mod },
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

    // Codegen CLI tests
    const codegen_test_mod = b.createModule(.{
        .root_source_file = b.path("src/codegen_cli.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "spacetimedb", .module = lib_mod },
            .{ .name = "websocket", .module = websocket_mod },
            .{ .name = "build_options", .module = build_options_mod },
        },
    });
    const codegen_tests = b.addTest(.{ .root_module = codegen_test_mod });
    const run_codegen_tests = b.addRunArtifact(codegen_tests);
    test_step.dependOn(&run_codegen_tests.step);

    // Property tests (zigcheck — can be run independently)
    const prop_mod = mkRootMod(b, b.path("src/property_tests.zig"), target, optimize, websocket_mod, build_options_mod, enable_brotli);
    prop_mod.addImport("zigcheck", zigcheck_mod);
    const prop_tests = b.addTest(.{ .root_module = prop_mod });
    const run_prop_tests = b.addRunArtifact(prop_tests);
    const prop_step = b.step("prop-test", "Run property-based tests (zigcheck)");
    prop_step.dependOn(&run_prop_tests.step);

    // Check step (fast type-checking)
    const check_mod = mkRootMod(b, b.path("src/root.zig"), target, optimize, websocket_mod, build_options_mod, enable_brotli);
    const check = b.addLibrary(.{
        .name = "spacetimedb",
        .root_module = check_mod,
    });
    const check_step = b.step("check", "Check for compilation errors");
    check_step.dependOn(&check.step);
}
