# Build rublk (vendored as the src/rublk submodule) with cargo, producing
# the "rublk" executable, which the rbd CLI's ublk device-type backend
# shells out to at runtime when RBD_UBLK=rublk is set (see
# src/tools/rbd/action/Ublk.cc) -- there is no compile-time link dependency
# from ceph's own code onto rublk, so nothing here produces an imported
# CMake target for other targets to link against.
#
# This is temporary vendoring (rublk has no upstream release/packaging of
# its own yet) purely so shaman-built packages carry an alternative ublk
# backend for teuthology testing, mirroring build_ublksrv() in
# BuildUblksrv.cmake for the C++ backend.
#
# Unlike ublksrv's autotools build, this needs network access at build
# time: cargo resolves and fetches rublk's dependency graph (tokio,
# libublk-rs, clap, serde, ...) from crates.io/git rather than linking
# against anything ceph already vendors, and nothing here vendors those
# crates into the source tree the way ublksrv's liburing dependency is
# sidestepped. "--locked" pins the exact versions from the committed
# Cargo.lock, but does not make the build offline.
# rublk's dependency graph needs a rustc newer than several distros' own
# packaged cargo/rustc: notably, libublk-rs-sys needs >= 1.80, but Ubuntu
# jammy/noble's "cargo"/"rustc" apt packages are both stuck at 1.75.0 (Rocky
# 10 and CentOS 9's dnf packages happen to already be new enough, at 1.92/
# 1.97). Rather than pin rublk's own dependencies down to whatever the
# oldest supported distro's ancient toolchain can build -- a moving,
# increasingly awkward target as rublk's own upstream dependencies bump
# their MSRVs over time -- bootstrap a private, version-pinned toolchain via
# rustup when the system cargo is too old, entirely independent of the
# distro's own package. This needs network access, already required
# unconditionally for cargo's own crates.io/git dependency fetches.
set(RUBLK_RUST_MIN_VERSION "1.80")
set(RUBLK_RUST_BOOTSTRAP_VERSION "1.82.0")

function(build_rublk)
  find_program(CARGO_EXECUTABLE cargo)
  set(cargo_new_enough FALSE)
  if(CARGO_EXECUTABLE)
    execute_process(
      COMMAND ${CARGO_EXECUTABLE} --version
      OUTPUT_VARIABLE cargo_version_output
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(cargo_version_output MATCHES "cargo ([0-9]+\\.[0-9]+\\.[0-9]+)")
      if(NOT CMAKE_MATCH_1 VERSION_LESS RUBLK_RUST_MIN_VERSION)
        set(cargo_new_enough TRUE)
      endif()
    endif()
  endif()

  if(NOT cargo_new_enough)
    set(rustup_home "${CMAKE_CURRENT_BINARY_DIR}/rublk-rustup-home")
    set(cargo_home "${CMAKE_CURRENT_BINARY_DIR}/rublk-cargo-home")
    set(bootstrapped_cargo "${cargo_home}/bin/cargo")
    if(NOT EXISTS "${bootstrapped_cargo}")
      message(STATUS "System cargo missing or older than ${RUBLK_RUST_MIN_VERSION} "
        "(required by WITH_RBD_RUBLK's rublk dependencies) -- bootstrapping "
        "rust ${RUBLK_RUST_BOOTSTRAP_VERSION} via rustup into ${cargo_home}")
      file(MAKE_DIRECTORY ${rustup_home} ${cargo_home})
      set(ENV{RUSTUP_HOME} ${rustup_home})
      set(ENV{CARGO_HOME} ${cargo_home})
      execute_process(
        COMMAND sh -c "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain ${RUBLK_RUST_BOOTSTRAP_VERSION} --no-modify-path"
        RESULT_VARIABLE rustup_rc)
      if(NOT rustup_rc EQUAL 0 OR NOT EXISTS "${bootstrapped_cargo}")
        message(FATAL_ERROR "Failed to bootstrap a rust toolchain via rustup for WITH_RBD_RUBLK")
      endif()
    endif()
    set(CARGO_EXECUTABLE "${bootstrapped_cargo}")
    # cargo's rustup shim resolves the actual toolchain via these same env
    # vars at *run* time too, not just during the rustup-init call above --
    # propagated into the ExternalProject_Add build step below.
    set(rublk_cargo_env "RUSTUP_HOME=${rustup_home}" "CARGO_HOME=${cargo_home}")
  endif()

  set(rublk_source_dir "${PROJECT_SOURCE_DIR}/src/rublk")
  set(rublk_target_dir "${CMAKE_CURRENT_BINARY_DIR}/rublk-target")
  set(rublk_binary "${rublk_target_dir}/release/rublk")

  # rublk's rbd target is hand-written FFI (see src/rbd.rs's own comment:
  # no Rust binding crate exists for librbd/librados on crates.io) that
  # only needs a link-time "-lrbd -lrados" (emitted by build.rs when the
  # "rbd" feature is enabled) against ceph's own just-built libraries, not
  # a system install -- same relationship ublk.rbd has to librbd/librados
  # in BuildUblksrv.cmake, minus the header search path (there's no C/C++
  # header for rustc to find here). RUSTFLAGS supplies the extra -L; the
  # DEPENDS below ensures those .so files actually exist first.
  include(ExternalProject)
  ExternalProject_Add(rublk_ext
    SOURCE_DIR ${rublk_source_dir}
    DEPENDS librbd librados
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND ""
    BUILD_COMMAND env RUSTFLAGS=-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY} ${rublk_cargo_env}
      ${CARGO_EXECUTABLE} build --release --locked
      --no-default-features --features rbd
      --target-dir ${rublk_target_dir}
    BUILD_BYPRODUCTS "${rublk_binary}"
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    LOG_BUILD ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)

  # nothing links against rublk_ext, so without this a plain "ninja"
  # (default target) would never build it
  add_custom_target(rublk ALL DEPENDS rublk_ext)

  # a plain install(PROGRAMS) suffices here (unlike ublksrv's scripted
  # install): there's a single output binary rather than several
  # autotools install targets to invoke, and CMake's own install() rule
  # already defers to real install time and picks up DESTDIR on its own,
  # so no cmake.in template/install(SCRIPT) indirection is needed.
  install(PROGRAMS ${rublk_binary} DESTINATION ${CMAKE_INSTALL_SBINDIR})
endfunction()
