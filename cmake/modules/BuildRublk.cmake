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
function(build_rublk)
  find_program(CARGO_EXECUTABLE cargo)
  if(NOT CARGO_EXECUTABLE)
    message(FATAL_ERROR "Can't find cargo, which is required for WITH_RBD_RUBLK")
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
    BUILD_COMMAND env RUSTFLAGS=-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
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
