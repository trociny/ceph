# Build ublksrv (vendored as the src/ublksrv submodule) with autotools,
# producing the "ublk" and "ublk.rbd" executables plus libublksrv.so, which
# the rbd CLI's ublk device-type backend shells out to at runtime (see
# src/tools/rbd/action/Ublk.cc) -- there is no compile-time link dependency
# from ceph's own code onto ublksrv, so nothing here produces an imported
# CMake target for other targets to link against.
#
# This is temporary vendoring (not yet an upstream ublksrv release) purely
# so shaman-built packages carry ublk support for teuthology testing.
function(build_ublksrv)
  include(FindMake)
  find_make("MAKE_EXECUTABLE" "make_cmd")

  set(ublksrv_source_dir "${PROJECT_SOURCE_DIR}/src/ublksrv")

  # ublksrv's own ./configure requires "liburing >= 2.2" via pkg-config,
  # a version several distros ship (e.g. Ubuntu 22.04's liburing is
  # 0.7, predating the 2.x series entirely -- 2.2 wasn't released until
  # after 22.04 shipped), so requiring a system package for this would
  # make WITH_RBD_UBLK impossible to satisfy on those distros at all.
  # Sidestep that the same way librbd/librados already are: point
  # ublksrv at ceph's own vendored liburing (URING_INCLUDE_DIR/
  # URING_LIBRARY_DIR or URING_LIBRARIES, set by build_uring()/
  # find_package(uring) -- see WITH_LIBURING/WITH_SYSTEM_LIBURING in
  # the top-level CMakeLists.txt, which runs before add_subdirectory
  # (src) and so before this ever executes) via a synthesized
  # liburing.pc, rather than via a system package of any version.
  if(DEFINED URING_LIBRARY_DIR)
    set(uring_lib_dir "${URING_LIBRARY_DIR}")
  elseif(DEFINED URING_LIBRARIES)
    get_filename_component(uring_lib_dir "${URING_LIBRARIES}" DIRECTORY)
  else()
    message(FATAL_ERROR "ublksrv requires liburing (enable WITH_LIBURING)")
  endif()
  set(uring_include_dir "${URING_INCLUDE_DIR}")

  set(ublksrv_pkgconfig_dir "${CMAKE_CURRENT_BINARY_DIR}/ublksrv-pkgconfig")
  file(MAKE_DIRECTORY ${ublksrv_pkgconfig_dir})
  file(WRITE ${ublksrv_pkgconfig_dir}/liburing.pc
"libdir=${uring_lib_dir}
includedir=${uring_include_dir}

Name: liburing
Description: ceph's own vendored liburing (see BuildUblksrv.cmake)
Version: 2.5
Libs: -L\${libdir} -luring
Cflags: -I\${includedir}
")

  # ublk.rbd links against ceph's own librbd/librados, not a system
  # install: headers come straight from the source tree (available
  # immediately), but the .so files only exist once those targets have
  # actually been built -- hence DEPENDS below, which is also why this
  # function is called from src/CMakeLists.txt (after add_subdirectory
  # (librbd)) rather than from the top-level CMakeLists.txt, where the
  # librbd/librados targets don't exist yet.
  #
  # Only "ublk" and "ublk.rbd" are built out of ublksrv's full target
  # list (which also includes ublk.loop, ublk.nbd, ublk.nfs, ublk.iscsi,
  # etc.) to avoid pulling in their assorted extra dependencies. A plain
  # top-level "make"/"make all" is deliberately avoided too: it would
  # also try to build demo_null/demo_event, which fail to compile once
  # ceph's own src/include is on the include path (it shadows the
  # system <error.h>). "lib" has to be built explicitly first, since
  # invoking "make ublk ublk.rbd" directly on a fresh checkout doesn't
  # know how to build the lib/libublksrv.la dependency it links against
  # (that only happens automatically via a full recursive "make").
  include(ExternalProject)
  ExternalProject_Add(ublksrv_ext
    SOURCE_DIR ${ublksrv_source_dir}
    DEPENDS librbd librados uring::uring
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND autoreconf -i
    COMMAND env CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER}
      PKG_CONFIG_PATH=${ublksrv_pkgconfig_dir}
      ${ublksrv_source_dir}/configure --with-librbd
      --prefix=${CMAKE_INSTALL_PREFIX}
      --libdir=${CMAKE_INSTALL_FULL_LIBDIR}
      # lib/Makefile.am's PKG_CHECK_MODULES-derived LIBURING_CFLAGS only
      # ends up in libublksrv_la_CFLAGS (the C compile rule), not
      # libublksrv_la_CPPFLAGS (shared by both C and C++ rules) -- so
      # lib/ublksrv_json.cpp (a C++ file) never sees it via any
      # per-target variable, only via this global CPPFLAGS. Never
      # mattered while a system liburing-devel was also required (its
      # liburing.h was already on the compiler's own default include
      # path), but does now that ublksrv links solely against ceph's
      # vendored copy (see the liburing.pc synthesis above).
      CPPFLAGS=-I${PROJECT_SOURCE_DIR}/src/include\ -I${uring_include_dir}
      LDFLAGS=-L${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    # MAKE_EXECUTABLE (the real, resolved path) deliberately, not
    # find_make()'s make_cmd/"$(MAKE)" recursive-submake token: that
    # token only expands correctly when GNU Make itself parses the
    # command as literal Makefile recipe text. LOG_BUILD ON below
    # (needed so a failure here doesn't dump megabytes of build output
    # straight to the console) makes CMake instead run each
    # BUILD_COMMAND/COMMAND via execute_process() from a generated
    # script, which passes "$(MAKE)" through unexpanded -- execve()
    # then fails outright since no literal file named "$(MAKE)" exists.
    # Only bites when the outer generator is actually "Unix Makefiles"
    # (as in rpm/deb builds, unlike a plain dev "ninja" build) -- that's
    # the one case where CMAKE_MAKE_PROGRAM matches "make" and
    # find_make() hands back the token instead of a real path.
    BUILD_COMMAND ${MAKE_EXECUTABLE} -C lib
    COMMAND ${MAKE_EXECUTABLE} ublk ublk.rbd
    BUILD_BYPRODUCTS
      "${ublksrv_source_dir}/ublk"
      "${ublksrv_source_dir}/ublk.rbd"
      "${ublksrv_source_dir}/lib/.libs/libublksrv.so"
      "${ublksrv_source_dir}/lib/.libs/libublksrv.so.0"
      "${ublksrv_source_dir}/lib/.libs/libublksrv.so.0.0.0"
    INSTALL_COMMAND ""
    UPDATE_COMMAND ""
    LOG_CONFIGURE ON
    LOG_BUILD ON
    LOG_MERGED_STDOUTERR ON
    LOG_OUTPUT_ON_FAILURE ON)

  # nothing links against ublksrv_ext, so without this a plain "ninja"
  # (default target) would never build it
  add_custom_target(ublksrv ALL DEPENDS ublksrv_ext)

  # ExternalProject's own INSTALL_COMMAND step (deliberately left empty
  # above) would, despite the name, run during the ordinary build step
  # (i.e. during plain "ninja"/rpm %build/dh_auto_build) -- well before
  # any real DESTDIR/buildroot staging exists. Defer the actual
  # "make install" to genuine install time ("ninja install", rpm
  # %install, dh_auto_install) instead, via install(SCRIPT ...), so it
  # picks up whatever DESTDIR is actually set then.
  #
  # Only "ublk"/"ublk.rbd" (via install-sbinPROGRAMS, scoped to just
  # those two via a sbin_PROGRAMS override -- ublksrv declares several
  # other sbin_PROGRAMS that were never built above) and libublksrv.so
  # (via install-libLTLIBRARIES, which also emits a static libublksrv.a
  # and .la file alongside the .so -- deleted again immediately after,
  # see the comment in ublksrv-install.cmake.in) are installed, not the
  # whole project.
  configure_file(
    ${PROJECT_SOURCE_DIR}/cmake/modules/ublksrv-install.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/ublksrv-install.cmake
    @ONLY)
  install(SCRIPT ${CMAKE_CURRENT_BINARY_DIR}/ublksrv-install.cmake)
endfunction()
