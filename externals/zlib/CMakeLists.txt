# CMake module for vendoring zlib
include(FetchContent)

set(ZLIB_VERSION "1.2.8")
set(ZLIB_URL "https://zlib.net/fossils/zlib-${ZLIB_VERSION}.tar.gz")
set(ZLIB_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/zlib-${ZLIB_VERSION}.tar.gz")
set(ZLIB_HASH "MD5=44d667c142d7cda120332623eab69f40")

# -> zlib <-
# NOTE: we deliberately do not use zlib's own configure/CMake build,
# but compile the sources ourselves into a single static libz.a
# with the cvmfs-specific tuning flags below. Our fsck is sensitive
# to the exact version/configuration of zlib used by cvmfs.

setup_external_build_mode(zlib)

FetchContent_Declare(
  ZLIB
  URL "${ZLIB_URL}" "${ZLIB_URL_MIRROR}"
  URL_HASH "${ZLIB_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
  INSTALL_DIR ${EXTERNALS_INSTALL_LOCATION}
  # zlib ships its own CMakeLists.txt (which would build the shared lib and run
  # its test suite). Point SOURCE_SUBDIR at a path with no CMakeLists.txt so
  # MakeAvailable only downloads and extracts -- we define the target ourselves.
  SOURCE_SUBDIR does-not-exist
  EXCLUDE_FROM_ALL
  # Deliberately no FIND_PACKAGE_ARGS: a system zlib must not be picked up
  # implicitly. Content hashes are computed over the deflate output, so a
  # different implementation (e.g. zlib-ng in compat mode, the system libz on
  # recent Fedora/RHEL) yields different object names for identical files.
  # Setting BUILTIN_EXTERNALS_EXCLUDE=zlib still switches to the system zlib,
  # because that selects FETCHCONTENT_TRY_FIND_PACKAGE_MODE=ALWAYS.
)

FetchContent_MakeAvailable(ZLIB)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if(ZLIB_FOUND)
  message(STATUS "Found system zlib: ${ZLIB_LIBRARIES}")
  check_zlib_deflate_fingerprint("${ZLIB_LIBRARIES}" "${ZLIB_INCLUDE_DIRS}")
  # The standard FindZLIB module already exposes the ZLIB::ZLIB imported target
  # and sets ZLIB_LIBRARIES / ZLIB_INCLUDE_DIRS. Promote them to the cache so the
  # rest of the tree sees them (this runs from the externals/ subdirectory).
  set(ZLIB_LIBRARIES    "${ZLIB_LIBRARIES}"    CACHE INTERNAL "zlib library")
  set(ZLIB_INCLUDE_DIRS "${ZLIB_INCLUDE_DIRS}" CACHE INTERNAL "zlib include dir")
else()
  message(STATUS "Building zlib from vendored sources.")

  set(ZLIB_SOURCES
    adler32.c crc32.c deflate.c infback.c inffast.c inflate.c inftrees.c
    trees.c zutil.c
    compress.c uncompr.c gzclose.c gzlib.c gzread.c gzwrite.c
  )
  list(TRANSFORM ZLIB_SOURCES PREPEND "${zlib_SOURCE_DIR}/")

  add_library(zlib STATIC EXCLUDE_FROM_ALL ${ZLIB_SOURCES})
  target_include_directories(zlib
    PUBLIC $<BUILD_INTERFACE:${zlib_SOURCE_DIR}>
           $<INSTALL_INTERFACE:include>
  )
  # options carried over verbatim from the previous bootstrap Makefile
  # CFLAGS ($(CVMFS_BASE_C_FLAGS) -O3 -D_LARGEFILE64_SOURCE=1 -DHAVE_HIDDEN
  # -DNO_vsnprintf -fPIC). CVMFS_BASE_C_FLAGS (incl. -fvisibility=hidden) come in
  # globally via CMAKE_C_FLAGS; -O3 is repeated here so it comes after the base -Os.
  target_compile_definitions(zlib PRIVATE
    _LARGEFILE64_SOURCE=1
    HAVE_HIDDEN
    NO_vsnprintf
  )
  # -fno-lto: the zlib symbols are hidden (-fvisibility=hidden -DHAVE_HIDDEN),
  # so with LTO the plugin sees only IR references to e.g. deflateInit_ and
  # internalizes it. That breaks as soon as the linker has to pull a member out
  # of libzlib.a *after* the LTO pass: on Fedora, libcrypto.so.3 leaves
  # compress()/uncompress() undefined (it gets them from libz.so.1 at runtime),
  # which drags compress.c.o and uncompr.c.o in at the end of the link, and
  # those then no longer find deflateInit_/deflateEnd. Keeping zlib out of LTO
  # is also what we want for the vendored copy anyway -- it exists to pin the
  # deflate output.
  target_compile_options(zlib PRIVATE -O3 -fno-lto)
  set_target_properties(zlib PROPERTIES
    POSITION_INDEPENDENT_CODE ON
  )

  add_library(ZLIB::ZLIB ALIAS zlib)

  set(ZLIB_LIBRARIES    zlib                  CACHE INTERNAL "zlib library")
  set(ZLIB_INCLUDE_DIRS "${zlib_SOURCE_DIR}"  CACHE INTERNAL "zlib include dir")

  install(TARGETS zlib DESTINATION ${EXTERNALS_INSTALL_LOCATION} COMPONENT external_deps EXCLUDE_FROM_ALL)
  install(FILES  ${zlib_SOURCE_DIR}/zlib.h ${zlib_SOURCE_DIR}/zconf.h DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL)
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "zlib" PARENT_SCOPE)
endif()
