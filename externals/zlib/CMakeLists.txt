# CMake module for vendoring zlib
include(FetchContent)

# zlib 1.2.8 -- kept byte-for-byte identical to the old bootstrap.sh build
# (MD5 44d667c142d7cda120332623eab69f40). The upstream release archive lives in
# zlib.net's "fossils" area now that 1.2.8 is superseded; the CERN mirror is the
# reliable fallback used by the other externals.
set(ZLIB_VERSION "1.2.8")
set(ZLIB_URL "https://zlib.net/fossils/zlib-${ZLIB_VERSION}.tar.gz")
set(ZLIB_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/zlib-${ZLIB_VERSION}.tar.gz")
set(ZLIB_HASH "MD5=44d667c142d7cda120332623eab69f40")

# -> zlib <-
# NOTE: we deliberately do NOT use zlib's own configure/CMake build. As in the
# old externals/zlib/src/Makefile.1211 we compile the sources ourselves into a
# single static libz.a with the cvmfs-specific tuning flags below, so the result
# is identical to the bootstrap.sh build. Doing it as a normal CMake target
# means the build honours CMAKE_C_COMPILER, toolchain files and the sysroot, so
# cross-compilation works out of the box.

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
  FIND_PACKAGE_ARGS
)

FetchContent_MakeAvailable(ZLIB)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if(ZLIB_FOUND)
  message(STATUS "Found system zlib: ${ZLIB_LIBRARIES}")
  # The standard FindZLIB module already exposes the ZLIB::ZLIB imported target
  # and sets ZLIB_LIBRARIES / ZLIB_INCLUDE_DIRS. Promote them to the cache so the
  # rest of the tree sees them (this runs from the externals/ subdirectory).
  set(ZLIB_LIBRARIES    "${ZLIB_LIBRARIES}"    CACHE INTERNAL "zlib library")
  set(ZLIB_INCLUDE_DIRS "${ZLIB_INCLUDE_DIRS}" CACHE INTERNAL "zlib include dir")
else()
  message(STATUS "Building zlib from vendored sources.")

  # Object list carried over verbatim from Makefile.1211 (OBJZ + OBJG): the
  # static-library sources only, no gz* asm and no shared-lib build.
  set(ZLIB_SOURCES
    adler32.c crc32.c deflate.c infback.c inffast.c inflate.c inftrees.c
    trees.c zutil.c
    compress.c uncompr.c gzclose.c gzlib.c gzread.c gzwrite.c
  )
  list(TRANSFORM ZLIB_SOURCES PREPEND "${zlib_SOURCE_DIR}/")

  # EXCLUDE_FROM_ALL: only build when a consumer links it. Matches the other
  # externals.
  add_library(zlib STATIC EXCLUDE_FROM_ALL ${ZLIB_SOURCES})
  target_include_directories(zlib
    PUBLIC $<BUILD_INTERFACE:${zlib_SOURCE_DIR}>
           $<INSTALL_INTERFACE:include>
  )
  # cvmfs-specific tuning, carried over verbatim from the old externals Makefile
  # CFLAGS ($(CVMFS_BASE_C_FLAGS) -O3 -D_LARGEFILE64_SOURCE=1 -DHAVE_HIDDEN
  # -DNO_vsnprintf -fPIC). CVMFS_BASE_C_FLAGS (incl. -fvisibility=hidden) come in
  # globally via CMAKE_C_FLAGS; -O3 is repeated here so it wins over the base -Os.
  target_compile_definitions(zlib PRIVATE
    _LARGEFILE64_SOURCE=1
    HAVE_HIDDEN
    NO_vsnprintf
  )
  target_compile_options(zlib PRIVATE -O3)
  set_target_properties(zlib PROPERTIES
    POSITION_INDEPENDENT_CODE ON
  )

  add_library(ZLIB::ZLIB ALIAS zlib)

  # Back-compat: the tree consumes zlib through the ${ZLIB_LIBRARIES} /
  # ${ZLIB_INCLUDE_DIRS} variables (like the old find_package(ZLIB)). Point them
  # at the target and the fetched sources. CACHE INTERNAL so they are visible in
  # the parent scope (this runs from the externals/ subdirectory).
  set(ZLIB_LIBRARIES    zlib                  CACHE INTERNAL "zlib library")
  set(ZLIB_INCLUDE_DIRS "${zlib_SOURCE_DIR}"  CACHE INTERNAL "zlib include dir")

  install(TARGETS zlib DESTINATION ${EXTERNALS_INSTALL_LOCATION} COMPONENT external_deps EXCLUDE_FROM_ALL)
  install(FILES  ${zlib_SOURCE_DIR}/zlib.h ${zlib_SOURCE_DIR}/zconf.h DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL)
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "zlib" PARENT_SCOPE)
endif()
