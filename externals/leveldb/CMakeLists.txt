# CMake module for vendoring leveldb
include(FetchContent)

set(LEVELDB_VERSION "1.23")
set(LEVELDB_URL "https://github.com/google/leveldb/archive/refs/tags/${LEVELDB_VERSION}.tar.gz")
set(LEVELDB_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/leveldb-${LEVELDB_VERSION}.tar.gz")
set(LEVELDB_HASH "MD5=afbde776fb8760312009963f09a586c7")

# -> leveldb <-
# Used for the persistent inode<->path maps in NFS-export mode
# (cvmfs/nfs_maps_leveldb.cc).
#
# Since 1.21 leveldb ships a native CMakeLists.txt and exports the
# leveldb::leveldb target, so we no longer need the hand-rolled Makefile hooks
# and the three patches we carried for 1.18:
#   * arm64_memory_barrier.patch  - obsolete: 1.20+ dropped port/atomic_pointer.h
#                                    and its custom MemoryBarrier() for C++11 <atomic>.
#   * dont_search_snappy.patch    - obsolete: the build_detect_platform probe is
#   * dont_search_tcmalloc.patch     gone; optional deps are now controlled by the
#                                    HAVE_SNAPPY / HAVE_TCMALLOC cache variables we
#                                    force OFF below (keeps the static lib self-contained).

setup_external_build_mode(leveldb)

# Keep the vendored static leveldb self-contained: don't pick up snappy, tcmalloc
# or an external crc32c from the build host (leveldb has a built-in crc32c
# fallback). This reproduces the intent of the old dont_search_* patches.
set(HAVE_SNAPPY   OFF CACHE BOOL "" FORCE)
set(HAVE_TCMALLOC OFF CACHE BOOL "" FORCE)
set(HAVE_CRC32C   OFF CACHE BOOL "" FORCE)

# leveldb's tests/benchmarks pull in the googletest/benchmark git submodules,
# which are not part of the release tarball; keep them off. We install the
# static archive and headers ourselves (below), so LEVELDB_INSTALL is off too.
set(LEVELDB_BUILD_TESTS      OFF CACHE BOOL "" FORCE)
set(LEVELDB_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(LEVELDB_INSTALL          OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
  leveldb
  URL "${LEVELDB_URL}" "${LEVELDB_URL_MIRROR}"
  URL_HASH "${LEVELDB_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
  INSTALL_DIR ${EXTERNALS_INSTALL_LOCATION}
  EXCLUDE_FROM_ALL
  FIND_PACKAGE_ARGS
)

FetchContent_MakeAvailable(leveldb)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if(leveldb_FOUND)
  message(STATUS "Found system leveldb: ${LEVELDB_LIBRARIES}")
  # Findleveldb.cmake exposes the uniform leveldb::leveldb imported target.
else()
  message(STATUS "Building leveldb from vendored sources.")

  # leveldb defines the target as plain "leveldb"; expose the same namespaced
  # target the system/config path provides, so consumers link leveldb::leveldb
  # unconditionally.
  if (NOT TARGET leveldb::leveldb)
    add_library(leveldb::leveldb ALIAS leveldb)
  endif ()

  # cvmfs links leveldb into the SHARED fuse client, so the static archive must
  # be position-independent.
  set_target_properties(leveldb PROPERTIES POSITION_INDEPENDENT_CODE ON)

  # leveldb's CMakeLists forces -fno-rtti, so it never emits the typeinfo for its
  # polymorphic classes. cvmfs subclasses leveldb::EnvWrapper (ForkAwareEnv in
  # nfs_maps_leveldb.cc) with RTTI on, which references
  # "typeinfo for leveldb::EnvWrapper"; without it libcvmfs_fuse.so fails to load
  # (undefined symbol _ZTIN7leveldb10EnvWrapperE) and every mount fails. Re-enable
  # RTTI for the vendored build: the later -frtti overrides leveldb's -fno-rtti.
  target_compile_options(leveldb PRIVATE -frtti)

  # Don't let leveldb's warning flags leak into cvmfs sources. On clang leveldb
  # puts "-Werror -Wthread-safety" on its PUBLIC (interface) compile options, so
  # they would be applied to every cvmfs translation unit in a target that links
  # leveldb::leveldb - turning cvmfs's own warnings into hard errors. cvmfs only
  # needs leveldb's headers, not its build flags.
  set_target_properties(leveldb PROPERTIES INTERFACE_COMPILE_OPTIONS "")

  install(TARGETS leveldb DESTINATION ${EXTERNALS_INSTALL_LOCATION}/lib COMPONENT external_deps EXCLUDE_FROM_ALL)
  install(DIRECTORY ${leveldb_SOURCE_DIR}/include/leveldb DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL)
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "leveldb" PARENT_SCOPE)
endif()
