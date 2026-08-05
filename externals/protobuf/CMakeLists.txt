# CMake module for vendoring protobuf
include(FetchContent)

set(PROTOBUF_VERSION "3.21.12")
set(PROTOBUF_URL "https://github.com/protocolbuffers/protobuf/releases/download/v21.12/protobuf-cpp-${PROTOBUF_VERSION}.tar.gz")
set(PROTOBUF_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/protobuf-cpp-${PROTOBUF_VERSION}.tar.gz")
set(PROTOBUF_HASH "MD5=6b4fd9cee2fa63834f29c7d433679855")

# -> protobuf <-
# We only need protobuf-lite (cache.proto uses optimize_for = LITE_RUNTIME) and
# the protoc compiler to generate cache.pb.{h,cc}. Unlike protobuf 2.6.1, which
# only shipped autotools, 3.21.x builds natively with CMake. 3.21.12 is also the
# last release before the Abseil dependency, so it still builds under C++11.
#
# Discovery is done explicitly with the built-in FindProtobuf *module* (capital
# "Protobuf") rather than FetchContent's FIND_PACKAGE_ARGS integration: the
# latter would call find_package(protobuf) (lower case, CONFIG mode only), which
# misses distro packages that ship no protobuf-config.cmake (e.g. EL9/EL10 only
# provide the module + protobuf.pc).

# Honour the same three modes as the other externals:
#   NEVER  (BUILTIN_EXTERNALS_LIST)    -> always build from source
#   ALWAYS (BUILTIN_EXTERNALS_EXCLUDE) -> require the system package
#   OPT_IN (default)                   -> try the system first, else source
setup_external_build_mode(protobuf)

# Clear our own re-exported cache entries before discovery. They reuse
# FindProtobuf's legacy all-caps variable names (PROTOBUF_LITE_LIBRARY etc.) and
# hold *target names* like "protobuf::libprotobuf-lite", not library paths. On a
# re-configure FindProtobuf's backward-compat shim copies the stale all-caps
# value into its modern Protobuf_LITE_LIBRARY and then "honors" it instead of
# searching, fabricating an imported protobuf::libprotobuf-lite target with no
# IMPORTED_LOCATION -> "IMPORTED_LOCATION not set" at generate time. Unsetting
# them here makes every configure search from a clean slate; we repopulate them
# with the correct values at the end of this module.
unset(PROTOBUF_LITE_LIBRARY      CACHE)
unset(PROTOBUF_PROTOC_EXECUTABLE CACHE)
unset(PROTOBUF_INCLUDE_DIRS      CACHE)

if(NOT FETCHCONTENT_TRY_FIND_PACKAGE_MODE STREQUAL "NEVER")
  if(FETCHCONTENT_TRY_FIND_PACKAGE_MODE STREQUAL "ALWAYS")
    find_package(Protobuf REQUIRED)
  else()
    find_package(Protobuf)
  endif()
endif()

if(Protobuf_FOUND)
  message(STATUS "Found system protobuf ${Protobuf_VERSION}")
  # Fedora/EL ship only the full libprotobuf (the lite runtime is bundled into
  # it), so the module exposes no protobuf::libprotobuf-lite. LITE_RUNTIME
  # generated code links fine against the full library, so fall back to it.
  if(TARGET protobuf::libprotobuf-lite)
    set(_pb_lite_target protobuf::libprotobuf-lite)
  else()
    message(STATUS "System protobuf has no separate lite runtime; "
                   "using the full libprotobuf.")
    set(_pb_lite_target protobuf::libprotobuf)
  endif()
  # Imported targets from find_package are directory-scoped; promote the ones we
  # re-export so sibling directories (e.g. cvmfs/) can link them.
  set_target_properties(${_pb_lite_target} PROPERTIES IMPORTED_GLOBAL TRUE)
  get_target_property(PROTOBUF_INCLUDE_DIRS ${_pb_lite_target}
                      INTERFACE_INCLUDE_DIRECTORIES)

  set(_pb_lite_library     "${_pb_lite_target}")
  # Use the system protoc binary directly: it is not a build target and matches
  # the system runtime we link against.
  set(_pb_protoc_executable "${Protobuf_PROTOC_EXECUTABLE}")
else()
  message(STATUS "Building protobuf ${PROTOBUF_VERSION} from vendored sources.")

  # Build only what we need: a static protobuf-lite plus the protoc compiler, no
  # zlib, tests, examples or install rules.
  set(protobuf_BUILD_TESTS           OFF CACHE BOOL "" FORCE)
  set(protobuf_BUILD_CONFORMANCE     OFF CACHE BOOL "" FORCE)
  set(protobuf_BUILD_EXAMPLES        OFF CACHE BOOL "" FORCE)
  set(protobuf_BUILD_SHARED_LIBS     OFF CACHE BOOL "" FORCE)
  set(protobuf_WITH_ZLIB             OFF CACHE BOOL "" FORCE)
  set(protobuf_INSTALL               OFF CACHE BOOL "" FORCE)
  set(protobuf_BUILD_PROTOC_BINARIES ON  CACHE BOOL "" FORCE)
  # protobuf-lite is statically linked into the (shared) fuse client, so it must
  # be position independent.
  set(CMAKE_POSITION_INDEPENDENT_CODE ON)

  FetchContent_Declare(
    protobuf
    URL "${PROTOBUF_URL}" "${PROTOBUF_URL_MIRROR}"
    URL_HASH "${PROTOBUF_HASH}"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
    INSTALL_DIR ${EXTERNALS_INSTALL_LOCATION}
    EXCLUDE_FROM_ALL
  )
  FetchContent_MakeAvailable(protobuf)

  set(PROTOBUF_INCLUDE_DIRS "${protobuf_SOURCE_DIR}/src")
  set(_pb_lite_library      protobuf::libprotobuf-lite)
  set(_pb_protoc_executable protobuf::protoc)

  # protobuf is third-party: silence all warnings from its sources so they do
  # not drown out cvmfs's own -Wall output. -w comes after the project-wide
  # -Wall in CMAKE_CXX_FLAGS, so it wins.
  foreach(_pb_target IN ITEMS libprotobuf-lite libprotobuf libprotoc protoc)
    if(TARGET ${_pb_target})
      target_compile_options(${_pb_target} PRIVATE -w)
    endif()
  endforeach()
endif()

unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

# Backwards-compatible variables consumed across the tree (cvmfs/, test/*):
#   PROTOBUF_LITE_LIBRARY      link target (vendored lite, or system full lib)
#   PROTOBUF_PROTOC_EXECUTABLE protoc build target or system binary path
#   PROTOBUF_INCLUDE_DIRS      protobuf headers
set(PROTOBUF_LITE_LIBRARY      "${_pb_lite_library}"      CACHE STRING "protobuf-lite link target" FORCE)
set(PROTOBUF_PROTOC_EXECUTABLE "${_pb_protoc_executable}" CACHE STRING "protoc compiler"           FORCE)
set(PROTOBUF_INCLUDE_DIRS      "${PROTOBUF_INCLUDE_DIRS}" CACHE STRING "protobuf include dirs"      FORCE)
mark_as_advanced(PROTOBUF_LITE_LIBRARY PROTOBUF_PROTOC_EXECUTABLE PROTOBUF_INCLUDE_DIRS)
