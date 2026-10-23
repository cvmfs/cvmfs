# CMake module for vendoring c-ares
include(FetchContent)

set(CARES_VERSION "1.34.6")
set(CARES_URL "https://github.com/c-ares/c-ares/releases/download/v${CARES_VERSION}/c-ares-${CARES_VERSION}.tar.gz")
set(CARES_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/c-ares-${CARES_VERSION}.tar.gz")
set(CARES_HASH "MD5=85fda96ad3859edb262f68284d11b971")

# -> c-ares <-
# NOTE: c-ares exports the alias target `c-ares::cares`. We resolve it to the
# NOTE: real build target below and use that everywhere, because the alias works
# NOTE: in neither install(TARGETS) nor curl's feature-check try_compile()s: a
# NOTE: `::` alias to a normal (non-imported) target is not resolvable in
# NOTE: try_compile's LINK_LIBRARIES, so curl's check_library_exists() calls
# NOTE: (which carry CARES_LIBRARY along) fail with "target not found". The real
# NOTE: target links fine in both. Its concrete name depends on the build config:
# NOTE: the `_static` suffix is only defined when a shared library is *also*
# NOTE: built, so in this static-only build (CARES_SHARED OFF) the target is
# NOTE: plain `c-ares`, not `c-ares_static`. Resolving keeps this robust across
# NOTE: upgrades.

# Configure c-ares: build a single static, position-independent library and
# skip the command line tools and test suite.
# c-ares caps its policy version at 3.10 (CMAKE_MINIMUM_REQUIRED VERSION
# 3.5.0...3.10.0), leaving CMP0077 (added in 3.13) at OLD inside its scope.
# Under OLD, c-ares's own option() calls overwrite the plain variables we set
# below — so without this policy override CARES_SHARED reverts to its ON default
# and c-ares builds a shared libcares.so instead of the static lib we want.
# Force CMP0077 to NEW so the option() calls honor the values passed in here.
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME external_deps)
set(CARES_STATIC     ON)
set(CARES_SHARED     OFF)
set(CARES_STATIC_PIC ON)
set(CARES_BUILD_TESTS           OFF)
set(CARES_BUILD_CONTAINER_TESTS OFF)
set(CARES_BUILD_TOOLS           OFF)
set(CARES_INSTALL               OFF)

setup_external_build_mode(c-ares)

FetchContent_Declare(
  c-ares
  URL "${CARES_URL}" "${CARES_URL_MIRROR}"
  URL_HASH "${CARES_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
  INSTALL_DIR ${EXTERNALS_INSTALL_LOCATION}
  EXCLUDE_FROM_ALL
  FIND_PACKAGE_ARGS
)

FetchContent_MakeAvailable(c-ares)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if(c-ares_FOUND)
  message(STATUS "Found c-ares target")
  set_target_properties(c-ares::cares PROPERTIES IMPORTED_GLOBAL TRUE)
else()
  message(STATUS "Building c-ares from vendored sources.")
  # Hand the in-tree c-ares to libcurl's bundled FindCARES.cmake: pre-populating
  # these cache entries makes its find_path()/find_library() no-ops, so curl links
  # our target instead of searching the system. Include dirs (incl. the generated
  # ares_build.h) also propagate to curl via the target's INTERFACE properties.
  # Resolve the c-ares::cares alias to the concrete library target (see note above).
  get_target_property(CARES_REAL_TARGET c-ares::cares ALIASED_TARGET)
  set(CARES_INCLUDE_DIR "${c-ares_SOURCE_DIR}/include" CACHE PATH "c-ares include dir (vendored)" FORCE)
  set(CARES_LIBRARY     "${CARES_REAL_TARGET}"         CACHE STRING "c-ares library (vendored)" FORCE)
  install(TARGETS ${CARES_REAL_TARGET} DESTINATION ${EXTERNALS_INSTALL_LOCATION} COMPONENT external_deps EXCLUDE_FROM_ALL)
  install(DIRECTORY ${c-ares_SOURCE_DIR}/include/ DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL FILES_MATCHING PATTERN "*.h")
  install(FILES ${c-ares_BINARY_DIR}/ares_build.h DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL OPTIONAL)
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "${CARES_REAL_TARGET}" PARENT_SCOPE)
endif()
