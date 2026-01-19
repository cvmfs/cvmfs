# CMake module for vendored dependencies
include(FetchContent)

# Required to maintain backwords compatibility with vendored dependencies
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)

set(LIBARCHIVE_VERSION "3.3.2")
set(LIBARCHIVE_LOCAL_PATH "${CMAKE_SOURCE_DIR}/externals/libarchive/libarchive-${LIBARCHIVE_VERSION}.tar.gz")
set(LIBARCHIVE_URL "")

# Resolve local path/URL of archive
function(get_source_url NAME LOCAL_PATH REMOTE_URL RESULT_VAR)
  if(EXISTS "${LOCAL_PATH}")
    message(STATUS "${NAME}: Using local archive: ${LOCAL_PATH}")
    set(${RESULT_VAR} "${LOCAL_PATH}" PARENT_SCOPE)
  elseif(NOT "${REMOTE_URL}" STREQUAL "")
    message(STATUS "${NAME}: Using remote URL: ${REMOTE_URL}")
    set(${RESULT_VAR} "${REMOTE_URL}" PARENT_SCOPE)
  else()
    message(FATAL_ERROR "${NAME}: Local archive not found and no remote URL provided!")
  endif()
endfunction()

# -> libarchive <-
# NOTE: This specific version of libarchive exports `archive` and `archive_static` as build targets.
# NOTE: If upgrading to newer verion verify target names and make sure they match with usage in cvmfs/CMakeLists.txt

get_source_url("libarchive" "${LIBARCHIVE_LOCAL_PATH}" "${LIBARCHIVE_URL}" LIBARCHIVE_SRC)
FetchContent_Declare(
  LibArchive
  URL "${LIBARCHIVE_SRC}"
  PATCH_COMMAND patch -p0 < "${CMAKE_SOURCE_DIR}/externals/libarchive/src/fix-new-glibc.patch" &&
                patch -p0 < "${CMAKE_SOURCE_DIR}/externals/libarchive/src/libarchive_cmake.patch"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

FetchContent_MakeAvailable(LibArchive)
