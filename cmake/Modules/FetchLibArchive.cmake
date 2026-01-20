# CMake module for vendoring libarchive

include(FetchContent)

set(LIBARCHIVE_VERSION "3.8.0")
set(LIBARCHIVE_LOCAL_PATH "${CMAKE_SOURCE_DIR}/externals/libarchive/libarchive-${LIBARCHIVE_VERSION}.tar.gz")
set(LIBARCHIVE_URL "https://libarchive.org/downloads/libarchive-${LIBARCHIVE_VERSION}.tar.gz")
set(LIBARCHIVE_HASH "MD5=d3ed99350b47a53d60ae629160726134")

# Resolve source URL
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
# NOTE: If upgrading to newer version verify target names and make sure they match with usage in cvmfs/CMakeLists.txt

# Configure libarchive (as per `configureHook.sh`)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
set(ENABLE_INSTALL  OFF CACHE BOOL "" FORCE)
set(ENABLE_ACL      OFF CACHE BOOL "" FORCE)
set(ENABLE_BZip2    OFF CACHE BOOL "" FORCE)
set(ENABLE_CAT      OFF CACHE BOOL "" FORCE)
set(ENABLE_CNG      OFF CACHE BOOL "" FORCE)
set(ENABLE_CPIO     OFF CACHE BOOL "" FORCE)
set(ENABLE_EXPAT    OFF CACHE BOOL "" FORCE)
set(ENABLE_ICONV    OFF CACHE BOOL "" FORCE)
set(ENABLE_LIBXML2  OFF CACHE BOOL "" FORCE)
set(ENABLE_LZMA     OFF CACHE BOOL "" FORCE)
set(ENABLE_NETTLE   OFF CACHE BOOL "" FORCE)
set(ENABLE_OPENSSL  OFF CACHE BOOL "" FORCE)
set(ENABLE_TAR      OFF CACHE BOOL "" FORCE)
set(ENABLE_TEST     OFF CACHE BOOL "" FORCE)
set(ENABLE_XATTR    OFF CACHE BOOL "" FORCE)
set(ENABLE_ZLIB     OFF CACHE BOOL "" FORCE)

# Declare libarchive
get_source_url("libarchive" "${LIBARCHIVE_LOCAL_PATH}" "${LIBARCHIVE_URL}" LIBARCHIVE_SRC)
FetchContent_Declare(
  LibArchive
  URL "${LIBARCHIVE_SRC}"
  URL_HASH "${LIBARCHIVE_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

# Make libarchive available
FetchContent_MakeAvailable(LibArchive)

# Unset variables from cache to avoid bleeding into other packages
unset(ENABLE_INSTALL  CACHE)
unset(ENABLE_ACL      CACHE)
unset(ENABLE_BZip2    CACHE)
unset(ENABLE_CAT      CACHE)
unset(ENABLE_CNG      CACHE)
unset(ENABLE_CPIO     CACHE)
unset(ENABLE_EXPAT    CACHE)
unset(ENABLE_ICONV    CACHE)
unset(ENABLE_LIBXML2  CACHE)
unset(ENABLE_LZMA     CACHE)
unset(ENABLE_NETTLE   CACHE)
unset(ENABLE_OPENSSL  CACHE)
unset(ENABLE_TAR      CACHE)
unset(ENABLE_TEST     CACHE)
unset(ENABLE_XATTR    CACHE)
unset(ENABLE_ZLIB     CACHE)
