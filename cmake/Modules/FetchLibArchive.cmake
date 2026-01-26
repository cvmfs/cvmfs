# CMake module for vendoring libarchive

include(FetchContent)

set(LIBARCHIVE_VERSION "3.8.5")
set(LIBARCHIVE_URL "https://libarchive.org/downloads/libarchive-${LIBARCHIVE_VERSION}.tar.gz")
set(LIBARCHIVE_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/libarchive-${LIBARCHIVE_VERSION}.tar.gz")
set(LIBARCHIVE_HASH "MD5=2226a84d3720b1a3d00deb0d11530a60")


# -> libarchive <-
# NOTE: This specific version of libarchive exports `archive` and `archive_static` as build targets.
# NOTE: If upgrading to newer version verify target names and make sure they match with usage in cvmfs/CMakeLists.txt

# Configure libarchive
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME external_deps)
set(CMAKE_INSTALL_PREFIX ${EXTERNALS_INSTALL_LOCATION})
set(ENABLE_INSTALL  ON)
set(BUILD_SHARED_LIBS OFF)
set(BUILD_TESTING   OFF)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")

set(ENABLE_ACL      OFF)
set(ENABLE_BZip2    OFF)
set(ENABLE_CAT      OFF)
set(ENABLE_CNG      OFF)
set(ENABLE_CPIO     OFF)
set(ENABLE_EXPAT    OFF)
set(ENABLE_ICONV    OFF)
set(ENABLE_LIBXML2  OFF)
set(ENABLE_LZMA     OFF)
set(ENABLE_NETTLE   OFF)
set(ENABLE_OPENSSL  OFF)
set(ENABLE_TAR      OFF)
set(ENABLE_TEST     OFF)
set(ENABLE_XATTR    OFF)
set(ENABLE_ZLIB     OFF)

set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE NEVER)
# Declare libarchive
FetchContent_Declare(
  LibArchive
  URL "${LIBARCHIVE_URL} ${LIBARCHIVE_URL_MIRROR}"
  URL_HASH "${LIBARCHIVE_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  DOWNLOAD_DIR ${EXTERNALS_LIB_LOCATION}/download
  FIND_PACKAGE_ARGS
)

# Make libarchive available
FetchContent_MakeAvailable(LibArchive)

if(LibArchive_FOUND)
  message(STATUS "Found LibArchive at: ${LibArchive_LIBRARY}")
  set_target_properties(LibArchive::LibArchive PROPERTIES IMPORTED_GLOBAL TRUE)
else()
  message(STATUS "Building LibArchive from vendored sources.")
  add_library(LibArchive::LibArchive ALIAS archive_static)
endif()
