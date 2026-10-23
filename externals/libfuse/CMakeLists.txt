# CMake module for vendoring the libfuse public headers
include(FetchContent)

set(LIBFUSE_VERSION "3.18.2")
set(LIBFUSE_URL "https://github.com/libfuse/libfuse/archive/refs/tags/fuse-${LIBFUSE_VERSION}.tar.gz")
set(LIBFUSE_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/libfuse-${LIBFUSE_VERSION}.tar.gz")
set(LIBFUSE_HASH "MD5=be3088b3f093e50145a00af86b453809")

# libcvmfs_nfsfuse implements the libfuse API itself, so only the headers are
# needed. Never taken from the system: an installed libfuse or macFUSE may
# declare a different ABI than the one libcvmfs_nfsfuse implements.
#
# The result is exposed as CVMFS_NFS_FUSE3_INCLUDE_DIR (contains fuse3/).

FetchContent_Declare(
  libfuse
  URL "${LIBFUSE_URL}" "${LIBFUSE_URL_MIRROR}"
  URL_HASH "${LIBFUSE_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
)

# libfuse has no CMakeLists.txt (meson), so this only downloads and extracts
FetchContent_MakeAvailable(libfuse)

set(CVMFS_NFS_FUSE3_INCLUDE_DIR "${libfuse_BINARY_DIR}/include"
    CACHE INTERNAL "libfuse headers for the NFS frontend")
file(GLOB _libfuse_headers ${libfuse_SOURCE_DIR}/include/*.h)
file(COPY ${_libfuse_headers} ${CMAKE_CURRENT_SOURCE_DIR}/libfuse_config.h
     DESTINATION ${CVMFS_NFS_FUSE3_INCLUDE_DIR}/fuse3)
