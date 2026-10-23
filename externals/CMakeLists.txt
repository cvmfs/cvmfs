
# provides setup_external_build_mode()
include(${CMAKE_SOURCE_DIR}/cmake/Modules/ExternalDepMacros.cmake)

set(EXTERNALS_INSTALL_TARGETS "")

# libarchive is only used for tarball ingestion by the publishing tools
# (cvmfs_swissknife, cvmfs_publish, cvmfs_preload) and their unit tests
if(BUILD_CVMFS OR BUILD_SERVER OR BUILD_SERVER_DEBUG OR BUILD_RECEIVER OR
   BUILD_PRELOADER OR BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)
  add_subdirectory(libarchive)
endif()
add_subdirectory(json)
add_subdirectory(pacparser)
# zlib should be configured before protobuf. The CMake config for protobuf
# on some distros does its own find_package(ZLIB), and that can collide with
# our vendored installation unless zlib is configured first.
add_subdirectory(zlib)
add_subdirectory(protobuf)
add_subdirectory(sparsehash)
add_subdirectory(leveldb)
add_subdirectory(sqlite3)

# symmetric crypto / hashing primitives for libcvmfs_crypto
add_subdirectory(libnettle)

# google benchmark is only needed by the micro benchmarks
if (BUILD_UBENCHMARKS)
  add_subdirectory(googlebench)
endif ()

# maxminddb is the geo-api's python reader, only needed for the server geoapi
if ((BUILD_SERVER OR BUILD_SERVER_DEBUG) AND BUILD_GEOAPI)
  add_subdirectory(maxminddb)
endif()
# c-ares must be configured before libcurl: libcurl's build consumes the
# c-ares::cares target for its async DNS resolver.
add_subdirectory(c-ares)
add_subdirectory(libcurl)

add_custom_target(install_externals
  COMMAND ${CMAKE_COMMAND} --install ${CMAKE_BINARY_DIR}
     --component external_deps
     --prefix ${EXTERNALS_INSTALL_LOCATION}
  COMMENT "Installing external dependencies to ${EXTERNALS_INSTALL_LOCATION}"
  DEPENDS ${EXTERNALS_INSTALL_TARGETS}
)
