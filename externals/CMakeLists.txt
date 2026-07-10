
# provides setup_external_build_mode()
include(${CMAKE_SOURCE_DIR}/cmake/Modules/ExternalDepMacros.cmake)

set(EXTERNALS_INSTALL_TARGETS "")

add_subdirectory(libarchive)
add_subdirectory(json)
add_subdirectory(pacparser)
add_subdirectory(protobuf)
add_subdirectory(sparsehash)
add_subdirectory(leveldb)
add_subdirectory(sqlite3)
add_subdirectory(zlib)

# maxminddb is the geo-api's python reader, only needed for the server geoapi
if ((BUILD_SERVER OR BUILD_SERVER_DEBUG) AND BUILD_GEOAPI)
  add_subdirectory(maxminddb)
endif()

add_custom_target(install_externals
  COMMAND ${CMAKE_COMMAND} --install ${CMAKE_BINARY_DIR}
     --component external_deps
     --prefix ${EXTERNALS_INSTALL_LOCATION}
  COMMENT "Installing external dependencies to ${EXTERNALS_INSTALL_LOCATION}"
  DEPENDS ${EXTERNALS_INSTALL_TARGETS}
)
