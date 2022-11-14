
add_custom_target(
  cvmfs_gateway_target ALL
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_gateway
)

file(GLOB_RECURSE CVMFS_GATEWAY_GO_SOURCES LIST_DIRECTORIES false ./*.go)

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_gateway
  COMMAND ${GO_COMPILER} build -mod=vendor -o ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_gateway -ldflags='-X main.Version=${CernVM-FS_VERSION_STRING}'
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${CVMFS_GATEWAY_GO_SOURCES}
  COMMENT "Build gateway using the Go Compiler"
)

install (
  PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_gateway
  DESTINATION "${CMAKE_INSTALL_PREFIX}/bin"
)

install (
  PROGRAMS pkg/run_cvmfs_gateway.sh
  DESTINATION "${CVMFS_LIBEXEC_DIR}-gateway/scripts"
)

# Systemd service unit
install (
  FILES pkg/cvmfs-gateway.service pkg/cvmfs-gateway@.service
  DESTINATION /usr/lib/systemd/system
  PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
)

install (
  FILES         config/repo.json config/user.json
  DESTINATION   "${SYSCONF_INSTALL_DIR}/cvmfs/gateway"
  PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
)
