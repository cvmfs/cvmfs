
add_custom_target(
  cvmfs_snapshotter_target ALL
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_snapshotter
)

file(GLOB_RECURSE CVMFS_SNAPSHOTTER_GO_SOURCES LIST_DIRECTORIES false ./*.go)

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_snapshotter
  COMMAND ${GO_COMPILER} build -mod=vendor -o ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_snapshotter -ldflags='-X main.Version=${CernVM-FS_VERSION_STRING}'
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${CVMFS_SNAPSHOTTER_GO_SOURCES}
  COMMENT "Build containerd cvmfs_snapshotter using the Go Compiler"
)

install (
  PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_snapshotter
  DESTINATION "${CMAKE_INSTALL_PREFIX}/bin"
)

# Systemd service unit
install (
  FILES script/config/etc/systemd/system/cvmfs-snapshotter.service
  DESTINATION /usr/lib/systemd/system
  PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
)
