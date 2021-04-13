
find_program(GO_COMPILER go)
if(NOT GO_COMPILER)
  message(FATAL_ERROR "go compiler not found")
else()
  message("found go compiler: ${GO_COMPILER}")
endif()

add_custom_target(
  cvmfs_snapshotter ALL
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_snapshotter
)

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_snapshotter
  COMMAND ${GO_COMPILER} build -mod=vendor -o ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_snapshotter
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*
  COMMENT "Build containerd snapshotter using the Go Compiler"
)

install (
  PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_snapshotter
  DESTINATION "${CMAKE_INSTALL_PREFIX}/bin"
)
