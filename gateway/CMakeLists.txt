
find_program(GO_COMPILER go)
if(NOT GO_COMPILER)
  message(FATAL_ERROR "go compiler not found")
else()
  message("found go compiler: ${GO_COMPILER}")
endif()

add_custom_target(
  cvmfs_gateway ALL
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_gateway
)

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_gateway
  COMMAND ${GO_COMPILER} build -mod=vendor -o ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_gateway
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*
  COMMENT "Build ducc using the Go Compiler"
)

install (
  PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_gateway
  DESTINATION "${CMAKE_INSTALL_PREFIX}/bin"
)


# and then we install the generated file
install (
  FILES ${CMAKE_BINARY_DIR}/cvmfs-gateway.service ${CMAKE_BINARY_DIR}/cvmfs-gateway@.service
  DESTINATION /usr/lib/systemd/system
)
