add_custom_target(
  cvmfs_validator_target ALL
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_validator
)

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_validator
  COMMAND ${GO_COMPILER} build -o ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_validator .
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS validate.go cvmfs_client.cue cvmfs_server.cue go.mod go.sum
  COMMENT "Build cvmfs_validator using the Go compiler"
)

install (
  PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_validator
  DESTINATION ${CVMFS_LIBEXEC_DIR}
)
