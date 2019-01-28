
find_program(GO_COMPILER go)
if(NOT GO_COMPILER)
  message(FATAL_ERROR "go compiler not found")
else()
  message("found go compiler: ${GO_COMPILER}")
endif()

add_custom_target(cvmfs_ducc ${GO_COMPILER} build -o cvmfs_ducc
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Build ducc using the Go Compiler")

install (
  PROGRAMS "${CMAKE_CURRENT_SOURCE_DIR}/cvmfs_ducc"
  DESTINATION "${CMAKE_INSTALL_PREFIX}/bin"
)
