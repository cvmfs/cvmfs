
find_program(GO_COMPILER go)
if(NOT GO_COMPILER)
  message(FATAL_ERROR "go compiler not found")
else()
  message("found go compiler: ${GO_COMPILER}")
endif()

add_custom_target(
  cvmfs_ducc ALL
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_ducc  
)

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_ducc  
  COMMAND ${GO_COMPILER} build -mod=vendor -o ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_ducc
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*
  COMMENT "Build ducc using the Go Compiler"
)

install (
  PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_ducc
  DESTINATION "${CMAKE_INSTALL_PREFIX}/bin"
)
