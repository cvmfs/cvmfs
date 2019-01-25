
add_custom_target(cvmfs_ducc go build -o cvmfs_ducc
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Build ducc using the Go Compiler")

install (
  PROGRAMS "${CMAKE_CURRENT_SOURCE_DIR}/cvmfs_ducc"
  DESTINATION "${CMAKE_INSTALL_PREFIX}/bin"
)
