
add_custom_target(ducc make
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Build ducc using the Go Compiler")

install (
  PROGRAMS "${CMAKE_CURRENT_SOURCE_DIR}/ducc"
  DESTINATION "${CMAKE_INSTALL_PREFIX}/bin"
)
