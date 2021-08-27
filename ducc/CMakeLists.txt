
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

# we first generate the services file, replacing the installation directory in the template file
configure_file (
  ${CMAKE_CURRENT_SOURCE_DIR}/unitfiles/cvmfs_ducc.service.in
  ${CMAKE_BINARY_DIR}/cvmfs_ducc.service
  @ONLY
)

# and then we install the generated file
install (
  FILES ${CMAKE_BINARY_DIR}/cvmfs_ducc.service
  DESTINATION /usr/lib/systemd/system
)

install(
	CODE "execute_process(COMMAND setcap \"cap_dac_override=ep cap_dac_read_search=ep cap_fowner=ep cap_chown=ep cap_sys_admin=ep cap_mknod=ep\" ${CMAKE_INSTALL_PREFIX}/bin/cvmfs_ducc)"
)
