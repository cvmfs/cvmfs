
add_custom_target(
  cvmfs_ducc_target ALL
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_ducc
)

file(GLOB_RECURSE CVMFS_CONTAINER_TOOLS_GO_SOURCES LIST_DIRECTORIES false ./*.go)

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_ducc
  COMMAND ${GO_COMPILER} build -mod=vendor -o ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_ducc -ldflags='-X github.com/cvmfs/ducc/cmd.Version=${CernVM-FS_VERSION_STRING}'
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${CVMFS_CONTAINER_TOOLS_GO_SOURCES}
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
  DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/systemd/system
)

install(
	CODE "execute_process(COMMAND setcap \"cap_dac_override=ep cap_dac_read_search=ep cap_fowner=ep cap_chown=ep cap_sys_admin=ep cap_mknod=ep\" ${CMAKE_INSTALL_PREFIX}/bin/cvmfs_ducc)"
)

install (
    FILES webhook/registry_webhook.py
    DESTINATION ${CVMFS_LIBEXEC_DIR}/ducc
    PERMISSIONS OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
)
