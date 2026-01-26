
include(${CMAKE_SOURCE_DIR}/cmake/Modules/FetchLibArchive.cmake)

add_custom_target(install_externals
  COMMAND ${CMAKE_COMMAND} --install ${CMAKE_BINARY_DIR} 
     --component external_deps 
     --prefix ${EXTERNALS_INSTALL_LOCATION}
  COMMENT "Installing external dependencies to ${EXTERNALS_INSTALL_LOCATION}"
  DEPENDS archive_static #TODO replace with a list that gets populated for each external
)
