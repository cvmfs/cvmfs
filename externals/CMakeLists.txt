function(setup_external_build_mode PACKAGE_NAME)
  # convert package name and lists to lowercase for robust comparison
  string(TOLOWER "${PACKAGE_NAME}" _pkg_lower)
  string(TOLOWER "${BUILTIN_EXTERNALS_LIST}" _list_lower)
  string(TOLOWER "${BUILTIN_EXTERNALS_EXCLUDE}" _exclude_lower)

  if("${_pkg_lower}" IN_LIST _exclude_lower)
    message(STATUS "External [${PACKAGE_NAME}]: Mode ALWAYS (System)")
    set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE ALWAYS PARENT_SCOPE)
  elseif("${_pkg_lower}" IN_LIST _list_lower)
    message(STATUS "External [${PACKAGE_NAME}]: Mode NEVER (Source)")
    set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE NEVER PARENT_SCOPE)
  else()
    message(STATUS "External [${PACKAGE_NAME}]: Mode OPTIONAL (Try System First)")
    set(FETCHCONTENT_TRY_FIND_PACKAGE_MODE OPT_IN PARENT_SCOPE)
  endif()
endfunction()

include(${CMAKE_SOURCE_DIR}/cmake/Modules/FetchLibArchive.cmake)

add_custom_target(install_externals
  COMMAND ${CMAKE_COMMAND} --install ${CMAKE_BINARY_DIR}
     --component external_deps
     --prefix ${EXTERNALS_INSTALL_LOCATION}
  COMMENT "Installing external dependencies to ${EXTERNALS_INSTALL_LOCATION}"
  DEPENDS archive_static #TODO replace with a list that gets populated for each external
)
