# -*- cmake -*-

# - Find pacparser (system installation)
# Used for the system-fallback path (e.g. BUILTIN_EXTERNALS_EXCLUDE=pacparser or
# BUILTIN_EXTERNALS=OFF). Defines the uniform imported target
#
#   pacparser::pacparser
#
# and, for FetchContent's FIND_PACKAGE_ARGS integration, the result variable
# pacparser_FOUND. Also sets, for backwards compatibility:
#   PACPARSER_INCLUDE_DIR   where to find pacparser.h
#   PACPARSER_LIBRARIES     the libraries needed to use pacparser

find_path(PACPARSER_INCLUDE_DIR pacparser.h)
find_library(PACPARSER_LIBRARY NAMES pacparser)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(pacparser
  REQUIRED_VARS PACPARSER_LIBRARY PACPARSER_INCLUDE_DIR)

if (pacparser_FOUND)
  set(PACPARSER_LIBRARIES ${PACPARSER_LIBRARY})
  set(PACPARSER_INCLUDE_DIRS ${PACPARSER_INCLUDE_DIR})
  if (NOT TARGET pacparser::pacparser)
    # GLOBAL so the target is usable from sibling directories (e.g. cvmfs/),
    # since this module may be invoked from the externals/ subdirectory.
    add_library(pacparser::pacparser UNKNOWN IMPORTED GLOBAL)
    set_target_properties(pacparser::pacparser PROPERTIES
      IMPORTED_LOCATION "${PACPARSER_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${PACPARSER_INCLUDE_DIR}")
    # Match the vendored target: a static system libpacparser bundles QuickJS,
    # which needs libm, pthreads and (for dlopen) libdl.
    find_package(Threads)
    set_property(TARGET pacparser::pacparser PROPERTY
      INTERFACE_LINK_LIBRARIES m Threads::Threads ${CMAKE_DL_LIBS})
  endif ()
endif ()

mark_as_advanced(PACPARSER_LIBRARY PACPARSER_INCLUDE_DIR)
