# -*- cmake -*-

# - Find sqlite3 (system installation)
# Used for the system-fallback path (e.g. BUILTIN_EXTERNALS_EXCLUDE=sqlite3 -- the
# default on macOS -- or BUILTIN_EXTERNALS=OFF). Defines the uniform imported
# target
#
#   SQLite::SQLite3
#
# and, for FetchContent's FIND_PACKAGE_ARGS integration, the result variable
# sqlite3_FOUND. Also sets, for the tree's global include_directories() list:
#   SQLITE3_INCLUDE_DIR   where to find sqlite3.h
#   SQLITE3_LIBRARY       the sqlite3 library

find_path(SQLITE3_INCLUDE_DIR sqlite3.h)
find_library(SQLITE3_LIBRARY NAMES sqlite3)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(sqlite3
  REQUIRED_VARS SQLITE3_LIBRARY SQLITE3_INCLUDE_DIR)

if (sqlite3_FOUND)
  # CACHE INTERNAL so the include dir is visible in the parent scope: this module
  # is invoked (via FetchContent) from the externals/ subdirectory.
  set(SQLITE3_INCLUDE_DIR "${SQLITE3_INCLUDE_DIR}" CACHE INTERNAL "sqlite3 include dir")
  if (NOT TARGET SQLite::SQLite3)
    # GLOBAL so the target is usable from sibling directories (e.g. cvmfs/),
    # since this module may be invoked from the externals/ subdirectory.
    add_library(SQLite::SQLite3 UNKNOWN IMPORTED GLOBAL)
    set_target_properties(SQLite::SQLite3 PROPERTIES
      IMPORTED_LOCATION "${SQLITE3_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${SQLITE3_INCLUDE_DIR}")
  endif ()
endif ()

mark_as_advanced(SQLITE3_LIBRARY SQLITE3_INCLUDE_DIR)
