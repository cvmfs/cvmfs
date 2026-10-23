# -*- cmake -*-

# - Find leveldb (system installation)
# Used for the system-fallback path (e.g. BUILTIN_EXTERNALS_EXCLUDE=leveldb or
# BUILTIN_EXTERNALS=OFF). Defines the uniform imported target
#
#   leveldb::leveldb
#
# and, for FetchContent's FIND_PACKAGE_ARGS integration, the result variable
# leveldb_FOUND. Also sets, for backwards compatibility:
#   LEVELDB_INCLUDE_DIR   where to find leveldb/db.h
#   LEVELDB_LIBRARIES     the libraries needed to use leveldb
#
# LEVELDB_DIR may be defined as a hint for where to look.

find_path(LEVELDB_INCLUDE_DIR leveldb/db.h
  HINTS ${LEVELDB_DIR} $ENV{LEVELDB_DIR}
  PATH_SUFFIXES include)
find_library(LEVELDB_LIBRARY NAMES leveldb
  HINTS ${LEVELDB_DIR} $ENV{LEVELDB_DIR}
  PATH_SUFFIXES lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(leveldb
  REQUIRED_VARS LEVELDB_LIBRARY LEVELDB_INCLUDE_DIR)

if (leveldb_FOUND)
  set(LEVELDB_LIBRARIES ${LEVELDB_LIBRARY})
  set(LEVELDB_INCLUDE_DIRS ${LEVELDB_INCLUDE_DIR})
  if (NOT TARGET leveldb::leveldb)
    # GLOBAL so the target is usable from sibling directories (e.g. cvmfs/),
    # since this module may be invoked from the externals/ subdirectory.
    add_library(leveldb::leveldb UNKNOWN IMPORTED GLOBAL)
    set_target_properties(leveldb::leveldb PROPERTIES
      IMPORTED_LOCATION "${LEVELDB_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${LEVELDB_INCLUDE_DIR}")
    # leveldb's port layer uses std::thread / pthreads.
    find_package(Threads)
    set_property(TARGET leveldb::leveldb PROPERTY
      INTERFACE_LINK_LIBRARIES Threads::Threads)
  endif ()
endif ()

mark_as_advanced(LEVELDB_LIBRARY LEVELDB_INCLUDE_DIR)
