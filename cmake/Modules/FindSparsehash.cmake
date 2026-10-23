# - Find Sparsehash (system installation)
# Used for the system-fallback path (e.g. BUILTIN_EXTERNALS_EXCLUDE=sparsehash
# or BUILTIN_EXTERNALS=OFF). sparsehash is header-only; this defines the uniform
# imported target
#
#   sparsehash::sparsehash
#
# and, for FetchContent's FIND_PACKAGE_ARGS integration, the result variable
# Sparsehash_FOUND. Also sets, for backwards compatibility:
#   SPARSEHASH_INCLUDE_DIR   where to find the google/ headers

if(SPARSEHASH_INCLUDE_DIR)
    set(Sparsehash_FIND_QUIETLY TRUE)
endif()

find_path(SPARSEHASH_INCLUDE_DIR google/sparsehash/sparsehashtable.h)

# handle the QUIETLY and REQUIRED arguments and set Sparsehash_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Sparsehash DEFAULT_MSG SPARSEHASH_INCLUDE_DIR)

if(Sparsehash_FOUND AND NOT TARGET sparsehash::sparsehash)
  # GLOBAL so the target is usable from sibling directories (e.g. cvmfs/),
  # since this module may be invoked from the externals/ subdirectory.
  add_library(sparsehash::sparsehash INTERFACE IMPORTED GLOBAL)
  set_target_properties(sparsehash::sparsehash PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${SPARSEHASH_INCLUDE_DIR}")
endif()

mark_as_advanced(SPARSEHASH_INCLUDE_DIR)
