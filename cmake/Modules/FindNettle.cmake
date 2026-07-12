# -*- cmake -*-

# - Find nettle (system installation), version-aware. cvmfs requires >= 3.10:
# cvmfs/crypto uses SHAKE128 (sha3_128_shake), added in 3.10. Defines:
#   Nettle::Nettle       imported target
#   Nettle_FOUND         consumed by FetchContent's FIND_PACKAGE_ARGS
#   Nettle_VERSION       checked against the requested minimum
#   NETTLE_INCLUDE_DIR   where to find <nettle/sha3.h>
#   NETTLE_LIBRARY       the nettle library

find_package(PkgConfig QUIET)
if (PkgConfig_FOUND)
  pkg_check_modules(PC_NETTLE QUIET nettle)
endif ()

find_path(NETTLE_INCLUDE_DIR
  NAMES nettle/sha3.h
  HINTS ${PC_NETTLE_INCLUDEDIR} ${PC_NETTLE_INCLUDE_DIRS})
find_library(NETTLE_LIBRARY
  NAMES nettle
  HINTS ${PC_NETTLE_LIBDIR} ${PC_NETTLE_LIBRARY_DIRS})

# Prefer <nettle/version.h> (since nettle 3.1), fall back to pkg-config. If
# neither yields a version the check below fails -> vendored build.
set(Nettle_VERSION "")
if (NETTLE_INCLUDE_DIR AND EXISTS "${NETTLE_INCLUDE_DIR}/nettle/version.h")
  file(STRINGS "${NETTLE_INCLUDE_DIR}/nettle/version.h" _nettle_ver_major
       REGEX "^#define[ \t]+NETTLE_VERSION_MAJOR[ \t]+[0-9]+")
  file(STRINGS "${NETTLE_INCLUDE_DIR}/nettle/version.h" _nettle_ver_minor
       REGEX "^#define[ \t]+NETTLE_VERSION_MINOR[ \t]+[0-9]+")
  string(REGEX REPLACE ".*[ \t]([0-9]+).*" "\\1" _nettle_ver_major "${_nettle_ver_major}")
  string(REGEX REPLACE ".*[ \t]([0-9]+).*" "\\1" _nettle_ver_minor "${_nettle_ver_minor}")
  if (_nettle_ver_major MATCHES "^[0-9]+$" AND _nettle_ver_minor MATCHES "^[0-9]+$")
    set(Nettle_VERSION "${_nettle_ver_major}.${_nettle_ver_minor}")
  endif ()
endif ()
if (NOT Nettle_VERSION AND PC_NETTLE_VERSION)
  set(Nettle_VERSION "${PC_NETTLE_VERSION}")
endif ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Nettle
  REQUIRED_VARS NETTLE_LIBRARY NETTLE_INCLUDE_DIR
  VERSION_VAR Nettle_VERSION)

if (Nettle_FOUND)
  # This module runs from the externals/ subdirectory, so cache the include dir
  # and make the target GLOBAL to reach sibling directories such as cvmfs/.
  set(NETTLE_INCLUDE_DIR "${NETTLE_INCLUDE_DIR}" CACHE INTERNAL "nettle include dir")
  if (NOT TARGET Nettle::Nettle)
    add_library(Nettle::Nettle UNKNOWN IMPORTED GLOBAL)
    set_target_properties(Nettle::Nettle PROPERTIES
      IMPORTED_LOCATION "${NETTLE_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${NETTLE_INCLUDE_DIR}")
  endif ()
endif ()

mark_as_advanced(NETTLE_LIBRARY NETTLE_INCLUDE_DIR)
