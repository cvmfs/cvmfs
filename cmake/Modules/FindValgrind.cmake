# - Try to find VALGRIND
# Once done this will define
#
#  VALGRIND_FOUND - system has VALGRIND
#  VALGRIND_INCLUDE_DIRS - the VALGRIND include directory
#  VALGRIND_DEFINITIONS - Compiler switches required for using VALGRIND
#
#  Copyright (c) 2008 Andreas Schneider <mail@cynapses.org>
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the accompanying COPYING-CMAKE-SCRIPTS file.
#

if (VALGRIND_INCLUDE_DIR)
  # in cache already
  set(VALGRIND_FOUND TRUE)
else (VALGRIND_INCLUDE_DIR)
  # use pkg-config to get the directories and then use these values
  # in the FIND_PATH() call
  if (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 4)
    include(UsePkgConfig)
    pkgconfig(valgrind _VALGRIND_INCLUDEDIR _VALGRIND_LIBDIR _VALGRIND_LDFLAGS _VALGRIND_CFLAGS)
  else (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 4)
    find_package(PkgConfig)
    if (PKG_CONFIG_FOUND)
      pkg_check_modules(_VALGRIND valgrind)
    endif (PKG_CONFIG_FOUND)
  endif (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 4)
  find_path(VALGRIND_INCLUDE_DIR
    NAMES
      valgrind/valgrind.h
    PATHS
      ${_VALGRIND_INCLUDEDIR}
      /usr/include
      /usr/local/include
      /opt/local/include
      /sw/include
  )

  if (VALGRIND_INCLUDE_DIR)
    set(VALGRIND_FOUND TRUE)
  endif (VALGRIND_INCLUDE_DIR)

  set(VALGRIND_INCLUDE_DIRS
    ${VALGRIND_INCLUDE_DIR}
  )

  if (VALGRIND_FOUND)
    if (NOT VALGRIND_FIND_QUIETLY)
      message(STATUS "Found VALGRIND: ${VALGRIND_INCLUDE_DIR}")
    endif (NOT VALGRIND_FIND_QUIETLY)
    # show the VALGRIND_INCLUDE_DIRS variables only in the advanced view
    mark_as_advanced(VALGRIND_INCLUDE_DIRS VALGRIND_INCLUDE_DIR)
  else (VALGRIND_FOUND)
    if (VALGRIND_FIND_REQUIRED)
      message(FATAL_ERROR "Could not find VALGRIND")
    endif (VALGRIND_FIND_REQUIRED)
  endif (VALGRIND_FOUND)

endif (VALGRIND_INCLUDE_DIR)
