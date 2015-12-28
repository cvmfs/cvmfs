# - Try to find VOMS
# Once done this will define
#
#  VOMS_FOUND - system has VOMS
#  VOMS_INCLUDE_DIRS - the VOMS include directory
#  VOMS_LIBRARIES - Link these to use VOMS
#  VOMS_DEFINITIONS - Compiler switches required for using VOMS
#
#  Copyright (c) 2008 Andreas Schneider <mail@cynapses.org>
#
#  Redistribution and use is allowed according to the terms of the New
#  BSD license.
#  For details see the accompanying COPYING-CMAKE-SCRIPTS file.
#

if (VOMS_LIBRARY AND VOMS_INCLUDE_DIR)
  # in cache already
  set(VOMS_FOUND TRUE)
else (VOMS_LIBRARY AND VOMS_INCLUDE_DIR)
  # use pkg-config to get the directories and then use these values
  # in the FIND_PATH() and FIND_LIBRARY() calls
  if (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 4)
    include(UsePkgConfig)
    pkgconfig(voms-2.0 _VOMS_INCLUDEDIR _VOMS_LIBDIR _VOMS_LDFLAGS _VOMS_CFLAGS)
  else (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 4)
    find_package(PkgConfig)
    if (PKG_CONFIG_FOUND)
      pkg_check_modules(_VOMS voms-2.0)
    endif (PKG_CONFIG_FOUND)
  endif (${CMAKE_MAJOR_VERSION} EQUAL 2 AND ${CMAKE_MINOR_VERSION} EQUAL 4)
  find_path(VOMS_INCLUDE_DIR
    NAMES
      voms/voms_apic.h
    PATHS
      ${_VOMS_INCLUDEDIR}
      /usr/include
      /usr/local/include
      /opt/local/include
      /sw/include
  )

  find_library(VOMS_LIBRARY
    NAMES
      vomsapi
    PATHS
      ${_VOMS_LIBDIR}
      /usr/lib
      /usr/local/lib
      /opt/local/lib
      /sw/lib
  )

  if (VOMS_LIBRARY)
    set(VOMS_FOUND TRUE)
  endif (VOMS_LIBRARY)

  set(VOMS_INCLUDE_DIRS
    ${VOMS_INCLUDE_DIR}
  )

  if (VOMS_FOUND)
    set(VOMS_LIBRARIES
      ${VOMS_LIBRARIES}
      ${VOMS_LIBRARY}
    )
  endif (VOMS_FOUND)

  if (VOMS_INCLUDE_DIRS AND VOMS_LIBRARIES)
     set(VOMS_FOUND TRUE)
  endif (VOMS_INCLUDE_DIRS AND VOMS_LIBRARIES)

  if (VOMS_FOUND)
    if (NOT VOMS_FIND_QUIETLY)
      message(STATUS "Found VOMS: ${VOMS_LIBRARIES}")
    endif (NOT VOMS_FIND_QUIETLY)
  else (VOMS_FOUND)
    if (VOMS_FIND_REQUIRED)
      message(FATAL_ERROR "Could not find VOMS")
    endif (VOMS_FIND_REQUIRED)
  endif (VOMS_FOUND)

  # show the VOMS_INCLUDE_DIRS and VOMS_LIBRARIES variables only in the advanced view
  mark_as_advanced(VOMS_INCLUDE_DIRS VOMS_LIBRARIES VOMS_INCLUDE_DIR VOMS_LIBRARY)

endif (VOMS_LIBRARY AND VOMS_INCLUDE_DIR)
