# - Try to find builtin libnettle low-level crypto library
#
# Once done this will define
#
#  Libnettle_FOUND - system has nettle
#  Libnettle_INCLUDE_DIRS - the nettle include directory
#  Libnettle_LIBRARIES - Link these to use nettle
#

find_path(
  Libnettle_INCLUDE_DIRS
  NAMES nettle/version.h
  PATHS ${EXTERNALS_INSTALL_LOCATION}/crypto/include
)

find_library(
  Libnettle_LIBRARIES
  NAMES nettle
  PATHS ${EXTERNALS_INSTALL_LOCATION}/crypto/lib
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
  Libnettle
  DEFAULT_MSG
  Libnettle_LIBRARIES
  Libnettle_INCLUDE_DIRS
)

if (Libnettle_FOUND)
  mark_as_advanced(Libnettle_LIBRARIES Libnettle_INCLUDE_DIRS)
endif()
