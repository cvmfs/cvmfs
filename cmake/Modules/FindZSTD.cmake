# - Try to find builtin zstd
#
# Once done this will define
#
#  ZSTD_FOUND - system has ZSTD
#  ZSTD_INCLUDE_DIRS - the ZSTD include directory
#  ZSTD_LIBRARIES - Link these to use ZSTD
#

find_path(
    ZSTD_INCLUDE_DIRS
    NAMES zstd.h zstd_errors.h
    PATHS ${EXTERNALS_INSTALL_LOCATION}/include
    NO_DEFAULT_PATH
)

find_library(
    ZSTD_LIBRARIES
    NAMES zstd
    PATHS ${EXTERNALS_INSTALL_LOCATION}/lib
    NO_DEFAULT_PATH
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    ZSTD
    DEFAULT_MSG
    ZSTD_LIBRARIES
    ZSTD_INCLUDE_DIRS
)

if(ZSTD_FOUND)
    mark_as_advanced(ZSTD_LIBRARIES ZSTD_INCLUDE_DIRS)
endif()

