# - Try to find SHA2
#
# Once done this will define
#
#  SHA2_FOUND - system has SHA2
#  SHA2_INCLUDE_DIRS - the SHA2 include directory
#  SHA2_LIBRARIES - Link these to use SHA2
#

find_path(
    SHA2_INCLUDE_DIRS
    NAMES sha2.h
    HINTS ${SHA2_INCLUDE_DIRS}
)

find_library(
    SHA2_LIBRARIES
    NAMES sha2
    HINTS ${SHA2_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    SHA2
    DEFAULT_MSG
    SHA2_LIBRARIES
    SHA2_INCLUDE_DIRS
)

if(SHA2_FOUND)
    mark_as_advanced(SHA2_LIBRARIES SHA2_INCLUDE_DIRS)
endif()

