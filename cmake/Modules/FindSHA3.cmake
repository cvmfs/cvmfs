# - Try to find SHA3
#
# Once done this will define
#
#  SHA3_FOUND - system has SHA3
#  SHA3_INCLUDE_DIRS - the SHA3 include directory
#  SHA3_LIBRARIES - Link these to use SHA3
#

find_path(
    SHA3_INCLUDE_DIRS
    NAMES SnP-interface.h
    HINTS ${SHA3_INCLUDE_DIRS}
)

find_library(
    SHA3_LIBRARIES
    NAMES sha3
    HINTS ${SHA3_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    SHA3
    DEFAULT_MSG
    SHA3_LIBRARIES
    SHA3_INCLUDE_DIRS
)

if(SHA3_FOUND)
    mark_as_advanced(SHA3_LIBRARIES SHA3_INCLUDE_DIRS)
endif()

