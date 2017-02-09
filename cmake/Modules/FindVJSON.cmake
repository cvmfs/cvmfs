# - Try to find VJSON
#
# Once done this will define
#
#  VJSON_FOUND - system has VJSON
#  VJSON_INCLUDE_DIRS - the VJSON include directory
#  VJSON_LIBRARIES - Link these to use VJSON
#

find_path(
    VJSON_INCLUDE_DIRS
    NAMES json.h
    HINTS ${VJSON_INCLUDE_DIRS}
)

find_library(
    VJSON_LIBRARIES
    NAMES vjson
    HINTS ${VJSON_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    VJSON
    DEFAULT_MSG
    VJSON_LIBRARIES
    VJSON_INCLUDE_DIRS
)

if(VJSON_FOUND)
    mark_as_advanced(VJSON_LIBRARIES VJSON_INCLUDE_DIRS)
endif()

