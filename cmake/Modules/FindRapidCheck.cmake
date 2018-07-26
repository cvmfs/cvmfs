# - Try to find RapidCheck
#
# Once done this will define
#
#  RAPIDCHECK_FOUND - system has RapidCheck
#  RAPIDCHECK_INCLUDE_DIRS - the RapidCheck include directory
#  RAPIDCHECK_LIBRARIES - Link these to use RapidCheck
#

find_path(
    RAPIDCHECK_INCLUDE_DIRS
    NAMES rapidcheck.h
    HINTS ${RAPIDCHECK_INCLUDE_DIRS}
)

find_library(
    RAPIDCHECK_LIBRARY
    NAMES rapidcheck
    HINTS ${RAPIDCHECK_LIBRARY_DIRS}
)

set(RAPIDCHECK_LIBRARIES ${RAPIDCHECK_LIBRARY})

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    RapidCheck
    DEFAULT_MSG
    RAPIDCHECK_LIBRARIES
    RAPIDCHECK_INCLUDE_DIRS
)

if(RAPIDCHECK_FOUND)
    mark_as_advanced(RAPIDCHECK_LIBRARIES RAPIDCHECK_INCLUDE_DIRS)
endif()
