# - Try to find TBB
#
# Once done this will define
#
#  TBB_FOUND - system has TBB
#  TBB_INCLUDE_DIRS - the TBB include directory
#  TBB_LIBRARIES - Link these to use TBB
#

find_path(
    TBB_INCLUDE_DIRS
    NAMES tbb/tbb.h
    HINTS ${TBB_INCLUDE_DIRS}
)

find_library(
    TBB_MAIN_LIBRARY
    NAMES tbb${TBB_LIB_SUFFIX}
    HINTS ${TBB_LIBRARY_DIRS}
)

find_library(
    TBB_MALLOC_LIBRARY
    NAMES tbbmalloc${TBB_LIB_SUFFIX}
    HINTS ${TBB_LIBRARY_DIRS}
)

find_library(
    TBB_MALLOC_PROXY_LIBRARY
    NAMES tbbmalloc_proxy${TBB_LIB_SUFFIX}
    HINTS ${TBB_LIBRARY_DIRS}
)

set(TBB_LIBRARIES ${TBB_MALLOC_LIBRARY} ${TBB_MALLOC_PROXY_LIBRARY} ${TBB_MAIN_LIBRARY})

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    TBB
    DEFAULT_MSG
    TBB_LIBRARIES
    TBB_INCLUDE_DIRS
)

if(TBB_FOUND)
    mark_as_advanced(TBB_LIBRARIES TBB_INCLUDE_DIRS)
endif()
