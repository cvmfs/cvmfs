# - Try to find LibWebsockets
#
# Once done this will define
#
#  LibWebsockets_FOUND - system has LibWebsockets
#  LibWebsockets_INCLUDE_DIRS - the LibWebsockets include directory
#  LibWebsockets_LIBRARIES - Link these to use LibWebsockets
#

find_path(
    LibWebsockets_INCLUDE_DIRS
    NAMES libwebsockets.h
    HINTS ${LibWebsockets_INCLUDE_DIRS}
)

find_library(
    LibWebsockets_LIBRARY
    NAMES websockets
    HINTS ${LibWebsockets_LIBRARY_DIRS}
)

set(LibWebsockets_LIBRARIES ${LibWebsockets_LIBRARY})

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    LibWebsockets
    DEFAULT_MSG
    LibWebsockets_LIBRARIES
    LibWebsockets_INCLUDE_DIRS
)

if(LibWebsockets_FOUND)
    mark_as_advanced(LibWebsockets_LIBRARIES LibWebsockets_INCLUDE_DIRS)
endif()
