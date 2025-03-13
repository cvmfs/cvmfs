find_path(
    LIBACL_INCLUDE_DIRS
    NAMES acl/libacl.h
    HINTS ${LIBACL_INCLUDE_DIRS}
)

find_library(
    LIBACL_LIBRARY
    NAMES acl
    HINTS ${LIBACL_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
    LibACL
    DEFAULT_MSG
    LIBACL_LIBRARY
    LIBACL_INCLUDE_DIRS
)

if(LIBACL_FOUND)
    mark_as_advanced(LIBACL_LIBRARY LIBACL_INCLUDE_DIRS)
endif()

