# Find the FUSE-T includes and library

# MACOS_FUSE_INCLUDE_DIR - where to find fuse.h, etc.
# MACOS_FUSE_LIBRARIES   - List of libraries when using FUSE-T
# MACOS_FUSE_FOUND       - True if either FUSE-T is found.

# set fuse headers include directory
# hardcoding this due to misplacement of fuse.h on installing FUSE-T
# in comparison to macFUSE 
# (the latter places fuse.h in /usr/local/include but the former in /usr/local/include/fuse)
set (MACOS_FUSE_INCLUDE_DIR /usr/local/include)
if (USE_MACFUSE_KEXT) 
        # find lib
        set (OSXFUSE_NAMES osxfuse.2 osxfuse)
        find_library(MACOS_FUSE_LIBRARY
                NAMES ${OSXFUSE_NAMES}
                PATHS /usr/lib /usr/local/lib
                        NO_DEFAULT_PATH
        )
else()
        # find FUSE-T lib as the primary lib
        set (MACOS_FUSE_LIB_NAMES fuse-t)
        find_library(MACOS_FUSE_LIBRARY 
                NAMES ${MACOS_FUSE_LIB_NAMES} 
                PATHS /usr/lib /usr/local/lib
        )
endif()


# check if lib was found and include is present
if (MACOS_FUSE_INCLUDE_DIR AND MACOS_FUSE_LIBRARY)
        set (MACOS_FUSE_FOUND TRUE)
        set (MACOS_FUSE_LIBRARIES ${MACOS_FUSE_LIBRARY})
else()
        set (MACOS_FUSE_FOUND FALSE)
        unset (MACOS_FUSE_LIBRARIES)
endif()

# let world know the results
if (MACOS_FUSE_FOUND)
        if (NOT MACOS_FUSE_FIND_QUIETLY)
                message(STATUS "Found FUSE library for macOS: ${MACOS_FUSE_LIBRARY}. fuse.h include directory: ${MACOS_FUSE_INCLUDE_DIR}")
        endif()
else ()
        if (NOT MACOS_FUSE_FIND_QUIETLY)
                message(STATUS "Looked for FUSE library named ${MACOS_FUSE_LIB_NAMES}.")
                message(FATAL_ERROR "Could NOT find FUSE library")
        endif()
endif()

mark_as_advanced (MACOS_FUSE_INCLUDE_DIR MACOS_FUSE_LIBRARIES)
