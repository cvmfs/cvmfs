# Find the FUSE-T includes and library

# MACOS_FUSE_INCLUDE_DIR - where to find fuse.h, etc.
# MACOS_FUSE_LIBRARIES   - List of libraries when using FUSE-T
# MACOS_FUSE_FOUND       - True if either FUSE-T is found.

# find includes
# NO_DEFAULT_PATH to avoid collisions with /Library/Frameworks/fuse_t.framework/Headers
FIND_PATH (MACOS_FUSE_INCLUDE_DIR fuse.h
        /usr/include
        /usr/local/include
        /usr/local/include/fuse NO_DEFAULT_PATH
)

# find FUSE-T lib as the primary lib
SET(MACOS_FUSE_LIB_NAMES fuse-t)
FIND_LIBRARY(MACOS_FUSE_LIBRARY 
        NAMES ${MACOS_FUSE_LIB_NAMES} 
        PATHS /usr/lib /usr/local/lib NO_DEFAULT_PATH
)

# check if lib was found and include is present
IF (MACOS_FUSE_INCLUDE_DIR AND MACOS_FUSE_LIBRARY)
        SET (MACOS_FUSE_FOUND TRUE)
        SET (MACOS_FUSE_LIBRARIES ${MACOS_FUSE_LIBRARY})
ELSE (MACOS_FUSE_INCLUDE_DIR AND MACOS_FUSE_LIBRARY)
        SET (MACOS_FUSE_FOUND FALSE)
        SET (MACOS_FUSE_LIBRARIES)
ENDIF (MACOS_FUSE_INCLUDE_DIR AND MACOS_FUSE_LIBRARY)

# let world know the results
IF (MACOS_FUSE_FOUND)
        IF (NOT MACOS_FUSE_FIND_QUIETLY)
                MESSAGE(STATUS "Found FUSE-T library for macOS: ${MACOS_FUSE_LIBRARY}. fuse.h include directory: ${MACOS_FUSE_INCLUDE_DIR}")
        ENDIF (NOT MACOS_FUSE_FIND_QUIETLY)
ELSE (MACOS_FUSE_FOUND)
        IF (MACOS_FUSE_FIND_QUIETLY)
                MESSAGE(STATUS "Looked for FUSE-T library named ${MACOS_FUSE_LIB_NAMES}.")
                MESSAGE(FATAL_ERROR "Could NOT find FUSE-T library")
        ENDIF (MACOS_FUSE_FIND_QUIETLY)
ENDIF (MACOS_FUSE_FOUND)

mark_as_advanced (MACOS_FUSE_INCLUDE_DIR MACOS_FUSE_LIBRARIES)