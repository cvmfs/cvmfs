# Find the FUSE includes and library
#
#  FUSE_INCLUDE_DIR - where to find fuse.h, etc.
#  FUSE_LIBRARIES   - List of libraries when using FUSE.
#  FUSE_FOUND       - True if FUSE lib is found.

# check if already in cache, be silent
IF (FUSE_INCLUDE_DIR)
        SET (FUSE_FIND_QUIETLY TRUE)
ENDIF (FUSE_INCLUDE_DIR)

# find includes
FIND_PATH (FUSE_INCLUDE_DIR fuse.h
        /usr/local/include
        /usr/include
)

# find lib
SET(FUSE_NAMES fuse)
FIND_LIBRARY(FUSE_LIBRARY
        NAMES ${FUSE_NAMES}
        PATHS /lib64 /lib /usr/lib64 /usr/lib /usr/local/lib64 /usr/local/lib
)

# check if lib was found and include is present
IF (FUSE_INCLUDE_DIR AND FUSE_LIBRARY)
        SET (FUSE_FOUND TRUE)
        SET (FUSE_LIBRARIES ${FUSE_LIBRARY})
ELSE (FUSE_INCLUDE_DIR AND FUSE_LIBRARY)
        SET (FUSE_FOUND FALSE)
        SET (FUSE_LIBRARIES)
ENDIF (FUSE_INCLUDE_DIR AND FUSE_LIBRARY)

# let world know the results
IF (FUSE_FOUND)
        IF (NOT FUSE_FIND_QUIETLY)
                MESSAGE(STATUS "Found FUSE: ${FUSE_LIBRARY}")
        ENDIF (NOT FUSE_FIND_QUIETLY)
ELSE (FUSE_FOUND)
        IF (FUSE_FIND_REQUIRED)
                MESSAGE(STATUS "Looked for FUSE libraries named ${FUSE_NAMES}.")
                MESSAGE(FATAL_ERROR "Could NOT find FUSE library")
        ENDIF (FUSE_FIND_REQUIRED)
ENDIF (FUSE_FOUND)

mark_as_advanced (FUSE_INCLUDE_DIR FUSE_LIBRARY)
