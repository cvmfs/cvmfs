# Find the FUSE2 includes and library
#
#  FUSE2_INCLUDE_DIR - where to find fuse.h, etc.
#  FUSE2_LIBRARIES   - List of libraries when using FUSE.
#  FUSE2_FOUND       - True if FUSE lib is found.

# check if already in cache, be silent
IF (FUSE2_INCLUDE_DIR)
        SET (FUSE2_FIND_QUIETLY TRUE)
ENDIF (FUSE2_INCLUDE_DIR)

# find includes
FIND_PATH (FUSE2_INCLUDE_DIR fuse.h
        /usr/local/include
        /usr/include
)

# find lib
SET(FUSE2_NAMES fuse)
FIND_LIBRARY(FUSE2_LIBRARY
        NAMES ${FUSE2_NAMES}
        PATHS /lib64 /lib /usr/lib64 /usr/lib /usr/local/lib64 /usr/local/lib
)

# check if lib was found and include is present
IF (FUSE2_INCLUDE_DIR AND FUSE2_LIBRARY)
        SET (FUSE2_FOUND TRUE)
        SET (FUSE2_LIBRARIES ${FUSE2_LIBRARY})
ELSE (FUSE2_INCLUDE_DIR AND FUSE2_LIBRARY)
        SET (FUSE2_FOUND FALSE)
        SET (FUSE2_LIBRARIES)
ENDIF (FUSE2_INCLUDE_DIR AND FUSE2_LIBRARY)

# let world know the results
IF (FUSE2_FOUND)
        IF (NOT FUSE2_FIND_QUIETLY)
                MESSAGE(STATUS "Found FUSE version 2: ${FUSE2_LIBRARY}")
        ENDIF ()
ELSE (FUSE2_FOUND)
        IF (FUSE2_FIND_REQUIRED)
                MESSAGE(STATUS "Looked for FUSE libraries named ${FUSE2_NAMES}.")
                MESSAGE(FATAL_ERROR "Could NOT find FUSE version 2 library")
        ENDIF ()
ENDIF (FUSE2_FOUND)

mark_as_advanced (FUSE2_INCLUDE_DIR FUSE2_LIBRARY)
