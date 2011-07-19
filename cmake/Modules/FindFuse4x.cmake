# Find the Fuse4x includes and library
#
#  FUSE4X_INCLUDE_DIR - where to find fuse.h, etc.
#  FUSE4X_LIBRARIES   - List of libraries when using Fuse4x.
#  FUSE4X_FOUND       - True if Fuse4x lib is found.

# check if already in cache, be silent
IF (FUSE4X_INCLUDE_DIR)
        SET (FUSE4X_FIND_QUIETLY TRUE)
ENDIF (FUSE4X_INCLUDE_DIR)

# find includes
FIND_PATH (FUSE4X_INCLUDE_DIR fuse.h
        /usr/local/include
        /usr/include
)

# find lib
SET(FUSE4X_NAMES fuse4x)
FIND_LIBRARY(FUSE4X_LIBRARY
        NAMES ${FUSE4X_NAMES}
        PATHS /usr/lib /usr/local/lib
		NO_DEFAULT_PATH
)

# check if lib was found and include is present
IF (FUSE4X_INCLUDE_DIR AND FUSE4X_LIBRARY)
        SET (FUSE4X_FOUND TRUE)
        SET (FUSE4X_LIBRARIES ${FUSE4X_LIBRARY})
ELSE (FUSE4X_INCLUDE_DIR AND FUSE4X_LIBRARY)
        SET (FUSE4X_FOUND FALSE)
        SET (FUSE4X_LIBRARIES)
ENDIF (FUSE4X_INCLUDE_DIR AND FUSE4X_LIBRARY)

# let world know the results
IF (FUSE4X_FOUND)
        IF (NOT FUSE4X_FIND_QUIETLY)
                MESSAGE(STATUS "Found Fuse4x: ${FUSE4X_LIBRARY}")
        ENDIF (NOT FUSE4X_FIND_QUIETLY)
ELSE (FUSE4X_FOUND)
        IF (FUSE4X_FIND_REQUIRED)
                MESSAGE(STATUS "Looked for Fuse4x libraries named ${FUSE4X_NAMES}.")
                MESSAGE(FATAL_ERROR "Could NOT find Fuse4x library")
        ENDIF (FUSE4X_FIND_REQUIRED)
ENDIF (FUSE4X_FOUND)

mark_as_advanced (FUSE4X_INCLUDE_DIR FUSE4X_LIBRARY)