# Find the osxfuse includes and library
#
#  OSXFUSE_INCLUDE_DIR - where to find fuse.h, etc.
#  OSXFUSE_LIBRARIES   - List of libraries when using osxfuse.
#  OSXFUSE_FOUND       - True if osxfuse lib is found.

# check if already in cache, be silent
IF (OSXFUSE_INCLUDE_DIR)
        SET (OSXFUSE_FIND_QUIETLY TRUE)
ENDIF (OSXFUSE_INCLUDE_DIR)

# find includes
FIND_PATH (OSXFUSE_INCLUDE_DIR fuse.h
        /usr/local/include
        /usr/include
        /usr/local/include/osxfuse
)

# find lib
SET(OSXFUSE_NAMES osxfuse)
FIND_LIBRARY(OSXFUSE_LIBRARY
        NAMES ${OSXFUSE_NAMES}
        PATHS /usr/lib /usr/local/lib osxfuse
		NO_DEFAULT_PATH
)

# check if lib was found and include is present
IF (OSXFUSE_INCLUDE_DIR AND OSXFUSE_LIBRARY)
        SET (OSXFUSE_FOUND TRUE)
        SET (OSXFUSE_LIBRARIES ${OSXFUSE_LIBRARY})
ELSE (OSXFUSE_INCLUDE_DIR AND OSXFUSE_LIBRARY)
        SET (OSXFUSE_FOUND FALSE)
        SET (OSXFUSE_LIBRARIES)
ENDIF (OSXFUSE_INCLUDE_DIR AND OSXFUSE_LIBRARY)

# let world know the results
IF (OSXFUSE_FOUND)
        IF (NOT OSXFUSE_FIND_QUIETLY)
                MESSAGE(STATUS "Found osxfuse: ${OSXFUSE_LIBRARY}")
        ENDIF (NOT OSXFUSE_FIND_QUIETLY)
ELSE (OSXFUSE_FOUND)
        IF (OSXFUSE_FIND_REQUIRED)
                MESSAGE(STATUS "Looked for osxfuse libraries named ${OSXFUSE_NAMES}.")
                MESSAGE(FATAL_ERROR "Could NOT find osxfuse library")
        ENDIF (OSXFUSE_FIND_REQUIRED)
ENDIF (OSXFUSE_FOUND)

mark_as_advanced (OSXFUSE_INCLUDE_DIR OSXFUSE_LIBRARY)