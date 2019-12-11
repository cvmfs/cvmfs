# Find the FUSE3 includes and library
#
#  FUSE3_INCLUDE_DIR - where to find fuse.h, etc.
#  FUSE3_LIBRARIES   - List of libraries when using FUSE.
#  FUSE3_FOUND       - True if FUSE3 lib is found.

# check if already in cache, be silent
IF (FUSE3_INCLUDE_DIR)
        SET (FUSE3_FIND_QUIETLY TRUE)
ENDIF (FUSE3_INCLUDE_DIR)

# find includes
FIND_PATH (FUSE3_INCLUDE_DIR fuse.h
        /usr/local/include/fuse3
        /usr/include/fuse3
)

# find lib
SET(FUSE3_NAMES fuse3)
FIND_LIBRARY(FUSE3_LIBRARY
        NAMES ${FUSE3_NAMES}
        PATHS /lib64 /lib /usr/lib64 /usr/lib /usr/local/lib64 /usr/local/lib
)

# check if lib was found and include is present
IF (FUSE3_INCLUDE_DIR AND FUSE3_LIBRARY)
        SET (FUSE3_FOUND TRUE)
        SET (FUSE3_LIBRARIES ${FUSE3_LIBRARY})
ELSE ()
        SET (FUSE3_FOUND FALSE)
        SET (FUSE3_LIBRARIES)
ENDIF ()

# let world know the results
IF (FUSE3_FOUND)
        IF (NOT FUSE3_FIND_QUIETLY)
                MESSAGE(STATUS "Found FUSE3: ${FUSE3_LIBRARY}")
        ENDIF ()

        # Check the cache_readdir field in fuse_file_info (libfuse >= 3.5)
        try_compile(HAS_FUSE_CACHE_READDIR ${CMAKE_BINARY_DIR} ${PROJECT_SOURCE_DIR}/cmake/check_fuse3_cache_readdir.c)
        if(HAS_FUSE_CACHE_READDIR)
                message("Enable Fuse3 cache_readdir support")
                set(CVMFS_ENABLE_FUSE3_CACHE_READDIR ON)
        else(HAS_FUSE_CACHE_READDIR)
                message("Disable Fuse3 cache_readdir support")
        endif(HAS_FUSE_CACHE_READDIR)
ELSE (FUSE3_FOUND)
        IF (FUSE3_FIND_REQUIRED)
                MESSAGE(STATUS "Looked for FUSE3 libraries named ${FUSE3_NAMES}.")
                MESSAGE(FATAL_ERROR "Could NOT find FUSE3 library")
        ENDIF ()
ENDIF (FUSE3_FOUND)

mark_as_advanced (FUSE3_INCLUDE_DIR FUSE3_LIBRARY)
