# Find the FUSE3 includes and library
#
#  FUSE3_INCLUDE_DIR - where to find fuse.h, etc.
#  FUSE3_LIBRARIES   - List of libraries when using FUSE.
#  FUSE3_FOUND       - True if FUSE3 lib is found.

# check if already in cache, be silent
IF (FUSE3_INCLUDE_DIR)
        SET (FUSE3_FIND_QUIETLY TRUE)
ENDIF (FUSE3_INCLUDE_DIR)

# find fuse3 only if they are not manually defined
# format:
# FUSE3_INCLUDE_DIR=/usr/local/include   ## do NOT add /fuse3 in the path
# FUSE3_LIBRARY=/usr/local/lib64/libfuse3.so
IF (NOT DEFINED FUSE3_INCLUDE_DIR AND NOT DEFINED FUSE3_LIBRARY)
# find includes
FIND_PATH (FUSE3_INCLUDE_DIR fuse3/fuse.h
           /usr/local/include
           /usr/include
)

# find lib
SET(FUSE3_NAMES fuse3)
FIND_LIBRARY(FUSE3_LIBRARY
        NAMES ${FUSE3_NAMES}
        PATHS /lib64 /lib /usr/lib64 /usr/lib /usr/local/lib64 /usr/local/lib
)
ENDIF()

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

        # Check if we can use fuse_loop_config in fuse_session_loop_mt (libfuse >= 3.12)
        get_filename_component(FUSE3_LIB_PATH ${FUSE3_LIBRARY} DIRECTORY)
        # Replace get_filename_component with cmake_path when CentOS7 is finally deprecated
        # cmake_path(GET FUSE3_LIBRARY PARENT_PATH FUSE3_LIB_PATH)
        try_compile(HAS_FUSE_LOOP_CONFIG "${CMAKE_BINARY_DIR}/temp"
                    "${PROJECT_SOURCE_DIR}/cmake/check_fuse3_loop_config.c"
                    LINK_LIBRARIES fuse3
                    CMAKE_FLAGS "-DINCLUDE_DIRECTORIES=${FUSE3_INCLUDE_DIR}"
                                "-DLINK_DIRECTORIES=${FUSE3_LIB_PATH}")

        if (HAS_FUSE_LOOP_CONFIG)
                message("Enable Fuse3 loop_config support")
                set(CVMFS_ENABLE_FUSE3_LOOP_CONFIG ON)
        else (HAS_FUSE_LOOP_CONFIG)
                message("Disable Fuse3 loop_config support")
        endif (HAS_FUSE_LOOP_CONFIG)
ELSE (FUSE3_FOUND)
        IF (FUSE3_FIND_REQUIRED)
                MESSAGE(STATUS "Looked for FUSE3 libraries named ${FUSE3_NAMES}.")
                MESSAGE(FATAL_ERROR "Could NOT find FUSE3 library")
        ENDIF ()
ENDIF (FUSE3_FOUND)

mark_as_advanced (FUSE3_INCLUDE_DIR FUSE3_LIBRARY)
