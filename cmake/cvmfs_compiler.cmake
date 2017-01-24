set (CVMFS_OPT_FLAGS "-Os")
if (CMAKE_COMPILER_IS_GNUCC)
  message (STATUS "checking gcc version...")
  execute_process (
    COMMAND ${CMAKE_C_COMPILER} -v
    OUTPUT_VARIABLE CVMFS_GCC_VERSION
    ERROR_VARIABLE  CVMFS_GCC_VERSION
  )
  STRING(REGEX REPLACE ".*([0-9]+)\\.[0-9]+\\.[0-9]+.*" "\\1" CVMFS_GCC_MAJOR "${CVMFS_GCC_VERSION}")
  STRING(REGEX REPLACE ".*[0-9]+\\.([0-9]+)\\.[0-9]+.*" "\\1" CVMFS_GCC_MINOR "${CVMFS_GCC_VERSION}")
  if (${CVMFS_GCC_MAJOR} LESS 4)
    message (FATAL_ERROR "GCC < 4.1 unsupported")
  endif (${CVMFS_GCC_MAJOR} LESS 4)
  if (${CVMFS_GCC_MAJOR} EQUAL 4)
    if (${CVMFS_GCC_MINOR} LESS 2)
      set (CVMFS_OPT_FLAGS "-O1")
    endif (${CVMFS_GCC_MINOR} LESS 2)
  endif (${CVMFS_GCC_MAJOR} EQUAL 4)
endif (CMAKE_COMPILER_IS_GNUCC)
message (STATUS "using compiler opt flag ${CVMFS_OPT_FLAGS}")
set (CVMFS_BASE_C_FLAGS "${CVMFS_OPT_FLAGS} -g -fno-strict-aliasing -fasynchronous-unwind-tables -fno-omit-frame-pointer -fvisibility=hidden -Wall")
set (CVMFS_BASE_CXX_FLAGS "${CVMFS_BASE_C_FLAGS} -fno-exceptions")
if (NOT USING_CLANG)
  set (CVMFS_BASE_C_FLAGS "${CVMFS_BASE_C_FLAGS} -fno-optimize-sibling-calls")
  set (CVMFS_BASE_CXX_FLAGS "${CVMFS_BASE_CXX_FLAGS} -fno-optimize-sibling-calls")
endif (NOT USING_CLANG)
if (ARM AND NOT IS_64_BIT)
  #
  # Fix TBB on the Raspberry Pi
  #
  set (CVMFS_BASE_CXX_FLAGS "${CVMFS_BASE_CXX_FLAGS} -DTBB_USE_GCC_BUILTINS=1 -D__TBB_64BIT_ATOMICS=0")
endif (ARM AND NOT IS_64_BIT)
set (CVMFS_BASE_DEFINES "-D_REENTRANT -D__EXTENSIONS__ -D_LARGEFILE64_SOURCE -D__LARGE64_FILES")

set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CVMFS_BASE_C_FLAGS} ${CVMFS_BASE_DEFINES}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CVMFS_BASE_CXX_FLAGS} ${CVMFS_BASE_DEFINES}")

if (NOT ARM AND NOT IS_64_BIT)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=i686")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=i686")
endif (NOT ARM AND NOT IS_64_BIT)