#
# Workaround for Debian packaging debhelper trying to pass -D_FORTIFY_SOURCE=2
# through CPPFLAGS that is not officially supported by CMake. Hence, debhelper
# appends CPPFLAGS to CFLAGS which breaks the build of the c-ares external.
# This filters out flags that scares c-ares's ./configure script.
#
set (TMP_C_FLAGS "${CFLAGS} ${CMAKE_C_FLAGS}")
set (CFLAGS        "") # this should better be `unset()` but CMake 2.6.2 doesn't
set (ENV{CFLAGS}   "") # support it. Should be changed as soon as SLES 11 brings
set (CMAKE_C_FLAGS "") # an update for CMake or we drop support for the platform
set (LDFLAGS $ENV{LDFLAGS})
separate_arguments (TMP_C_FLAGS)
foreach (CMPLR_FLAG ${TMP_C_FLAGS})
  if (${CMPLR_FLAG} MATCHES ^-[DIU].*)
    message ("Moving ${CMPLR_FLAG} from CFLAGS into CPPFLAGS")
    set (CPPFLAGS "${CPPFLAGS} ${CMPLR_FLAG}")
  elseif (${CMPLR_FLAG} MATCHES ^-[Ll].*)
    message ("Moving ${CMPLR_FLAG} from CFLAGS into LDFLAGS")
    set (LDFLAGS "${LDFLAGS} ${CMPLR_FLAG}")
    set (CMAKE_LD_FLAGS "${CMAKE_LD_FLAGS} ${CMPLR_FLAG}")
  else (${CMPLR_FLAG} MATCHES ^-[DUILl].*)
    set (CFLAGS "${CFLAGS} ${CMPLR_FLAG}")
  endif (${CMPLR_FLAG} MATCHES ^-[DIU].*)
endforeach (CMPLR_FLAG)
set (ENV{CPPFLAGS}   "${CPPFLAGS}")
set (CMAKE_C_FLAGS   "${CFLAGS}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CPPFLAGS}")
set (ENV{CFLAGS}     "${CFLAGS}")
set (ENV{LDFLAGS}    "${LDFLAGS}")

#
# set some default flags
#
# flags in CMAKE_C**_FLAGS are always passed to the compiler
#
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
if (APPLE)
  if (${CMAKE_SYSTEM_VERSION} GREATER 14.5.0)
    set(CVMFS_BASE_C_FLAGS "${CVMFS_BASE_C_FLAGS} -mmacosx-version-min=10.11")
  endif(${CMAKE_SYSTEM_VERSION} GREATER 14.5.0)
endif(APPLE)
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

if (BUILD_COVERAGE AND NOT USING_CLANG)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fprofile-arcs -ftest-coverage")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fprofile-arcs -ftest-coverage -fPIC")
endif (BUILD_COVERAGE AND NOT USING_CLANG)

# Check for a potentially buggy Xcode version
if(NOT CMAKE_CXX_COMPILER_VERSION) # work around for cmake versions smaller than 2.8.10
  execute_process(COMMAND ${CMAKE_CXX_COMPILER} -dumpversion OUTPUT_VARIABLE CMAKE_CXX_COMPILER_VERSION)
endif()
if (APPLE AND ${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang" AND ${CMAKE_CXX_COMPILER_VERSION} VERSION_GREATER 9.1.0.0)
  message("Xcode version 9.3 or newer detected")
  set (CVMFS_BUGGY_XCODE ON)
endif()

# Check for old Linux version that don't have a complete inotify implementation
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
  try_compile(HAS_INOTIFY_INIT1 ${CMAKE_BINARY_DIR} ${PROJECT_SOURCE_DIR}/cmake/check_inotify_init1.c)
  if(HAS_INOTIFY_INIT1)
    message("Enable inotify support")
    set(CVMFS_ENABLE_INOTIFY ON)
  endif(HAS_INOTIFY_INIT1)
endif(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")