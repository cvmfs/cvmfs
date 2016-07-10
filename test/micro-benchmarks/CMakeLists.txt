
#
# global micro benchmark configuration
#
set (PROJECT_UBENCHMARKS_NAME "cvmfs_ubenchmarks")

#
# micro benchmarks files
#
set(CVMFS_UBENCHMARKS_FILES
  main.cc

  b_smallhash.cc
)

#
# unit test source files
#
set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_UBENCHMARKS_SOURCES

  ${CVMFS_UBENCHMARKS_FILES}

  # dependencies
  ${CVMFS_SOURCE_DIR}/atomic.h
  ${CVMFS_SOURCE_DIR}/murmur.h
  ${CVMFS_SOURCE_DIR}/prng.h
  ${CVMFS_SOURCE_DIR}/smallhash.h
  ${CVMFS_SOURCE_DIR}/smalloc.h
)


#
# Compiler and Linker Flags for micro benchmarks
#
set (CVMFS_UBENCHMARKS_CFLAGS "${CVMFS_UBENCHMARKS_CFLAGS} -DNDEBUG")
set (CVMFS_UBENCHMARKS_LDFLAGS "${CVMFS_UBENCHMARKS_LDFLAGS} -DNDEBUG")

#
# build CernVM-FS micro benchmarks
#
include_directories (${INCLUDE_DIRECTORIES})

if (BUILD_UBENCHMARKS)
  add_executable (${PROJECT_UBENCHMARKS_NAME} ${CVMFS_UBENCHMARKS_SOURCES})
else (BUILD_UBENCHMARKS)
  add_executable (${PROJECT_UBENCHMARKS_NAME} EXCLUDE_FROM_ALL ${CVMFS_UBENCHMARKS_SOURCES})
endif (BUILD_UBENCHMARKS)

add_dependencies (${PROJECT_UBENCHMARKS_NAME} googlebench)

#
# add optional dependencies
#
if (BUILD_UBENCHMARKS)
  #if (ZLIB_BUILTIN)
  #  add_dependencies (${PROJECT_UBENCHMARKS_NAME} zlib)
  #endif (ZLIB_BUILTIN)

  add_dependencies (${PROJECT_UBENCHMARKS_NAME} libsha3)
endif (BUILD_UBENCHMARKS)


#
# set build flags
#
set_target_properties (${PROJECT_UBENCHMARKS_NAME} PROPERTIES
                       COMPILE_FLAGS "${CVMFS_UBENCHMARKS_CFLAGS}"
                       LINK_FLAGS "${CVMFS_UBENCHMARKS_LD_FLAGS}")

#
# link the stuff (*_LIBRARIES are dynamic link libraries)
#
set (UBENCHMARKS_LINK_LIBRARIES ${GOOGLEBENCH_ARCHIVE} ${RT_LIBRARY}
                                pthread dl)

target_link_libraries (${PROJECT_UBENCHMARKS_NAME} ${UBENCHMARKS_LINK_LIBRARIES})
