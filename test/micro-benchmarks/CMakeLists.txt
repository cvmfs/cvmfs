
#
# global micro benchmark configuration
#
set (PROJECT_UBENCHMARKS_NAME "cvmfs_ubenchmarks")

#
# micro benchmarks files
#
set(CVMFS_UBENCHMARKS_FILES
  bm_util.h
  main.cc

  b_compression.cc
  b_gluebuffer.cc
  b_hash.cc
  b_smallhash.cc
  b_syscalls.cc
)

#
# unit test source files
#
set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_UBENCHMARKS_SOURCES

  ${CVMFS_UBENCHMARKS_FILES}

  # dependencies
  ${CVMFS_SOURCE_DIR}/atomic.h
  ${CVMFS_SOURCE_DIR}/bigvector.h
  ${CVMFS_SOURCE_DIR}/compression.cc ${CVMFS_SOURCE_DIR}/compression.h
  ${CVMFS_SOURCE_DIR}/directory_entry.cc ${CVMFS_SOURCE_DIR}/directory_entry.h
  ${CVMFS_SOURCE_DIR}/duplex_zlib.h
  ${CVMFS_SOURCE_DIR}/fs_traversal.h
  ${CVMFS_SOURCE_DIR}/glue_buffer.cc ${CVMFS_SOURCE_DIR}/glue_buffer.h
  ${CVMFS_SOURCE_DIR}/logging.cc ${CVMFS_SOURCE_DIR}/logging.h ${CVMFS_SOURCE_DIR}/logging_internal.h
  ${CVMFS_SOURCE_DIR}/hash.cc ${CVMFS_SOURCE_DIR}/hash.h
  ${CVMFS_SOURCE_DIR}/murmur.h
  ${CVMFS_SOURCE_DIR}/platform.h
  ${CVMFS_SOURCE_DIR}/prng.h
  ${CVMFS_SOURCE_DIR}/sink.h
  ${CVMFS_SOURCE_DIR}/shortstring.h
  ${CVMFS_SOURCE_DIR}/smallhash.h
  ${CVMFS_SOURCE_DIR}/smalloc.h
  ${CVMFS_SOURCE_DIR}/util/algorithm.cc ${CVMFS_SOURCE_DIR}/util/algorithm.h
  ${CVMFS_SOURCE_DIR}/util/plugin.h
  ${CVMFS_SOURCE_DIR}/util/pointer.h
  ${CVMFS_SOURCE_DIR}/util/posix.cc ${CVMFS_SOURCE_DIR}/util/posix.h
  ${CVMFS_SOURCE_DIR}/util/single_copy.h
  ${CVMFS_SOURCE_DIR}/util/string.cc ${CVMFS_SOURCE_DIR}/util/string.h
)


#
# Compiler and Linker Flags for micro benchmarks
#
set (CVMFS_UBENCHMARKS_CFLAGS "${CVMFS_UBENCHMARKS_CFLAGS} -fexceptions")
set (CVMFS_UBENCHMARKS_LDFLAGS "${CVMFS_UBENCHMARKS_LDFLAGS}")

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
  if (ZLIB_BUILTIN)
    add_dependencies (${PROJECT_UBENCHMARKS_NAME} zlib)
  endif (ZLIB_BUILTIN)

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
set (UBENCHMARKS_LINK_LIBRARIES ${GOOGLEBENCH_ARCHIVE} ${OPENSSL_LIBRARIES}
                                ${RT_LIBRARY} ${ZLIB_LIBRARIES} ${ZLIB_ARCHIVE}
                                ${RT_LIBRARY} ${SHA3_ARCHIVE} pthread dl)

target_link_libraries (${PROJECT_UBENCHMARKS_NAME} ${UBENCHMARKS_LINK_LIBRARIES})
