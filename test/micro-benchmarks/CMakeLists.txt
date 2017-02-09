
#
# global micro benchmark configuration
#
set (PROJECT_UBENCHMARKS_NAME "cvmfs_ubenchmarks")

#
# micro benchmarks files
#
set(CVMFS_UBENCHMARKS_FILES
  main.cc

  b_compression.cc
  b_gluebuffer.cc
  b_hash.cc
  b_smallhash.cc
  b_syscalls.cc
  b_messaging.cc
)

#
# unit test source files
#
set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_UBENCHMARKS_SOURCES

  ${CVMFS_UBENCHMARKS_FILES}

  # dependencies
  ${CVMFS_SOURCE_DIR}/cache_transport.cc
  ${CVMFS_SOURCE_DIR}/compression.cc
  ${CVMFS_SOURCE_DIR}/directory_entry.cc
  ${CVMFS_SOURCE_DIR}/glue_buffer.cc
  ${CVMFS_SOURCE_DIR}/logging.cc
  ${CVMFS_SOURCE_DIR}/hash.cc
  ${CVMFS_SOURCE_DIR}/util/algorithm.cc
  ${CVMFS_SOURCE_DIR}/util/posix.cc
  ${CVMFS_SOURCE_DIR}/util/string.cc
  cache.pb.cc
)

set_source_files_properties(cache.pb.cc cache.pb.h
                            PROPERTIES GENERATED true)

add_custom_command(OUTPUT cache.pb.cc cache.pb.h
                   COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} --cpp_out=.
                           ${CVMFS_SOURCE_DIR}/cache.proto
                           -I${CVMFS_SOURCE_DIR}
                   DEPENDS ${PROTOBUF_ARCHIVE}
                           ${CVMFS_SOURCE_DIR}/cache.proto
                   COMMENT "Generating protobuf sources")


#
# Compiler and Linker Flags for micro benchmarks
#
set (CVMFS_UBENCHMARKS_CFLAGS "${CVMFS_UBENCHMARKS_CFLAGS} -fexceptions")
set (CVMFS_UBENCHMARKS_LDFLAGS "${CVMFS_UBENCHMARKS_LDFLAGS}")

#
# build CernVM-FS micro benchmarks
#
include_directories (${CMAKE_CURRENT_BINARY_DIR} ${INCLUDE_DIRECTORIES})

if (BUILD_UBENCHMARKS)
  add_executable (${PROJECT_UBENCHMARKS_NAME} ${CVMFS_UBENCHMARKS_SOURCES})
else (BUILD_UBENCHMARKS)
  add_executable (${PROJECT_UBENCHMARKS_NAME} EXCLUDE_FROM_ALL ${CVMFS_UBENCHMARKS_SOURCES})
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
set (UBENCHMARKS_LINK_LIBRARIES ${GOOGLEBENCH_LIBRARIES} ${OPENSSL_LIBRARIES}
                                ${RT_LIBRARY} ${ZLIB_LIBRARIES}
                                ${RT_LIBRARY} ${SHA3_LIBRARIES}
                                ${PROTOBUF_LIBRARIES} pthread dl)

target_link_libraries (${PROJECT_UBENCHMARKS_NAME} ${UBENCHMARKS_LINK_LIBRARIES})
