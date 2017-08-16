
#
# global micro quickcheck configuration
#
set (PROJECT_QC_NAME "cvmfs_qc")

# Add the test/common directory to the include path
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../common)

#
# micro quickcheck files
#
set(CVMFS_QC_FILES
  main.cc

  ../common/env.cc
  ../common/testutil.cc
  ../common/catalog_test_tools.cc

  qc_dummy.cc
)

#
# unit test source files
#
set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_QC_SOURCES

  ${CVMFS_QC_FILES}

  # dependencies
  ${CVMFS_SOURCE_DIR}/cache_transport.cc
  ${CVMFS_SOURCE_DIR}/catalog.cc
  ${CVMFS_SOURCE_DIR}/catalog_counters.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_ro.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_rw.cc
  ${CVMFS_SOURCE_DIR}/catalog_rw.cc
  ${CVMFS_SOURCE_DIR}/catalog_sql.cc
  ${CVMFS_SOURCE_DIR}/compression.cc
  ${CVMFS_SOURCE_DIR}/directory_entry.cc
  ${CVMFS_SOURCE_DIR}/dns.cc
  ${CVMFS_SOURCE_DIR}/download.cc
  ${CVMFS_SOURCE_DIR}/file_processing/async_reader.cc
  ${CVMFS_SOURCE_DIR}/file_processing/chunk.cc
  ${CVMFS_SOURCE_DIR}/file_processing/chunk_detector.cc
  ${CVMFS_SOURCE_DIR}/file_processing/file.cc
  ${CVMFS_SOURCE_DIR}/file_processing/file_processor.cc
  ${CVMFS_SOURCE_DIR}/file_processing/io_dispatcher.cc
  ${CVMFS_SOURCE_DIR}/file_processing/processor.cc
  ${CVMFS_SOURCE_DIR}/gateway_util.cc
  ${CVMFS_SOURCE_DIR}/globals.cc
  ${CVMFS_SOURCE_DIR}/glue_buffer.cc
  ${CVMFS_SOURCE_DIR}/hash.cc
  ${CVMFS_SOURCE_DIR}/json_document.cc
  ${CVMFS_SOURCE_DIR}/logging.cc
  ${CVMFS_SOURCE_DIR}/malloc_arena.cc
  ${CVMFS_SOURCE_DIR}/manifest.cc
  ${CVMFS_SOURCE_DIR}/monitor.cc
  ${CVMFS_SOURCE_DIR}/options.cc
  ${CVMFS_SOURCE_DIR}/pack.cc
  ${CVMFS_SOURCE_DIR}/reflog.cc
  ${CVMFS_SOURCE_DIR}/reflog_sql.cc
  ${CVMFS_SOURCE_DIR}/upload_facility.cc
  ${CVMFS_SOURCE_DIR}/upload_local.cc
  ${CVMFS_SOURCE_DIR}/upload_gateway.cc
  ${CVMFS_SOURCE_DIR}/upload_s3.cc
  ${CVMFS_SOURCE_DIR}/upload_spooler_definition.cc
  ${CVMFS_SOURCE_DIR}/sanitizer.cc
  ${CVMFS_SOURCE_DIR}/session_context.cc
  ${CVMFS_SOURCE_DIR}/sql.cc
  ${CVMFS_SOURCE_DIR}/sqlitemem.cc
  ${CVMFS_SOURCE_DIR}/statistics.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_json.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_curl.cc
  ${CVMFS_SOURCE_DIR}/s3fanout.cc
  ${CVMFS_SOURCE_DIR}/upload.cc
  ${CVMFS_SOURCE_DIR}/util/algorithm.cc
  ${CVMFS_SOURCE_DIR}/util/mmap_file.cc
  ${CVMFS_SOURCE_DIR}/util/posix.cc
  ${CVMFS_SOURCE_DIR}/util/string.cc
  ${CVMFS_SOURCE_DIR}/util_concurrency.cc
  ${CVMFS_SOURCE_DIR}/xattr.cc
  cache.pb.cc cache.pb.h
)

# First .h then .cc is important to avoid races during the build process
set_source_files_properties(cache.pb.h cache.pb.cc
                            PROPERTIES GENERATED true)

add_custom_command(OUTPUT cache.pb.h cache.pb.cc
                   COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} --cpp_out=.
                           ${CVMFS_SOURCE_DIR}/cache.proto
                           -I${CVMFS_SOURCE_DIR}
                   DEPENDS ${PROTOBUF_PROTOC_EXECUTABLE}
                           ${CVMFS_SOURCE_DIR}/cache.proto
                   COMMENT "Generating protobuf sources")

add_custom_target(cache.pb.generated-qc
                  DEPENDS cache.pb.h cache.pb.cc)


#
# Compiler and Linker Flags for quickcheck tests
#
set (CVMFS_QC_CFLAGS "${CVMFS_QC_CFLAGS} -fexceptions -std=c++11 -DCVMFS_LIBCVMFS")
set (CVMFS_QC_LDFLAGS "${CVMFS_QC_LDFLAGS}")

#
# build CernVM-FS quickcheck tests
#
include_directories (${CMAKE_CURRENT_BINARY_DIR} ${INCLUDE_DIRECTORIES})

add_executable (${PROJECT_QC_NAME} ${CVMFS_QC_SOURCES})
add_dependencies (${PROJECT_QC_NAME} cache.pb.generated-qc)

#
# set build flags
#
set_target_properties (${PROJECT_QC_NAME} PROPERTIES
                       COMPILE_FLAGS "${CVMFS_QC_CFLAGS}"
                       LINK_FLAGS "${CVMFS_QC_LD_FLAGS}")

#
# link the stuff (*_LIBRARIES are dynamic link libraries)
#
set (QC_LINK_LIBRARIES
  ${RAPIDCHECK_LIBRARIES} ${GTEST_LIBRARIES} ${SQLITE3_LIBRARY}
  ${CURL_LIBRARIES} ${CARES_LIBRARIES} ${OPENSSL_LIBRARIES}
  ${RT_LIBRARY} ${ZLIB_LIBRARIES} ${SHA3_LIBRARIES} ${PROTOBUF_LITE_LIBRARY}
  ${VJSON_LIBRARIES} ${TBB_LIBRARIES}
  pthread dl)

target_link_libraries (${PROJECT_QC_NAME} ${QC_LINK_LIBRARIES})
