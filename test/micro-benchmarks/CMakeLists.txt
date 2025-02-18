set (PROJECT_UBENCHMARKS_NAME "cvmfs_ubenchmarks")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../common)

set(CVMFS_UBENCHMARKS_FILES
  main.cc

  # test utility functions
  ../common/env.cc
  ../common/testutil.cc
  ../common/catalog_test_tools.cc

  b_catalog_grafting.cc
  b_compression.cc
  b_gluebuffer.cc
  b_hash.cc
  b_smallhash.cc
  b_syscalls.cc
  b_messaging.cc
  b_utils.cc
)

set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_UBENCHMARKS_SOURCES
  ${CVMFS_UBENCHMARKS_FILES}

  # dependencies
  ${CVMFS_SOURCE_DIR}/backoff.cc
  ${CVMFS_SOURCE_DIR}/cache_transport.cc
  ${CVMFS_SOURCE_DIR}/catalog.cc
  ${CVMFS_SOURCE_DIR}/catalog_counters.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_ro.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_rw.cc
  ${CVMFS_SOURCE_DIR}/catalog_rw.cc
  ${CVMFS_SOURCE_DIR}/catalog_sql.cc
  ${CVMFS_SOURCE_DIR}/compression/compression.cc
  ${CVMFS_SOURCE_DIR}/crypto/hash.cc
  ${CVMFS_SOURCE_DIR}/crypto/signature.cc
  ${CVMFS_SOURCE_DIR}/directory_entry.cc
  ${CVMFS_SOURCE_DIR}/glue_buffer.cc
  ${CVMFS_SOURCE_DIR}/ingestion/chunk_detector.cc
  ${CVMFS_SOURCE_DIR}/ingestion/item.cc
  ${CVMFS_SOURCE_DIR}/ingestion/item_mem.cc
  ${CVMFS_SOURCE_DIR}/ingestion/pipeline.cc
  ${CVMFS_SOURCE_DIR}/ingestion/task_chunk.cc
  ${CVMFS_SOURCE_DIR}/ingestion/task_compress.cc
  ${CVMFS_SOURCE_DIR}/ingestion/task_hash.cc
  ${CVMFS_SOURCE_DIR}/ingestion/task_read.cc
  ${CVMFS_SOURCE_DIR}/ingestion/task_register.cc
  ${CVMFS_SOURCE_DIR}/ingestion/task_write.cc
  ${CVMFS_SOURCE_DIR}/gateway_util.cc
  ${CVMFS_SOURCE_DIR}/globals.cc
  ${CVMFS_SOURCE_DIR}/history_sql.cc
  ${CVMFS_SOURCE_DIR}/history_sqlite.cc
  ${CVMFS_SOURCE_DIR}/json_document.cc
  ${CVMFS_SOURCE_DIR}/malloc_arena.cc
  ${CVMFS_SOURCE_DIR}/manifest.cc
  ${CVMFS_SOURCE_DIR}/manifest_fetch.cc
  ${CVMFS_SOURCE_DIR}/monitor.cc
  ${CVMFS_SOURCE_DIR}/network/dns.cc
  ${CVMFS_SOURCE_DIR}/network/download.cc
  ${CVMFS_SOURCE_DIR}/network/jobinfo.cc
  ${CVMFS_SOURCE_DIR}/network/s3fanout.cc
  ${CVMFS_SOURCE_DIR}/network/sink_file.cc
  ${CVMFS_SOURCE_DIR}/network/sink_mem.cc
  ${CVMFS_SOURCE_DIR}/network/sink_path.cc
  ${CVMFS_SOURCE_DIR}/options.cc
  ${CVMFS_SOURCE_DIR}/pack.cc
  ${CVMFS_SOURCE_DIR}/receiver/lease_path_util.cc
  ${CVMFS_SOURCE_DIR}/reflog.cc
  ${CVMFS_SOURCE_DIR}/reflog_sql.cc
  ${CVMFS_SOURCE_DIR}/upload_facility.cc
  ${CVMFS_SOURCE_DIR}/upload_local.cc
  ${CVMFS_SOURCE_DIR}/upload_gateway.cc
  ${CVMFS_SOURCE_DIR}/upload_s3.cc
  ${CVMFS_SOURCE_DIR}/upload_spooler_definition.cc
  ${CVMFS_SOURCE_DIR}/sanitizer.cc
  ${CVMFS_SOURCE_DIR}/server_tool.cc
  ${CVMFS_SOURCE_DIR}/session_context.cc
  ${CVMFS_SOURCE_DIR}/shortstring.cc
  ${CVMFS_SOURCE_DIR}/ssl.cc
  ${CVMFS_SOURCE_DIR}/sql.cc
  ${CVMFS_SOURCE_DIR}/sqlitemem.cc
  ${CVMFS_SOURCE_DIR}/statistics.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_json.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_curl.cc
  ${CVMFS_SOURCE_DIR}/upload.cc
  ${CVMFS_SOURCE_DIR}/util/algorithm.cc
  ${CVMFS_SOURCE_DIR}/util/concurrency.cc
  ${CVMFS_SOURCE_DIR}/util/exception.cc
  ${CVMFS_SOURCE_DIR}/util/file_backed_buffer.cc
  ${CVMFS_SOURCE_DIR}/util/logging.cc
  ${CVMFS_SOURCE_DIR}/util/mmap_file.cc
  ${CVMFS_SOURCE_DIR}/util/posix.cc
  ${CVMFS_SOURCE_DIR}/util/raii_temp_dir.cc
  ${CVMFS_SOURCE_DIR}/util/string.cc
  ${CVMFS_SOURCE_DIR}/whitelist.cc
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

add_custom_target(cache.pb.generated-ubenchmarks
                  DEPENDS cache.pb.h cache.pb.cc)


set (CVMFS_UBENCHMARKS_CFLAGS "${CVMFS_UBENCHMARKS_CFLAGS} -fexceptions")
set (CVMFS_UBENCHMARKS_LDFLAGS "${CVMFS_UBENCHMARKS_LDFLAGS}")

include_directories (${CMAKE_CURRENT_BINARY_DIR} ${INCLUDE_DIRECTORIES})

add_executable (${PROJECT_UBENCHMARKS_NAME} ${CVMFS_UBENCHMARKS_SOURCES})
add_dependencies (${PROJECT_UBENCHMARKS_NAME} cache.pb.generated-ubenchmarks)

set_target_properties (${PROJECT_UBENCHMARKS_NAME} PROPERTIES
                       COMPILE_FLAGS "${CVMFS_UBENCHMARKS_CFLAGS}"
                       LINK_FLAGS "${CVMFS_UBENCHMARKS_LD_FLAGS}")

set (UBENCHMARKS_LINK_LIBRARIES ${GOOGLEBENCH_LIBRARIES}
                                ${GTEST_LIBRARIES}
                                ${CURL_LIBRARIES}
                                ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                                ${OPENSSL_LIBRARIES}
                                ${Libcrypto_LIBRARIES}
                                ${SHA3_LIBRARIES}
                                ${PROTOBUF_LITE_LIBRARY}
                                ${ZLIB_LIBRARIES}
                                ${RT_LIBRARY}
                                ${SQLITE3_LIBRARY}
                                ${VJSON_LIBRARIES}
                                pthread dl)

target_link_libraries (${PROJECT_UBENCHMARKS_NAME} ${UBENCHMARKS_LINK_LIBRARIES})