set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")

set (CVMFS_STRESS_SOURCES
  ${CVMFS_SOURCE_DIR}/compression.cc
  ${CVMFS_SOURCE_DIR}/dns.cc
  ${CVMFS_SOURCE_DIR}/gateway_util.cc
  ${CVMFS_SOURCE_DIR}/hash.cc
  ${CVMFS_SOURCE_DIR}/json_document.cc
  ${CVMFS_SOURCE_DIR}/logging.cc
  ${CVMFS_SOURCE_DIR}/options.cc
  ${CVMFS_SOURCE_DIR}/pack.cc
  ${CVMFS_SOURCE_DIR}/s3fanout.cc
  ${CVMFS_SOURCE_DIR}/sanitizer.cc
  ${CVMFS_SOURCE_DIR}/session_context.cc
  ${CVMFS_SOURCE_DIR}/statistics.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_curl.cc
  ${CVMFS_SOURCE_DIR}/upload_facility.cc
  ${CVMFS_SOURCE_DIR}/upload_gateway.cc
  ${CVMFS_SOURCE_DIR}/upload_local.cc
  ${CVMFS_SOURCE_DIR}/upload_s3.cc
  ${CVMFS_SOURCE_DIR}/upload_spooler_definition.cc
  ${CVMFS_SOURCE_DIR}/util/exception.cc
  ${CVMFS_SOURCE_DIR}/util/file_backed_buffer.cc
  ${CVMFS_SOURCE_DIR}/util/mmap_file.cc
  ${CVMFS_SOURCE_DIR}/util/posix.cc
  ${CVMFS_SOURCE_DIR}/util/string.cc
)

set (CVMFS_S3_MOCK_SERVER_SOURCES
  ${CVMFS_SOURCE_DIR}/logging.cc
  ${CVMFS_SOURCE_DIR}/malloc_arena.cc
  ${CVMFS_SOURCE_DIR}/malloc_heap.cc
  ${CVMFS_SOURCE_DIR}/util/algorithm.cc
  ${CVMFS_SOURCE_DIR}/util/exception.cc
  ${CVMFS_SOURCE_DIR}/util/posix.cc
  ${CVMFS_SOURCE_DIR}/util/string.cc
)


add_executable(s3benchmark test/stress/s3benchmark.cc ${CVMFS_STRESS_SOURCES})

target_link_libraries (s3benchmark
${CURL_LIBRARIES} ${CARES_LIBRARIES} ${CARES_LDFLAGS}
${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES}
${VJSON_LIBRARIES} ${SHA3_LIBRARIES} pthread dl)

add_executable(s3mockserver test/stress/s3mockserver.cc ${CVMFS_S3_MOCK_SERVER_SOURCES})

target_link_libraries (s3mockserver pthread dl)
