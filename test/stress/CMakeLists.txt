set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")

add_executable(s3benchmark
               test/stress/s3benchmark.cc
               ${CVMFS_SOURCE_DIR}/compression.cc
               ${CVMFS_SOURCE_DIR}/gateway_util.cc
               ${CVMFS_SOURCE_DIR}/json_document.cc
               ${CVMFS_SOURCE_DIR}/network/dns.cc
               ${CVMFS_SOURCE_DIR}/network/s3fanout.cc
               ${CVMFS_SOURCE_DIR}/options.cc
               ${CVMFS_SOURCE_DIR}/pack.cc
               ${CVMFS_SOURCE_DIR}/sanitizer.cc
               ${CVMFS_SOURCE_DIR}/session_context.cc
               ${CVMFS_SOURCE_DIR}/ssl.cc
               ${CVMFS_SOURCE_DIR}/statistics.cc
               ${CVMFS_SOURCE_DIR}/swissknife_lease_curl.cc
               ${CVMFS_SOURCE_DIR}/upload_facility.cc
               ${CVMFS_SOURCE_DIR}/upload_gateway.cc
               ${CVMFS_SOURCE_DIR}/upload_local.cc
               ${CVMFS_SOURCE_DIR}/upload_s3.cc
               ${CVMFS_SOURCE_DIR}/upload_spooler_definition.cc
)

target_link_libraries (s3benchmark
                       cvmfs_crypto
                       cvmfs_util
                       ${CURL_LIBRARIES}
                       ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                       ${ZLIB_LIBRARIES}
                       ${OPENSSL_LIBRARIES}
                       ${VJSON_LIBRARIES}
                       pthread
                       dl
)


add_executable(s3mockserver
               test/stress/s3mockserver.cc
               ${CVMFS_SOURCE_DIR}/malloc_arena.cc
               ${CVMFS_SOURCE_DIR}/malloc_heap.cc
)

target_link_libraries (s3mockserver
                       cvmfs_util
                       pthread
                       dl
)
