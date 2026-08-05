
#
# global micro quickcheck configuration
#
set (PROJECT_QC_NAME "cvmfs_qc")

# Add the test/common directory to the include path
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../common)

#
# FuzzTest (property-based / fuzz testing framework).
#
# FuzzTest requires C++17 and pulls in its own GoogleTest, Abseil and RE2 via
# FetchContent. In unit-test mode (the default, FUZZTEST_FUZZING_MODE OFF) each
# FUZZ_TEST runs as a bounded GoogleTest case with randomized iterations -- the
# direct replacement for the previous RapidCheck property tests. Coverage-guided
# fuzzing can be enabled later by building with Clang and -fsanitize=fuzzer.
#
# FuzzTest publishes date-based releases with auto-generated source tarballs.
# Update FUZZTEST_VERSION (and the URL_HASH below) to bump the version. Follows
# the same local-archive-or-download pattern as GoogleTest in
# test/unittests/CMakeLists.txt.
#
Include(FetchContent)

# FuzzTest and its transitively fetched dependencies (notably antlr_cpp, used
# for grammar-based fuzzing) require C++ exceptions, but cvmfs is compiled with
# -fno-exceptions globally (set in cmake/Modules/cvmfs_compiler.cmake). Swap in
# -fexceptions for everything built in this directory scope, which covers the
# FetchContent targets created by FetchContent_MakeAvailable below. This does
# not affect the main cvmfs build in other directories, and the cvmfs_qc target
# already forces -fexceptions for its own sources.
string(REPLACE "-fno-exceptions" "-fexceptions"
       CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

set(FUZZTEST_VERSION "2026-06-29" CACHE STRING "FuzzTest release to build")
message(STATUS "Fetching FuzzTest ${FUZZTEST_VERSION} for quickcheck tests...")

set(FUZZTEST_LOCAL_PATH
    "${PROJECT_SOURCE_DIR}/externals/fuzztest-${FUZZTEST_VERSION}.tar.gz")

if(EXISTS "${FUZZTEST_LOCAL_PATH}")
    message(STATUS "Using local FuzzTest archive: ${FUZZTEST_LOCAL_PATH}")
    FetchContent_Declare(
        fuzztest
        DOWNLOAD_DIR ${PROJECT_SOURCE_DIR}/externals
        URL "file://${FUZZTEST_LOCAL_PATH}"
        URL_HASH MD5=7a98889baf1fc17db983d40d2cf5db7b
    )
else()
    message(STATUS "Local FuzzTest archive not found, downloading from remote")
    FetchContent_Declare(
        fuzztest
        DOWNLOAD_DIR ${PROJECT_SOURCE_DIR}/externals
        URL https://github.com/google/fuzztest/releases/download/${FUZZTEST_VERSION}/fuzztest-${FUZZTEST_VERSION}.tar.gz
            https://ecsft.cern.ch/dist/cvmfs/build_externals/fuzztest-${FUZZTEST_VERSION}.tar.gz
        URL_HASH MD5=7a98889baf1fc17db983d40d2cf5db7b
    )
endif()

FetchContent_MakeAvailable(fuzztest)

#
# micro quickcheck files
#
set(CVMFS_QC_FILES
  main.cc

  ../common/env.cc
  ../common/testutil.cc
  ../common/catalog_test_tools.cc

  qc_example.cc
  qc_catalog_merge_tool.cc
)

#
# unit test source files
#
set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_QC_SOURCES

  ${CVMFS_QC_FILES}

  # dependencies
  ${CVMFS_SOURCE_DIR}/backoff.cc
  ${CVMFS_SOURCE_DIR}/cache_transport.cc
  ${CVMFS_SOURCE_DIR}/catalog.cc
  ${CVMFS_SOURCE_DIR}/catalog_counters.cc
  ${CVMFS_SOURCE_DIR}/catalog_downloader.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_ro.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_rw.cc
  ${CVMFS_SOURCE_DIR}/catalog_rw.cc
  ${CVMFS_SOURCE_DIR}/catalog_sql.cc
  ${CVMFS_SOURCE_DIR}/compression/compressor.cc
  ${CVMFS_SOURCE_DIR}/compression/compressor_echo.cc
  ${CVMFS_SOURCE_DIR}/compression/compressor_zlib.cc
  ${CVMFS_SOURCE_DIR}/compression/compressor_zstd.cc
  ${CVMFS_SOURCE_DIR}/compression/decompressor.cc
  ${CVMFS_SOURCE_DIR}/compression/decompressor_echo.cc
  ${CVMFS_SOURCE_DIR}/compression/decompressor_guess.cc
  ${CVMFS_SOURCE_DIR}/compression/decompressor_zlib.cc
  ${CVMFS_SOURCE_DIR}/compression/decompressor_zstd.cc
  ${CVMFS_SOURCE_DIR}/compression/input_cache.cc
  ${CVMFS_SOURCE_DIR}/compression/input_fd.cc
  ${CVMFS_SOURCE_DIR}/compression/input_file.cc
  ${CVMFS_SOURCE_DIR}/compression/input_mem.cc
  ${CVMFS_SOURCE_DIR}/compression/input_path.cc
  ${CVMFS_SOURCE_DIR}/compression/util.cc
  ${CVMFS_SOURCE_DIR}/crypto/hash.cc
  ${CVMFS_SOURCE_DIR}/crypto/signature.cc
  ${CVMFS_SOURCE_DIR}/compression/compression.cc
  ${CVMFS_SOURCE_DIR}/directory_entry.cc
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
  ${CVMFS_SOURCE_DIR}/glue_buffer.cc
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
  ${CVMFS_SOURCE_DIR}/shortstring.cc
  ${CVMFS_SOURCE_DIR}/reflog.cc
  ${CVMFS_SOURCE_DIR}/reflog_sql.cc
  ${CVMFS_SOURCE_DIR}/upload_facility.cc
  ${CVMFS_SOURCE_DIR}/upload_local.cc
  ${CVMFS_SOURCE_DIR}/upload_gateway.cc
  ${CVMFS_SOURCE_DIR}/upload_s3.cc
  ${CVMFS_SOURCE_DIR}/upload_spooler_definition.cc
  ${CVMFS_SOURCE_DIR}/sanitizer.cc
  ${CVMFS_SOURCE_DIR}/server_tool.cc
  ${CVMFS_SOURCE_DIR}/ssl.cc
  ${CVMFS_SOURCE_DIR}/session_context.cc
  ${CVMFS_SOURCE_DIR}/sql.cc
  ${CVMFS_SOURCE_DIR}/sqlitemem.cc
  ${CVMFS_SOURCE_DIR}/statistics.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_json.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_curl.cc
  ${CVMFS_SOURCE_DIR}/upload.cc
  ${CVMFS_SOURCE_DIR}/util/algorithm.cc
  ${CVMFS_SOURCE_DIR}/util/capabilities.cc
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

add_custom_target(cache.pb.generated-qc
                  DEPENDS cache.pb.h cache.pb.cc)


#
# Compiler and Linker Flags for quickcheck tests
#
# NOTE: C++17 (required by FuzzTest) rather than the C++11 used elsewhere; this
# is an isolated executable so the standard bump is local to the qc target. The
# _GLIBCXX_DEBUG flag is intentionally dropped: it changes the libstdc++ ABI and
# is incompatible with the prebuilt FuzzTest/Abseil libraries.
set (CVMFS_QC_CFLAGS "${CVMFS_QC_CFLAGS} -fexceptions -std=c++17 -DCVMFS_LIBCVMFS")
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
                       CXX_STANDARD 17
                       CXX_STANDARD_REQUIRED ON
                       COMPILE_FLAGS "${CVMFS_QC_CFLAGS}"
                       LINK_FLAGS "${CVMFS_QC_LD_FLAGS}")

#
# link the stuff (*_LIBRARIES are dynamic link libraries)
#
# fuzztest::fuzztest provides the FUZZ_TEST macros and transitively pulls in the
# GoogleTest that FuzzTest fetched, so no separate GTEST_LIBRARIES is needed.
set (QC_LINK_LIBRARIES
  # libcvmfs_crypto wraps the vendored (patched) OpenSSL used for hashing and
  # signing. It must be linked instead of compiling crypto/hash.cc and
  # crypto/signature.cc directly against the system OpenSSL: the system OpenSSL
  # 3.x crypto policy rejects SHA-1 signatures, which SignatureManager relies
  # on, so KeysMatch() and manifest verification would fail. The rest of the
  # binary still links the system OpenSSL (below) for libcurl/TLS.
  cvmfs_crypto
  fuzztest::fuzztest
  fuzztest::init_fuzztest
  GTest::gtest
  SQLite::SQLite3
  CURL::libcurl
  c-ares::cares
  ${CARES_LDFLAGS}
  ${OPENSSL_LIBRARIES}
  ${RT_LIBRARY}
  ${ZLIB_LIBRARIES}
  ${SHA3_LIBRARIES}
  ${PROTOBUF_LITE_LIBRARY}
  nlohmann_json::nlohmann_json
  ${TBB_LIBRARIES}
  ${CAP_LIBRARIES}
  ${ZSTD_LIBRARIES}
  pthread dl
  )

target_link_libraries (${PROJECT_QC_NAME} ${QC_LINK_LIBRARIES})
