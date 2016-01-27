
#
# global test configuration
#
set (PROJECT_TEST_NAME "cvmfs_unittests")
set (PROJECT_TEST_DEBUG_NAME ${PROJECT_TEST_NAME}_debug)

#
# unit test files
#
set(CVMFS_UNITTEST_FILES
  # test steering
  main.cc

  # test utility functions
  testutil.cc testutil.h
  env.cc env.h

  t_atomic.cc
  t_smallhash.cc
  t_bigvector.cc
  t_util.cc
  t_util_concurrency.cc
  t_polymorphic_construction.cc
  t_catalog_counters.cc
  t_catalog_traversal.cc
  t_catalog_mgr.cc
  t_fs_traversal.cc
  t_pipe.cc
  t_prng.cc
  t_buffer.cc
  t_chunk_detectors.cc
  t_upload_facility.cc
  t_uploaders.cc
  t_file_processing.cc
  t_async_reader.cc
  t_test_utils.cc
  t_sanitizer.cc
  t_file_sandbox.cc
  t_synchronizing_counter.cc
  t_blocking_counter.cc
  t_shash.cc
  t_header_lists.cc
  t_base64.cc
  t_pathspec.cc
  t_dirtab.cc
  t_relaxed_path_filter.cc
  t_callbacks.cc
  t_lru.cc
  t_sqlite_database.cc
  t_unique_ptr.cc
  t_unlink_guard.cc
  t_history.cc
  t_dns.cc
  t_garbage_collector.cc
  t_hash_filters.cc
  t_download.cc
  t_uuid.cc
  t_object_fetcher.cc
  t_catalog_sql.cc
  t_catalog.cc
  t_xattr.cc
  t_statistics.cc
  t_options.cc
  t_cache.cc
  t_quota.cc
  t_libcvmfs.cc
  t_backoff.cc
  t_wpad.cc
  t_smalloc.cc
  t_uid_map.cc
  t_fetch.cc
  t_manifest.cc
  t_tracer.cc
  t_file_chunk.cc
  t_platforms.cc
  t_compressor.cc
  t_compression.cc
  t_clientctx.cc
)

if (VOMS_FOUND)
  set (CVMFS_UNITTEST_FILES ${CVMFS_UNITTEST_FILES} t_voms.cc)
endif (VOMS_FOUND)

#
# unit test source files
#
set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_UNITTEST_SOURCES

  ${CVMFS_UNITTEST_FILES}

  # test dependencies
  ${CVMFS_SOURCE_DIR}/atomic.h
  ${CVMFS_SOURCE_DIR}/logging.h
  ${CVMFS_SOURCE_DIR}/logging_internal.h
  ${CVMFS_SOURCE_DIR}/logging.cc
  ${CVMFS_SOURCE_DIR}/murmur.h
  ${CVMFS_SOURCE_DIR}/smallhash.h
  ${CVMFS_SOURCE_DIR}/bigvector.h
  ${CVMFS_SOURCE_DIR}/smalloc.h
  ${CVMFS_SOURCE_DIR}/util_concurrency.h
  ${CVMFS_SOURCE_DIR}/util_concurrency.cc
  ${CVMFS_SOURCE_DIR}/util_concurrency_impl.h
  ${CVMFS_SOURCE_DIR}/uuid.h
  ${CVMFS_SOURCE_DIR}/uuid.cc

  ${CVMFS_SOURCE_DIR}/platform.h
  ${CVMFS_SOURCE_DIR}/platform_linux.h
  ${CVMFS_SOURCE_DIR}/platform_osx.h
  ${CVMFS_SOURCE_DIR}/prng.h
  ${CVMFS_SOURCE_DIR}/util.h
  ${CVMFS_SOURCE_DIR}/util.cc
  ${CVMFS_SOURCE_DIR}/hash.h
  ${CVMFS_SOURCE_DIR}/hash.cc
  ${CVMFS_SOURCE_DIR}/shortstring.h
  ${CVMFS_SOURCE_DIR}/sanitizer.h
  ${CVMFS_SOURCE_DIR}/sanitizer.cc
  ${CVMFS_SOURCE_DIR}/duplex_curl.h
  ${CVMFS_SOURCE_DIR}/download.h
  ${CVMFS_SOURCE_DIR}/download.cc
  ${CVMFS_SOURCE_DIR}/options.h
  ${CVMFS_SOURCE_DIR}/options.cc

  ${CVMFS_SOURCE_DIR}/globals.cc

  ${CVMFS_SOURCE_DIR}/catalog.cc
  ${CVMFS_SOURCE_DIR}/catalog_sql.h
  ${CVMFS_SOURCE_DIR}/catalog_sql.cc
  ${CVMFS_SOURCE_DIR}/catalog_counters.cc
  ${CVMFS_SOURCE_DIR}/catalog_rw.cc

  ${CVMFS_SOURCE_DIR}/file_processing/chunk_detector.cc
  ${CVMFS_SOURCE_DIR}/file_processing/file_processor.cc
  ${CVMFS_SOURCE_DIR}/file_processing/io_dispatcher.cc
  ${CVMFS_SOURCE_DIR}/file_processing/processor.cc
  ${CVMFS_SOURCE_DIR}/file_processing/file.cc
  ${CVMFS_SOURCE_DIR}/file_processing/chunk.cc
  ${CVMFS_SOURCE_DIR}/file_processing/async_reader.cc
  ${CVMFS_SOURCE_DIR}/upload_facility.cc
  ${CVMFS_SOURCE_DIR}/upload_local.cc
  ${CVMFS_SOURCE_DIR}/upload_s3.cc
  ${CVMFS_SOURCE_DIR}/s3fanout.cc
  ${CVMFS_SOURCE_DIR}/upload_spooler_definition.cc
  ${CVMFS_SOURCE_DIR}/file_chunk.cc
  ${CVMFS_SOURCE_DIR}/compression.cc
  ${CVMFS_SOURCE_DIR}/compression.h

  ${CVMFS_SOURCE_DIR}/pathspec/pathspec.cc
  ${CVMFS_SOURCE_DIR}/pathspec/pathspec.h
  ${CVMFS_SOURCE_DIR}/pathspec/pathspec_pattern.cc
  ${CVMFS_SOURCE_DIR}/pathspec/pathspec_pattern.h

  ${CVMFS_SOURCE_DIR}/path_filters/dirtab.cc
  ${CVMFS_SOURCE_DIR}/path_filters/dirtab.h
  ${CVMFS_SOURCE_DIR}/path_filters/relaxed_path_filter.cc
  ${CVMFS_SOURCE_DIR}/path_filters/relaxed_path_filter.h

  ${CVMFS_SOURCE_DIR}/manifest.cc
  ${CVMFS_SOURCE_DIR}/manifest.h
  ${CVMFS_SOURCE_DIR}/manifest_fetch.cc
  ${CVMFS_SOURCE_DIR}/manifest_fetch.h
  ${CVMFS_SOURCE_DIR}/signature.cc
  ${CVMFS_SOURCE_DIR}/whitelist.cc

  ${CVMFS_SOURCE_DIR}/history_sql.cc
  ${CVMFS_SOURCE_DIR}/history_sqlite.cc
  ${CVMFS_SOURCE_DIR}/sql.cc
  ${CVMFS_SOURCE_DIR}/duplex_cares.h
  ${CVMFS_SOURCE_DIR}/dns.h
  ${CVMFS_SOURCE_DIR}/dns.cc
  ${CVMFS_SOURCE_DIR}/xattr.h
  ${CVMFS_SOURCE_DIR}/xattr.cc
  ${CVMFS_SOURCE_DIR}/statistics.h
  ${CVMFS_SOURCE_DIR}/statistics.cc

  ${CVMFS_SOURCE_DIR}/cache.h
  ${CVMFS_SOURCE_DIR}/cache.cc
  ${CVMFS_SOURCE_DIR}/quota.h
  ${CVMFS_SOURCE_DIR}/quota.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr.h
  ${CVMFS_SOURCE_DIR}/catalog_mgr_impl.h
  ${CVMFS_SOURCE_DIR}/catalog_mgr_client.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_client.h
  ${CVMFS_SOURCE_DIR}/backoff.h
  ${CVMFS_SOURCE_DIR}/backoff.cc
  ${CVMFS_SOURCE_DIR}/monitor.h
  ${CVMFS_SOURCE_DIR}/monitor.cc
  ${CVMFS_SOURCE_DIR}/tracer.cc
  ${CVMFS_SOURCE_DIR}/tracer.h
  ${CVMFS_SOURCE_DIR}/wpad.h
  ${CVMFS_SOURCE_DIR}/wpad.cc
  ${CVMFS_SOURCE_DIR}/libcvmfs.h
  ${CVMFS_SOURCE_DIR}/libcvmfs.cc
  ${CVMFS_SOURCE_DIR}/libcvmfs_int.h
  ${CVMFS_SOURCE_DIR}/libcvmfs_int.cc
  ${CVMFS_SOURCE_DIR}/sink.h
  ${CVMFS_SOURCE_DIR}/uid_map.h
  ${CVMFS_SOURCE_DIR}/fetch.h
  ${CVMFS_SOURCE_DIR}/fetch.cc
  ${CVMFS_SOURCE_DIR}/sqlitevfs.cc
  ${CVMFS_SOURCE_DIR}/sqlitevfs.h
  ${CVMFS_SOURCE_DIR}/clientctx.h
  ${CVMFS_SOURCE_DIR}/clientctx.cc
)

if (VOMS_FOUND)
  set (CVMFS_UNITTEST_SOURCES ${CVMFS_UNITTEST_SOURCES}
       ${CVMFS_SOURCE_DIR}/voms_authz/voms_cred.cc
       ${CVMFS_SOURCE_DIR}/voms_authz/voms_authz.h
       ${CVMFS_SOURCE_DIR}/voms_authz/voms_curl.cc)
endif (VOMS_FOUND)

set (CVMFS_UNITTEST_DEBUG_SOURCES ${CVMFS_UNITTEST_SOURCES})

#
# Compiler and Linker Flags for unit tests
#
set (CVMFS_UNITTESTS_CFLAGS "${CVMFS_UNITTESTS_CFLAGS} -DGTEST_HAS_TR1_TUPLE=0 -DCVMFS_LIBCVMFS -D_FILE_OFFSET_BITS=64 -fexceptions")
set (CVMFS_UNITTESTS_DEBUG_CFLAGS "${CVMFS_UNITTESTS_CFLAGS} -O0 -DDEBUGMSG -g")
set (CVMFS_UNITTESTS_LD_FLAGS "${CVMFS_UNITTESTS_LD_FLAGS}")
set (CVMFS_UNITTESTS_DEBUG_LD_FLAGS "${CVMFS_UNITTESTS_LD_FLAGS}")

#
# build CernVM-FS test cases
#
include_directories (${INCLUDE_DIRECTORIES})

if (BUILD_UNITTESTS)
  add_executable (${PROJECT_TEST_NAME} ${CVMFS_UNITTEST_SOURCES})
else (BUILD_UNITTESTS)
  add_executable (${PROJECT_TEST_NAME} EXCLUDE_FROM_ALL ${CVMFS_UNITTEST_SOURCES})
endif (BUILD_UNITTESTS)

add_dependencies (${PROJECT_TEST_NAME} googletest)
add_dependencies (${PROJECT_TEST_NAME} sparsehash)
add_dependencies (${PROJECT_TEST_NAME} libsha3)
add_test (NAME ${PROJECT_TEST_NAME} COMMAND ${PROJECT_TEST_NAME} --gtest_filter=-*Slow)
add_dependencies (check ${PROJECT_TEST_NAME})

if (BUILD_UNITTESTS_DEBUG)
  add_executable (${PROJECT_TEST_DEBUG_NAME} ${CVMFS_UNITTEST_DEBUG_SOURCES})
  add_dependencies (${PROJECT_TEST_DEBUG_NAME} googletest)
  add_dependencies (${PROJECT_TEST_DEBUG_NAME} libvjson)
endif (BUILD_UNITTESTS_DEBUG)

#
# add optional dependencies
#
if (BUILD_UNITTESTS)
  if (LIBCURL_BUILTIN)
    add_dependencies (${PROJECT_TEST_NAME} libcares libcurl)
  endif (LIBCURL_BUILTIN)

  if (SQLITE3_BUILTIN)
    add_dependencies (${PROJECT_TEST_NAME} sqlite3)
  endif (SQLITE3_BUILTIN)

  if (ZLIB_BUILTIN)
    add_dependencies (${PROJECT_TEST_NAME} zlib)
  endif (ZLIB_BUILTIN)

  if (PACPARSER_BUILTIN)
    add_dependencies (${PROJECT_TEST_NAME} libpacparser)
  endif (PACPARSER_BUILTIN)

  if (TBB_PRIVATE_LIB)
    add_dependencies (${PROJECT_TEST_NAME} libtbb)
  endif (TBB_PRIVATE_LIB)
endif (BUILD_UNITTESTS)

if (BUILD_UNITTESTS_DEBUG)
  if (LIBCURL_BUILTIN)
    add_dependencies (${PROJECT_TEST_DEBUG_NAME} libcares libcurl)
  endif (LIBCURL_BUILTIN)

  if (SQLITE3_BUILTIN)
    add_dependencies (${PROJECT_TEST_DEBUG_NAME} sqlite3)
  endif (SQLITE3_BUILTIN)

  if (ZLIB_BUILTIN)
    add_dependencies (${PROJECT_TEST_DEBUG_NAME} zlib)
  endif (ZLIB_BUILTIN)

  if (PACPARSER_BUILTIN)
    add_dependencies (${PROJECT_TEST_DEBUG_NAME} libpacparser)
  endif (PACPARSER_BUILTIN)

  if (TBB_PRIVATE_LIB)
    add_dependencies (${PROJECT_TEST_DEBUG_NAME} libtbb)
  endif (TBB_PRIVATE_LIB)

  add_dependencies (${PROJECT_TEST_DEBUG_NAME} libsha3)
endif (BUILD_UNITTESTS_DEBUG)

#
# set build flags
#
set_target_properties (${PROJECT_TEST_NAME} PROPERTIES COMPILE_FLAGS "${CVMFS_UNITTESTS_CFLAGS}" LINK_FLAGS "${CVMFS_UNITTESTS_LD_FLAGS}")

if (BUILD_UNITTESTS_DEBUG)
  set_target_properties (${PROJECT_TEST_DEBUG_NAME} PROPERTIES COMPILE_FLAGS "${CVMFS_UNITTESTS_DEBUG_CFLAGS}" LINK_FLAGS "${CVMFS_UNITTESTS_DEBUG_LD_FLAGS}")
endif (BUILD_UNITTESTS_DEBUG)

#
# link the stuff (*_LIBRARIES are dynamic link libraries)
#
set (UNITTEST_LINK_LIBRARIES ${GTEST_LIBRARIES} ${GOOGLETEST_ARCHIVE} ${OPENSSL_LIBRARIES} ${CURL_LIBRARIES}
                             ${LIBCURL_ARCHIVE} ${CARES_LIBRARIES} ${CARES_ARCHIVE} ${OPENSSL_LIBRARIES}
                             ${SQLITE3_LIBRARY} ${SQLITE3_ARCHIVE} ${TBB_LIBRARIES}
                             ${ZLIB_LIBRARIES} ${ZLIB_ARCHIVE} ${RT_LIBRARY} ${UUID_LIBRARIES}
                             ${SHA3_ARCHIVE} ${PACPARSER_LIBRARIES} ${PACPARSER_ARCHIVE}
                             pthread dl)

target_link_libraries (${PROJECT_TEST_NAME} ${UNITTEST_LINK_LIBRARIES})

if (BUILD_UNITTESTS_DEBUG)
  target_link_libraries (${PROJECT_TEST_DEBUG_NAME} ${UNITTEST_LINK_LIBRARIES})
endif (BUILD_UNITTESTS_DEBUG)

#
# Install the generated unit test binary
#
if (INSTALL_UNITTESTS)
  install (
    TARGETS        ${PROJECT_TEST_NAME}
    RUNTIME
    DESTINATION    bin
  )
endif (INSTALL_UNITTESTS)

if (INSTALL_UNITTESTS_DEBUG)
  install (
    TARGETS        ${PROJECT_TEST_DEBUG_NAME}
    RUNTIME
    DESTINATION    bin
  )
endif (INSTALL_UNITTESTS_DEBUG)
