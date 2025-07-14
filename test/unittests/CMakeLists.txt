#
# This file is part of the CernVM File System
#

if (BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)

set (CVMFS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/cvmfs")
set (CVMFS_UNITTEST_COMMON_CFLAGS "-DCVMFS_LIBCVMFS -D_FILE_OFFSET_BITS=64 -fexceptions")

#
# Protobuf generated sources
#
set_source_files_properties (cache.pb.h cache.pb.cc PROPERTIES GENERATED true)
add_custom_command (OUTPUT cache.pb.h cache.pb.cc
                    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} --cpp_out=.
                            ${CVMFS_SOURCE_DIR}/cache.proto
                            -I${CVMFS_SOURCE_DIR}
                    DEPENDS ${PROTOBUF_PROTOC_EXECUTABLE}
                            ${CVMFS_SOURCE_DIR}/cache.proto
                    COMMENT "Generating protobuf sources")
add_custom_target (cache.pb.generated-unittests DEPENDS cache.pb.h cache.pb.cc)

include_directories (${CMAKE_CURRENT_BINARY_DIR}
                     ${CMAKE_CURRENT_SOURCE_DIR}/../common
                     ${INCLUDE_DIRECTORIES})



#
# cvmfs_test_cache
#
if (BUILD_LIBCVMFS_CACHE)
  add_executable (cvmfs_test_cache
                  cache.pb.cc cache.pb.h
                  cache_plugin/main.cc
                  cache_plugin/t_cache_plugin.cc

                  ${CVMFS_SOURCE_DIR}/cache.cc
                  ${CVMFS_SOURCE_DIR}/cache_extern.cc
                  ${CVMFS_SOURCE_DIR}/cache_transport.cc
                  ${CVMFS_SOURCE_DIR}/compression/compression.cc
                  ${CVMFS_SOURCE_DIR}/manifest.cc
                  ${CVMFS_SOURCE_DIR}/quota.cc
  )
  add_dependencies (cvmfs_test_cache cache.pb.generated-unittests)
  set_target_properties (cvmfs_test_cache PROPERTIES
                         COMPILE_FLAGS "${CVMFS_UNITTEST_COMMON_CFLAGS}"
  )
  target_link_libraries (cvmfs_test_cache
                         cvmfs_crypto
                         cvmfs_util
                         GTest::gtest
                         ${ZLIB_LIBRARIES}
                         ${RT_LIBRARY}
                         ${PROTOBUF_LITE_LIBRARY}
                         pthread
  )
  if (INSTALL_UNITTESTS)
    install (
      TARGETS      cvmfs_test_cache
      RUNTIME
      DESTINATION  bin
    )
  endif ()
endif (BUILD_LIBCVMFS_CACHE)



#
# cvmfs_test_shrinkwrap
#
if (BUILD_SHRINKWRAP)
  add_executable (cvmfs_test_shrinkwrap
                  shrinkwrap/main.cc
                  ../common/env.cc

                  shrinkwrap/t_fs_traversal_interface.cc
                  shrinkwrap/t_fs_posix.cc
                  shrinkwrap/t_spec_tree.cc
                  shrinkwrap/testutil_shrinkwrap.cc

                  ${CVMFS_SOURCE_DIR}/shrinkwrap/fs_traversal.cc
                  ${CVMFS_SOURCE_DIR}/shrinkwrap/posix/data_dir_mgmt.cc
                  ${CVMFS_SOURCE_DIR}/shrinkwrap/posix/garbage_collector.cc
                  ${CVMFS_SOURCE_DIR}/shrinkwrap/posix/helpers.cc
                  ${CVMFS_SOURCE_DIR}/shrinkwrap/posix/interface.cc
                  ${CVMFS_SOURCE_DIR}/shrinkwrap/fs_traversal_libcvmfs.cc
                  ${CVMFS_SOURCE_DIR}/shrinkwrap/util.cc
                  ${CVMFS_SOURCE_DIR}/shrinkwrap/spec_tree.cc

                  ${CVMFS_SOURCE_DIR}/statistics.cc
                  ${CVMFS_SOURCE_DIR}/xattr.cc
  )
  set_target_properties (cvmfs_test_shrinkwrap PROPERTIES
                         COMPILE_FLAGS "${CVMFS_UNITTEST_COMMON_CFLAGS}"
  )
  target_link_libraries (cvmfs_test_shrinkwrap
                         cvmfs_client
                         cvmfs_crypto
                         cvmfs_util
                         GTest::gtest
                         pthread
                         dl
  )
  if (INSTALL_UNITTESTS)
    install (
      TARGETS       cvmfs_test_shrinkwrap
      RUNTIME
      DESTINATION   bin
    )
  endif ()
endif (BUILD_SHRINKWRAP)



#
# cvmfs_test_publish
#
if (BUILD_SERVER)
  set (CVMFS_UNITTEST_SERVER_SOURCES
       c_http_server.cc
       publish/c_repository.cc
       publish/main.cc
       publish/t_command.cc
       publish/t_env.cc
       publish/t_diff.cc
       publish/t_session.cc
       publish/t_repository.cc
       publish/t_settings.cc
       publish/t_transaction.cc
       publish/t_util.cc

       ../common/env.cc

       ${CVMFS_SOURCE_DIR}/publish/cmd_help.cc
       ${CVMFS_SOURCE_DIR}/publish/cmd_mkfs.cc
       ${CVMFS_SOURCE_DIR}/publish/cmd_util.cc
       ${CVMFS_SOURCE_DIR}/publish/cmd_zpipe.cc
       ${CVMFS_SOURCE_DIR}/publish/command.cc
       ${CVMFS_SOURCE_DIR}/publish/except.cc
       ${CVMFS_SOURCE_DIR}/publish/repository.cc
       ${CVMFS_SOURCE_DIR}/publish/repository_abort.cc
       ${CVMFS_SOURCE_DIR}/publish/repository_env.cc
       ${CVMFS_SOURCE_DIR}/publish/repository_diff.cc
       ${CVMFS_SOURCE_DIR}/publish/repository_managed.cc
       ${CVMFS_SOURCE_DIR}/publish/repository_session.cc
       ${CVMFS_SOURCE_DIR}/publish/repository_tags.cc
       ${CVMFS_SOURCE_DIR}/publish/repository_transaction.cc
       ${CVMFS_SOURCE_DIR}/publish/repository_util.cc
       ${CVMFS_SOURCE_DIR}/publish/settings.cc

       ${CVMFS_SOURCE_DIR}/backoff.cc
       ${CVMFS_SOURCE_DIR}/catalog.cc
       ${CVMFS_SOURCE_DIR}/catalog_counters.cc
       ${CVMFS_SOURCE_DIR}/catalog_downloader.cc
       ${CVMFS_SOURCE_DIR}/catalog_sql.cc
       ${CVMFS_SOURCE_DIR}/catalog_rw.cc
       ${CVMFS_SOURCE_DIR}/catalog_mgr_ro.cc
       ${CVMFS_SOURCE_DIR}/catalog_mgr_rw.cc
       ${CVMFS_SOURCE_DIR}/catalog_virtual.cc
       ${CVMFS_SOURCE_DIR}/compression/compression.cc
       ${CVMFS_SOURCE_DIR}/directory_entry.cc
       ${CVMFS_SOURCE_DIR}/gateway_util.cc
       ${CVMFS_SOURCE_DIR}/globals.cc
       ${CVMFS_SOURCE_DIR}/history_sql.cc
       ${CVMFS_SOURCE_DIR}/history_sqlite.cc
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
       ${CVMFS_SOURCE_DIR}/json_document.cc
       ${CVMFS_SOURCE_DIR}/malloc_arena.cc
       ${CVMFS_SOURCE_DIR}/manifest.cc
       ${CVMFS_SOURCE_DIR}/manifest_fetch.cc
       ${CVMFS_SOURCE_DIR}/network/dns.cc
       ${CVMFS_SOURCE_DIR}/network/download.cc
       ${CVMFS_SOURCE_DIR}/network/jobinfo.cc
       ${CVMFS_SOURCE_DIR}/network/s3fanout.cc
       ${CVMFS_SOURCE_DIR}/network/sink_file.cc
       ${CVMFS_SOURCE_DIR}/network/sink_mem.cc
       ${CVMFS_SOURCE_DIR}/network/sink_path.cc
       ${CVMFS_SOURCE_DIR}/options.cc
       ${CVMFS_SOURCE_DIR}/pack.cc
       ${CVMFS_SOURCE_DIR}/reflog.cc
       ${CVMFS_SOURCE_DIR}/reflog_sql.cc
       ${CVMFS_SOURCE_DIR}/sanitizer.cc
       ${CVMFS_SOURCE_DIR}/session_context.cc
       ${CVMFS_SOURCE_DIR}/shortstring.cc
       ${CVMFS_SOURCE_DIR}/sql.cc
       ${CVMFS_SOURCE_DIR}/sqlitemem.cc
       ${CVMFS_SOURCE_DIR}/ssl.cc
       ${CVMFS_SOURCE_DIR}/statistics.cc
       ${CVMFS_SOURCE_DIR}/swissknife_assistant.cc
       ${CVMFS_SOURCE_DIR}/swissknife_lease_curl.cc
       ${CVMFS_SOURCE_DIR}/sync_item.cc
       ${CVMFS_SOURCE_DIR}/sync_item_dummy.cc
       ${CVMFS_SOURCE_DIR}/sync_item_tar.cc
       ${CVMFS_SOURCE_DIR}/sync_mediator.cc
       ${CVMFS_SOURCE_DIR}/sync_union.cc
       ${CVMFS_SOURCE_DIR}/sync_union_aufs.cc
       ${CVMFS_SOURCE_DIR}/sync_union_overlayfs.cc
       ${CVMFS_SOURCE_DIR}/sync_union_tarball.cc
       ${CVMFS_SOURCE_DIR}/upload.cc
       ${CVMFS_SOURCE_DIR}/upload_facility.cc
       ${CVMFS_SOURCE_DIR}/upload_gateway.cc
       ${CVMFS_SOURCE_DIR}/upload_local.cc
       ${CVMFS_SOURCE_DIR}/upload_s3.cc
       ${CVMFS_SOURCE_DIR}/upload_spooler_definition.cc
       ${CVMFS_SOURCE_DIR}/whitelist.cc
       ${CVMFS_SOURCE_DIR}/xattr.cc
  )
  set (CVMFS_UNITTEST_SERVER_LINK_LIBRARIES "")
  list (APPEND CVMFS_UNITTEST_SERVER_LINK_LIBRARIES
        GTest::gtest
        ${OPENSSL_LIBRARIES}
        ${CURL_LIBRARIES}
        ${CARES_LIBRARIES} ${CARES_LDFLAGS}
        ${OPENSSL_LIBRARIES}
        ${SQLITE3_LIBRARY}
        ${ZLIB_LIBRARIES}
        ${VJSON_LIBRARIES}
        ${CAP_LIBRARIES}
        ${LibArchive_LIBRARY}
        ${RT_LIBRARY}
        pthread
        dl
  )
  add_executable (cvmfs_test_publish ${CVMFS_UNITTEST_SERVER_SOURCES})

  set_target_properties (cvmfs_test_publish PROPERTIES
                         COMPILE_FLAGS "${CVMFS_UNITTEST_COMMON_CFLAGS}"
  )

  target_link_libraries (cvmfs_test_publish
                         cvmfs_crypto
                         cvmfs_util
                         ${CVMFS_UNITTEST_SERVER_LINK_LIBRARIES})

  if (INSTALL_UNITTESTS)
    install (
      TARGETS      cvmfs_test_publish
      RUNTIME
      DESTINATION  bin
    )
  endif ()

  if (BUILD_UNITTESTS_DEBUG)
  add_executable (cvmfs_test_publish_debug ${CVMFS_UNITTEST_SERVER_SOURCES})
  set_target_properties (cvmfs_test_publish_debug PROPERTIES
                         COMPILE_FLAGS "${CVMFS_UNITTEST_COMMON_CFLAGS} -O0 -DDEBUGMSG"
  )
  target_link_libraries (cvmfs_test_publish_debug
                         cvmfs_crypto_debug
                         cvmfs_util_debug
                         ${CVMFS_UNITTEST_SERVER_LINK_LIBRARIES})

  if (INSTALL_UNITTESTS)
    install (
      TARGETS      cvmfs_test_publish_debug
      RUNTIME
      DESTINATION  bin
    )
  endif ()
endif ()
endif (BUILD_SERVER)




#
# unit test files
#
set(CVMFS_UNITTEST_SOURCES
  # test steering
  main.cc

  # test utility functions
  ../common/env.cc
  ../common/testutil.cc
  ../common/catalog_test_tools.cc

  t_acl.cc
  t_assert_or_log.cc
  t_atomic.cc
  t_authz_fetch.cc
  t_authz_session.cc
  t_backoff.cc
  t_base64.cc
  t_bigqueue.cc
  t_bigvector.cc
  t_blocking_counter.cc
  t_cache.cc
  t_cache_extern.cc
  t_cache_ram.cc
  t_cache_stream.cc
  t_cache_tiered.cc
  t_callbacks.cc
  t_catalog.cc
  t_catalog_counters.cc
  t_catalog_merge_tool.cc
  t_catalog_mgr.cc
  t_catalog_mgr_client.cc
  t_catalog_mgr_rw.cc
  t_catalog_sql.cc
  t_catalog_traversal.cc
  t_catalog_virtual.cc
  t_chunk_detectors.cc
  t_clientctx.cc
  t_compression.cc
  t_compressor.cc
  t_dirtab.cc
  t_dns.cc
  t_download.cc
  t_encrypt.cc
  t_fd_table.cc
  t_fence.cc
  t_fetch.cc
  t_file_backed_buffer.cc
  t_file_chunk.cc
  t_file_guard.cc
  t_file_sandbox.cc
  t_fs_traversal.cc
  t_fuse_evict.cc
  t_garbage_collector.cc
  t_gateway_key_parser.cc
  t_glue_buffer.cc
  t_hash_filters.cc
  t_header_lists.cc
  t_history.cc
  t_ingestion.cc
  t_ingestion_stress.cc
  t_ingestion_tube.cc
  t_json.cc
  t_kvstore.cc
  t_lease_path_util.cc
  t_libcvmfs.cc
  t_logging.cc
  t_lru.cc
  t_magic_xattr.cc
  t_malloc_arena.cc
  t_malloc_heap.cc
  t_manifest.cc
  t_mountpoint.cc
  t_namespace.cc
  t_notify_messages.cc
  t_object_fetcher.cc
  t_options.cc
  t_pack.cc
  t_panic.cc
  t_pathspec.cc
  t_payload_processor.cc
  t_pipe.cc
  t_platforms.cc
  t_polymorphic_construction.cc
  t_prng.cc
  t_quota.cc
  t_reactor.cc
  t_reflog.cc
  t_relaxed_path_filter.cc
  t_s3fanout.cc
  t_resolv_conf_event_handler.cc
  t_ring_buffer.cc
  t_sanitizer.cc
  t_session_context.cc
  t_session_token.cc
  t_shash.cc
  t_smallhash.cc
  t_smalloc.cc
  t_shared_ptr.cc
  t_signature.cc
  t_sink.cc
  t_sqlite_database.cc
  t_sqlitemem.cc
  t_statistics.cc
  t_statistics_sql.cc
  t_suid_util.cc
  t_supervisor.cc
  t_swissknife_lease.cc
  t_sync_union_tarball.cc
  t_synchronizing_counter.cc
  t_telemetry_aggregator.cc
  t_raii_temp_dir.cc
  t_test_utils.cc
  t_tracer.cc
  t_trim_string.cc
  t_tube.cc
  t_uid_map.cc
  t_unique_ptr.cc
  t_upload_facility.cc
  t_uploaders.cc
  t_gateway_uploader.cc
  t_url.cc
  t_util.cc
  t_util_concurrency.cc
  t_uuid.cc
  t_whitelist.cc
  t_wpad.cc
  t_xattr.cc

  ${CVMFS_SOURCE_DIR}/acl.cc
  ${CVMFS_SOURCE_DIR}/authz/authz.cc
  ${CVMFS_SOURCE_DIR}/authz/authz_curl.cc
  ${CVMFS_SOURCE_DIR}/authz/authz_fetch.cc
  ${CVMFS_SOURCE_DIR}/authz/authz_session.cc
  ${CVMFS_SOURCE_DIR}/backoff.cc
  ${CVMFS_SOURCE_DIR}/cache.cc
  ${CVMFS_SOURCE_DIR}/cache_extern.cc
  ${CVMFS_SOURCE_DIR}/cache_posix.cc
  ${CVMFS_SOURCE_DIR}/cache_plugin/channel.cc
  ${CVMFS_SOURCE_DIR}/cache_ram.cc
  ${CVMFS_SOURCE_DIR}/cache_stream.cc
  ${CVMFS_SOURCE_DIR}/cache_tiered.cc
  ${CVMFS_SOURCE_DIR}/cache_transport.cc
  ${CVMFS_SOURCE_DIR}/catalog.cc
  ${CVMFS_SOURCE_DIR}/catalog_counters.cc
  ${CVMFS_SOURCE_DIR}/catalog_downloader.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_client.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_ro.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_rw.cc
  ${CVMFS_SOURCE_DIR}/catalog_sql.cc
  ${CVMFS_SOURCE_DIR}/catalog_rw.cc
  ${CVMFS_SOURCE_DIR}/catalog_virtual.cc
  ${CVMFS_SOURCE_DIR}/clientctx.cc
  ${CVMFS_SOURCE_DIR}/compression/compression.cc
  ${CVMFS_SOURCE_DIR}/cvmfs_suid_util.cc
  ${CVMFS_SOURCE_DIR}/directory_entry.cc
  ${CVMFS_SOURCE_DIR}/duplex_fuse.cc
  ${CVMFS_SOURCE_DIR}/fd_refcount_mgr.cc
  ${CVMFS_SOURCE_DIR}/fetch.cc
  ${CVMFS_SOURCE_DIR}/file_chunk.cc
  ${CVMFS_SOURCE_DIR}/file_watcher.cc
  ${CVMFS_SOURCE_DIR}/fuse_evict.cc
  ${CVMFS_SOURCE_DIR}/gateway_util.cc
  ${CVMFS_SOURCE_DIR}/globals.cc
  ${CVMFS_SOURCE_DIR}/glue_buffer.cc
  ${CVMFS_SOURCE_DIR}/history_sql.cc
  ${CVMFS_SOURCE_DIR}/history_sqlite.cc
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
  ${CVMFS_SOURCE_DIR}/json_document.cc
  ${CVMFS_SOURCE_DIR}/kvstore.cc
  ${CVMFS_SOURCE_DIR}/libcvmfs.cc
  ${CVMFS_SOURCE_DIR}/libcvmfs_int.cc
  ${CVMFS_SOURCE_DIR}/libcvmfs_legacy.cc
  ${CVMFS_SOURCE_DIR}/libcvmfs_options.cc
  ${CVMFS_SOURCE_DIR}/magic_xattr.cc
  ${CVMFS_SOURCE_DIR}/malloc_arena.cc
  ${CVMFS_SOURCE_DIR}/malloc_heap.cc
  ${CVMFS_SOURCE_DIR}/manifest.cc
  ${CVMFS_SOURCE_DIR}/manifest_fetch.cc
  ${CVMFS_SOURCE_DIR}/monitor.cc
  ${CVMFS_SOURCE_DIR}/mountpoint.cc
  ${CVMFS_SOURCE_DIR}/network/dns.cc
  ${CVMFS_SOURCE_DIR}/network/download.cc
  ${CVMFS_SOURCE_DIR}/network/jobinfo.cc
  ${CVMFS_SOURCE_DIR}/network/s3fanout.cc
  ${CVMFS_SOURCE_DIR}/network/sink_file.cc
  ${CVMFS_SOURCE_DIR}/network/sink_mem.cc
  ${CVMFS_SOURCE_DIR}/network/sink_path.cc
  ${CVMFS_SOURCE_DIR}/notify/messages.cc
  ${CVMFS_SOURCE_DIR}/options.cc
  ${CVMFS_SOURCE_DIR}/pack.cc
  ${CVMFS_SOURCE_DIR}/path_filters/dirtab.cc
  ${CVMFS_SOURCE_DIR}/path_filters/relaxed_path_filter.cc
  ${CVMFS_SOURCE_DIR}/pathspec/pathspec.cc
  ${CVMFS_SOURCE_DIR}/pathspec/pathspec_pattern.cc
  ${CVMFS_SOURCE_DIR}/quota.cc
  ${CVMFS_SOURCE_DIR}/quota_posix.cc
  ${CVMFS_SOURCE_DIR}/receiver/commit_processor.cc
  ${CVMFS_SOURCE_DIR}/receiver/params.cc
  ${CVMFS_SOURCE_DIR}/receiver/payload_processor.cc
  ${CVMFS_SOURCE_DIR}/receiver/reactor.cc
  ${CVMFS_SOURCE_DIR}/receiver/session_token.cc
  ${CVMFS_SOURCE_DIR}/reflog.cc
  ${CVMFS_SOURCE_DIR}/reflog_sql.cc
  ${CVMFS_SOURCE_DIR}/repository_tag.cc
  ${CVMFS_SOURCE_DIR}/resolv_conf_event_handler.cc
  ${CVMFS_SOURCE_DIR}/ring_buffer.cc
  ${CVMFS_SOURCE_DIR}/sanitizer.cc
  ${CVMFS_SOURCE_DIR}/server_tool.cc
  ${CVMFS_SOURCE_DIR}/session_context.cc
  ${CVMFS_SOURCE_DIR}/signing_tool.cc
  ${CVMFS_SOURCE_DIR}/shortstring.cc
  ${CVMFS_SOURCE_DIR}/sql.cc
  ${CVMFS_SOURCE_DIR}/sqlitemem.cc
  ${CVMFS_SOURCE_DIR}/sqlitevfs.cc
  ${CVMFS_SOURCE_DIR}/ssl.cc
  ${CVMFS_SOURCE_DIR}/statistics.cc
  ${CVMFS_SOURCE_DIR}/statistics_database.cc
  ${CVMFS_SOURCE_DIR}/supervisor.cc
  ${CVMFS_SOURCE_DIR}/swissknife.cc
  ${CVMFS_SOURCE_DIR}/swissknife_assistant.cc
  ${CVMFS_SOURCE_DIR}/swissknife_history.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_json.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_curl.cc
  ${CVMFS_SOURCE_DIR}/sync_item.cc
  ${CVMFS_SOURCE_DIR}/sync_item_dummy.cc
  ${CVMFS_SOURCE_DIR}/sync_item_tar.cc
  ${CVMFS_SOURCE_DIR}/sync_mediator.cc
  ${CVMFS_SOURCE_DIR}/sync_union.cc
  ${CVMFS_SOURCE_DIR}/sync_union_tarball.cc
  ${CVMFS_SOURCE_DIR}/telemetry_aggregator.cc
  ${CVMFS_SOURCE_DIR}/telemetry_aggregator_influx.cc
  ${CVMFS_SOURCE_DIR}/tracer.cc
  ${CVMFS_SOURCE_DIR}/upload.cc
  ${CVMFS_SOURCE_DIR}/upload_facility.cc
  ${CVMFS_SOURCE_DIR}/upload_local.cc
  ${CVMFS_SOURCE_DIR}/upload_gateway.cc
  ${CVMFS_SOURCE_DIR}/upload_s3.cc
  ${CVMFS_SOURCE_DIR}/upload_spooler_definition.cc
  ${CVMFS_SOURCE_DIR}/url.cc
  ${CVMFS_SOURCE_DIR}/whitelist.cc
  ${CVMFS_SOURCE_DIR}/wpad.cc
  ${CVMFS_SOURCE_DIR}/xattr.cc

  cache.pb.cc cache.pb.h
  c_http_server.cc
)

# The FileWatcher unit test only makes sense on macOS or on Linux versions which support inotify (inotify)
if (MACOSX)
  list (APPEND CVMFS_UNITTEST_SOURCES ${CVMFS_SOURCE_DIR}/file_watcher_kqueue.cc t_file_watcher.cc)
else ()
  if (CVMFS_ENABLE_INOTIFY)
    list (APPEND CVMFS_UNITTEST_SOURCES ${CVMFS_SOURCE_DIR}/file_watcher_inotify.cc t_file_watcher.cc)
    add_definitions (-DCVMFS_ENABLE_INOTIFY)
  endif ()
endif ()

set (CVMFS_UNITTESTS_LINK_LIBRARIES "")
list (APPEND CVMFS_UNITTESTS_LINK_LIBRARIES
      GTest::gtest
      GTest::gmock
      ${OPENSSL_LIBRARIES}
      ${CURL_LIBRARIES}
      ${CARES_LIBRARIES} ${CARES_LDFLAGS}
      ${OPENSSL_LIBRARIES}
      ${SQLITE3_LIBRARY}
      ${ZLIB_LIBRARIES}
      ${UUID_LIBRARIES}
      ${PACPARSER_LIBRARIES}
      ${VJSON_LIBRARIES}
      ${PROTOBUF_LITE_LIBRARY}
      ${LibArchive_LIBRARY}
      ${RT_LIBRARY}
      pthread
      dl
)

set(CVMFS_UNITTEST_MOCKFUSE_SOURCES
  # test steering
  main_mockfuse.cc

  # test utility functions
  #../common/env.cc
  ../common/testutil.cc
  ../common/catalog_test_tools.cc

  t_cvmfs.cc
  ${CVMFS_SOURCE_DIR}/acl.cc
  ${CVMFS_SOURCE_DIR}/authz/authz.cc
  ${CVMFS_SOURCE_DIR}/authz/authz_curl.cc
  ${CVMFS_SOURCE_DIR}/authz/authz_fetch.cc
  ${CVMFS_SOURCE_DIR}/authz/authz_session.cc
  ${CVMFS_SOURCE_DIR}/auto_umount.cc
  ${CVMFS_SOURCE_DIR}/backoff.cc
  ${CVMFS_SOURCE_DIR}/cache.cc
  ${CVMFS_SOURCE_DIR}/cache_extern.cc
  ${CVMFS_SOURCE_DIR}/cache_posix.cc
  ${CVMFS_SOURCE_DIR}/cache_plugin/channel.cc
  ${CVMFS_SOURCE_DIR}/cache_ram.cc
  ${CVMFS_SOURCE_DIR}/cache_stream.cc
  ${CVMFS_SOURCE_DIR}/cache_tiered.cc
  ${CVMFS_SOURCE_DIR}/cache_transport.cc
  ${CVMFS_SOURCE_DIR}/catalog.cc
  ${CVMFS_SOURCE_DIR}/catalog_counters.cc
  ${CVMFS_SOURCE_DIR}/catalog_downloader.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_client.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_ro.cc
  ${CVMFS_SOURCE_DIR}/catalog_mgr_rw.cc
  ${CVMFS_SOURCE_DIR}/catalog_sql.cc
  ${CVMFS_SOURCE_DIR}/catalog_rw.cc
  ${CVMFS_SOURCE_DIR}/catalog_virtual.cc
  ${CVMFS_SOURCE_DIR}/clientctx.cc
  ${CVMFS_SOURCE_DIR}/compat.cc
  ${CVMFS_SOURCE_DIR}/compression/compression.cc
  ${CVMFS_SOURCE_DIR}/cvmfs_suid_util.cc
  ${CVMFS_SOURCE_DIR}/directory_entry.cc
  ${CVMFS_SOURCE_DIR}/duplex_fuse.cc
  ${CVMFS_SOURCE_DIR}/fd_refcount_mgr.cc
  ${CVMFS_SOURCE_DIR}/fetch.cc
  ${CVMFS_SOURCE_DIR}/file_chunk.cc
  ${CVMFS_SOURCE_DIR}/file_watcher.cc
  ${CVMFS_SOURCE_DIR}/fuse_evict.cc
  ${CVMFS_SOURCE_DIR}/fuse_remount.cc
  ${CVMFS_SOURCE_DIR}/gateway_util.cc
  ${CVMFS_SOURCE_DIR}/globals.cc
  ${CVMFS_SOURCE_DIR}/glue_buffer.cc
  ${CVMFS_SOURCE_DIR}/history_sql.cc
  ${CVMFS_SOURCE_DIR}/history_sqlite.cc
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
  ${CVMFS_SOURCE_DIR}/json_document.cc
  ${CVMFS_SOURCE_DIR}/kvstore.cc
  #  ${CVMFS_SOURCE_DIR}/libcvmfs.cc
  #  ${CVMFS_SOURCE_DIR}/libcvmfs_int.cc
  #  ${CVMFS_SOURCE_DIR}/libcvmfs_legacy.cc
  #  ${CVMFS_SOURCE_DIR}/libcvmfs_options.cc
  ${CVMFS_SOURCE_DIR}/magic_xattr.cc
  ${CVMFS_SOURCE_DIR}/malloc_arena.cc
  ${CVMFS_SOURCE_DIR}/malloc_heap.cc
  ${CVMFS_SOURCE_DIR}/manifest.cc
  ${CVMFS_SOURCE_DIR}/manifest_fetch.cc
  ${CVMFS_SOURCE_DIR}/monitor.cc
  ${CVMFS_SOURCE_DIR}/mountpoint.cc
  ${CVMFS_SOURCE_DIR}/network/dns.cc
  ${CVMFS_SOURCE_DIR}/network/download.cc
  ${CVMFS_SOURCE_DIR}/network/jobinfo.cc
  ${CVMFS_SOURCE_DIR}/network/s3fanout.cc
  ${CVMFS_SOURCE_DIR}/network/sink_file.cc
  ${CVMFS_SOURCE_DIR}/network/sink_mem.cc
  ${CVMFS_SOURCE_DIR}/network/sink_path.cc
  ${CVMFS_SOURCE_DIR}/notification_client.cc
  ${CVMFS_SOURCE_DIR}/notify/messages.cc
  ${CVMFS_SOURCE_DIR}/notify/subscriber_supervisor.cc
  ${CVMFS_SOURCE_DIR}/notify/subscriber_sse.cc
  ${CVMFS_SOURCE_DIR}/options.cc
  ${CVMFS_SOURCE_DIR}/pack.cc
  ${CVMFS_SOURCE_DIR}/path_filters/dirtab.cc
  ${CVMFS_SOURCE_DIR}/path_filters/relaxed_path_filter.cc
  ${CVMFS_SOURCE_DIR}/pathspec/pathspec.cc
  ${CVMFS_SOURCE_DIR}/pathspec/pathspec_pattern.cc
  ${CVMFS_SOURCE_DIR}/quota.cc
  ${CVMFS_SOURCE_DIR}/quota_posix.cc
  ${CVMFS_SOURCE_DIR}/quota_listener.cc
  ${CVMFS_SOURCE_DIR}/receiver/commit_processor.cc
  ${CVMFS_SOURCE_DIR}/receiver/params.cc
  ${CVMFS_SOURCE_DIR}/receiver/payload_processor.cc
  ${CVMFS_SOURCE_DIR}/receiver/reactor.cc
  ${CVMFS_SOURCE_DIR}/receiver/session_token.cc
  ${CVMFS_SOURCE_DIR}/reflog.cc
  ${CVMFS_SOURCE_DIR}/reflog_sql.cc
  ${CVMFS_SOURCE_DIR}/repository_tag.cc
  ${CVMFS_SOURCE_DIR}/resolv_conf_event_handler.cc
  ${CVMFS_SOURCE_DIR}/ring_buffer.cc
  ${CVMFS_SOURCE_DIR}/sanitizer.cc
  ${CVMFS_SOURCE_DIR}/server_tool.cc
  ${CVMFS_SOURCE_DIR}/session_context.cc
  ${CVMFS_SOURCE_DIR}/signing_tool.cc
  ${CVMFS_SOURCE_DIR}/shortstring.cc
  ${CVMFS_SOURCE_DIR}/sql.cc
  ${CVMFS_SOURCE_DIR}/sqlitemem.cc
  ${CVMFS_SOURCE_DIR}/sqlitevfs.cc
  ${CVMFS_SOURCE_DIR}/ssl.cc
  ${CVMFS_SOURCE_DIR}/statistics.cc
  ${CVMFS_SOURCE_DIR}/statistics_database.cc
  ${CVMFS_SOURCE_DIR}/supervisor.cc
  ${CVMFS_SOURCE_DIR}/swissknife.cc
  ${CVMFS_SOURCE_DIR}/swissknife_assistant.cc
  ${CVMFS_SOURCE_DIR}/swissknife_history.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_json.cc
  ${CVMFS_SOURCE_DIR}/swissknife_lease_curl.cc
  ${CVMFS_SOURCE_DIR}/sync_item.cc
  ${CVMFS_SOURCE_DIR}/sync_item_dummy.cc
  ${CVMFS_SOURCE_DIR}/sync_item_tar.cc
  ${CVMFS_SOURCE_DIR}/sync_mediator.cc
  ${CVMFS_SOURCE_DIR}/sync_union.cc
  ${CVMFS_SOURCE_DIR}/sync_union_tarball.cc
  ${CVMFS_SOURCE_DIR}/talk.cc
  ${CVMFS_SOURCE_DIR}/telemetry_aggregator.cc
  ${CVMFS_SOURCE_DIR}/telemetry_aggregator_influx.cc
  ${CVMFS_SOURCE_DIR}/tracer.cc
  ${CVMFS_SOURCE_DIR}/upload.cc
  ${CVMFS_SOURCE_DIR}/upload_facility.cc
  ${CVMFS_SOURCE_DIR}/upload_local.cc
  ${CVMFS_SOURCE_DIR}/upload_gateway.cc
  ${CVMFS_SOURCE_DIR}/upload_s3.cc
  ${CVMFS_SOURCE_DIR}/upload_spooler_definition.cc
  ${CVMFS_SOURCE_DIR}/url.cc
  ${CVMFS_SOURCE_DIR}/whitelist.cc
  ${CVMFS_SOURCE_DIR}/wpad.cc
  ${CVMFS_SOURCE_DIR}/xattr.cc

  cache.pb.cc cache.pb.h
)

# Add file watcher implementation for mockfuse tests
if (MACOSX)
  list (APPEND CVMFS_UNITTEST_MOCKFUSE_SOURCES ${CVMFS_SOURCE_DIR}/file_watcher_kqueue.cc)
else ()
  if (CVMFS_ENABLE_INOTIFY)
    list (APPEND CVMFS_UNITTEST_MOCKFUSE_SOURCES ${CVMFS_SOURCE_DIR}/file_watcher_inotify.cc)
  endif ()
endif ()

add_executable (cvmfs_unittests_mockfuse ${CVMFS_UNITTEST_MOCKFUSE_SOURCES})
add_dependencies (cvmfs_unittests_mockfuse cache.pb.generated-unittests)
set_target_properties (cvmfs_unittests_mockfuse PROPERTIES
	COMPILE_FLAGS "${CVMFS_UNITTEST_COMMON_CFLAGS}  -g"
)
target_link_libraries (cvmfs_unittests_mockfuse
                       cvmfs_crypto
                       cvmfs_util
		       ${FUSE3_LIBRARIES}
                       ${CVMFS_UNITTESTS_LINK_LIBRARIES})

add_executable (cvmfs_unittests ${CVMFS_UNITTEST_SOURCES})
add_dependencies (cvmfs_unittests cache.pb.generated-unittests)
set_target_properties (cvmfs_unittests PROPERTIES
	COMPILE_FLAGS "${CVMFS_UNITTEST_COMMON_CFLAGS}"
)
target_link_libraries (cvmfs_unittests
                       cvmfs_crypto
                       cvmfs_util
                       ${CVMFS_UNITTESTS_LINK_LIBRARIES})

if (INSTALL_UNITTESTS)
  install (
    TARGETS      cvmfs_unittests
    RUNTIME
    DESTINATION  bin
  )
endif ()

if (BUILD_UNITTESTS_DEBUG)
  add_executable (cvmfs_unittests_debug ${CVMFS_UNITTEST_SOURCES})
  add_dependencies (cvmfs_unittests_debug cache.pb.generated-unittests)
  set_target_properties (cvmfs_unittests_debug PROPERTIES
                         COMPILE_FLAGS "${CVMFS_UNITTEST_COMMON_CFLAGS} -O0 -DDEBUGMSG"
  )
  target_link_libraries (cvmfs_unittests_debug
                         cvmfs_crypto_debug
                         cvmfs_util_debug
                         ${CVMFS_UNITTESTS_LINK_LIBRARIES})

  if (INSTALL_UNITTESTS)
    install (
      TARGETS      cvmfs_unittests_debug
      RUNTIME
      DESTINATION  bin
    )
  endif ()
endif ()



#
# For `make check`
#
add_test (NAME cvmfs_unittests COMMAND cvmfs_unittests --gtest_filter=-*Slow)
add_dependencies (check cvmfs_unittests)

endif (BUILD_UNITTESTS OR BUILD_UNITTESTS_DEBUG)
