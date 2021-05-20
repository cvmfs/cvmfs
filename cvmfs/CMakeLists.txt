#
# This file is part of the CernVM File System
#

# /usr/bin/cvmfs2
set (CVMFS2_BINARY_SOURCES
  fuse_main.cc
  logging.cc
  util/exception.cc
  util/posix.cc
  util/string.cc
)

# /usr/lib/libcvmfs_fuse_stub[3]
set (CVMFS_STUB_SOURCES
  globals.cc
  hash.cc
  loader.cc
  loader_talk.cc
  logging.cc
  options.cc
  sanitizer.cc
  statistics.cc
  util/exception.cc
  util/posix.cc
  util/string.cc
)

# /usr/lib/libcvmfs_fuse[3] and /usr/lib/libcvmfs.a
set (CVMFS_CLIENT_SOURCES
  authz/authz.cc
  authz/authz_curl.cc
  authz/authz_fetch.cc
  authz/authz_session.cc
  backoff.cc
  cache.cc
  cache.pb.cc cache.pb.h
  cache_extern.cc
  cache_posix.cc
  cache_ram.cc
  cache_tiered.cc
  cache_transport.cc
  catalog.cc
  catalog_counters.cc
  catalog_mgr_client.cc
  catalog_sql.cc
  clientctx.cc
  compression.cc
  directory_entry.cc
  duplex_fuse.cc
  dns.cc
  download.cc
  fetch.cc
  file_chunk.cc
  file_watcher.cc
  globals.cc
  glue_buffer.cc
  hash.cc
  history_sql.cc
  history_sqlite.cc
  json_document.cc
  kvstore.cc
  logging.cc
  magic_xattr.cc
  malloc_arena.cc
  malloc_heap.cc
  manifest.cc
  manifest_fetch.cc
  monitor.cc
  mountpoint.cc
  options.cc
  quota.cc
  quota_posix.cc
  resolv_conf_event_handler.cc
  sanitizer.cc
  signature.cc
  sql.cc
  sqlitemem.cc
  sqlitevfs.cc
  statistics.cc
  tracer.cc
  uuid.cc
  util/algorithm.cc
  util/exception.cc
  util/posix.cc
  util/string.cc
  util_concurrency.cc
  whitelist.cc
  wpad.cc
  xattr.cc
  )

if (APPLE)
  list(APPEND CVMFS_CLIENT_SOURCES file_watcher_kqueue.cc)
else(APPLE)
  if (CVMFS_ENABLE_INOTIFY)
    list(APPEND CVMFS_CLIENT_SOURCES file_watcher_inotify.cc)
    add_definitions(-DCVMFS_ENABLE_INOTIFY)
  endif(CVMFS_ENABLE_INOTIFY)
endif (APPLE)

if (CVMFS_ENABLE_FUSE3_CACHE_READDIR)
  add_definitions(-DCVMFS_ENABLE_FUSE3_CACHE_READDIR)
endif()

# First .h then .cc is important to avoid races during the build process
set_source_files_properties(cache.pb.h cache.pb.cc
                            PROPERTIES GENERATED true)

add_custom_command(OUTPUT cache.pb.h cache.pb.cc
                   COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} --cpp_out=.
                           ${CMAKE_CURRENT_SOURCE_DIR}/cache.proto
                           -I${CMAKE_CURRENT_SOURCE_DIR}
                   DEPENDS ${PROTOBUF_PROTOC_EXECUTABLE}
                           ${CMAKE_CURRENT_SOURCE_DIR}/cache.proto
                   COMMENT "Generating protobuf sources")

add_custom_target(cache.pb.generated
                  DEPENDS cache.pb.h cache.pb.cc)

# /usr/lib/libcvmfs_fuse[3]_debug
set (CVMFS_FUSE_SOURCES
  ${CVMFS_CLIENT_SOURCES}

  auto_umount.cc
  compat.cc
  cvmfs.cc
  fuse_evict.cc
  fuse_remount.cc
  nfs_maps_leveldb.cc
  nfs_maps_sqlite.cc
  notification_client.cc
  notify/messages.cc
  notify/subscriber_supervisor.cc
  notify/subscriber_sse.cc
  quota_listener.cc
  supervisor.cc
  talk.cc
  url.cc
)

# /usr/lib/libcvmfs.a
set (LIBCVMFS_SOURCES
  ${CVMFS_CLIENT_SOURCES}

  libcvmfs.cc
  libcvmfs_int.cc
  libcvmfs_legacy.cc
  libcvmfs_options.cc
)

set (TEST_LIBCVMFS_SOURCES
  test_libcvmfs.cc
)

# /usr/lib/libcvmfs_cache.a
set (LIBCVMFS_CACHE_SOURCES
  cache.pb.cc cache.pb.h
  cache_plugin/libcvmfs_cache.cc
  cache_plugin/libcvmfs_cache_options.cc
  cache_plugin/channel.cc
  cache_transport.cc
  hash.cc
  logging.cc
  monitor.cc
  options.cc
  sanitizer.cc
  util/exception.cc
  util/posix.cc
  util/string.cc
  util_concurrency.cc
)

set (CVMFS_CACHE_NULL_SOURCES
  cache_plugin/cvmfs_cache_null.cc
)

set (CVMFS_CACHE_RAM_SOURCES
  cache_plugin/cvmfs_cache_ram.cc
  logging.cc
  malloc_heap.cc
  statistics.cc
  util_concurrency.cc
  util/exception.cc
  util/posix.cc
  util/string.cc
)

set (CVMFS_CACHE_POSIX_SOURCES
  cache_plugin/cvmfs_cache_posix.cc
  cache.cc
  cache_posix.cc
  compression.cc
  hash.cc
  logging.cc
  manifest.cc
  quota.cc
  util/exception.cc
  util/posix.cc
  util/string.cc
)

set (CVMFS_FSCK_SOURCES
  compression.cc
  cvmfs_fsck.cc
  hash.cc
  logging.cc
  statistics.cc
  util/exception.cc
  util/posix.cc
  util/string.cc
)

set (CVMFS_TALK_SOURCES
  cvmfs_talk.cc
  logging.cc
  options.cc
  sanitizer.cc
  util/exception.cc
  util/posix.cc
  util/string.cc
)

set (CVMFS_SHRINKWRAP_SOURCES
  hash.cc
  logging.cc
  monitor.cc
  shrinkwrap/fs_traversal.cc
  shrinkwrap/fs_traversal_libcvmfs.cc
  shrinkwrap/posix/data_dir_mgmt.cc
  shrinkwrap/posix/garbage_collector.cc
  shrinkwrap/posix/helpers.cc
  shrinkwrap/posix/interface.cc
  shrinkwrap/shrinkwrap.cc
  shrinkwrap/spec_tree.cc
  shrinkwrap/util.cc
  statistics.cc
  util/exception.cc
  util/posix.cc
  util/string.cc
  util_concurrency.cc
  xattr.cc
)

# TODO(jblomer) Rename to cvmfs_server
set (CVMFS_PUBLISH_SOURCES
  backoff.cc
  compression.cc
  directory_entry.cc
  dns.cc
  download.cc
  hash.cc
  logging.cc
  manifest.cc
  options.cc
  publish/cmd_abort.cc
  publish/cmd_diff.cc
  publish/cmd_enter.cc
  publish/cmd_hash.cc
  publish/cmd_help.cc
  publish/cmd_info.cc
  publish/cmd_mkfs.cc
  publish/cmd_transaction.cc
  publish/cmd_util.cc
  publish/cmd_zpipe.cc
  publish/command.cc
  publish/except.cc
  publish/main.cc
  publish/settings.cc
  sanitizer.cc
  signature.cc
  statistics.cc
  upload_spooler_definition.cc
  util/algorithm.cc
  util/exception.cc
  util/namespace.cc
  util/posix.cc
  util/string.cc
  whitelist.cc
)

set (LIBCVMFS_SERVER_SOURCES
  backoff.cc
  catalog.cc
  catalog_counters.cc
  catalog_rw.cc
  catalog_sql.cc
  catalog_mgr_ro.cc
  catalog_mgr_rw.cc
  catalog_virtual.cc
  compression.cc
  directory_entry.cc
  dns.cc
  download.cc
  gateway_util.cc
  globals.cc
  hash.cc
  history_sql.cc
  history_sqlite.cc
  ingestion/chunk_detector.cc
  ingestion/item.cc
  ingestion/item_mem.cc
  ingestion/pipeline.cc
  ingestion/task_chunk.cc
  ingestion/task_compress.cc
  ingestion/task_hash.cc
  ingestion/task_read.cc
  ingestion/task_register.cc
  ingestion/task_write.cc
  json_document.cc
  logging.cc
  manifest.cc
  manifest_fetch.cc
  malloc_arena.cc
  options.cc
  pack.cc
  publish/except.cc
  publish/repository.cc
  publish/repository_abort.cc
  publish/repository_env.cc
  publish/repository_diff.cc
  publish/repository_managed.cc
  publish/repository_session.cc
  publish/repository_tags.cc
  publish/repository_transaction.cc
  publish/repository_util.cc
  publish/settings.cc
  reflog.cc
  reflog_sql.cc
  s3fanout.cc
  sanitizer.cc
  session_context.cc
  signature.cc
  sql.cc
  sqlitemem.cc
  statistics.cc
  swissknife_assistant.cc
  swissknife_lease_curl.cc
  sync_item.cc
  sync_item_tar.cc
  sync_mediator.cc
  sync_union.cc
  sync_union_aufs.cc
  sync_union_overlayfs.cc
  sync_union_tarball.cc
  upload.cc
  upload_facility.cc
  upload_gateway.cc
  upload_local.cc
  upload_s3.cc
  upload_spooler_definition.cc
  util_concurrency.cc
  util/algorithm.cc
  util/exception.cc
  util/file_backed_buffer.cc
  util/mmap_file.cc
  util/posix.cc
  util/raii_temp_dir.cc
  util/string.cc
  whitelist.cc
  xattr.cc
)

set (CVMFS_SWISSKNIFE_SOURCES
  backoff.cc
  catalog.cc
  catalog_counters.cc
  catalog_mgr_ro.cc
  catalog_mgr_rw.cc
  catalog_sql.cc
  catalog_rw.cc
  catalog_virtual.cc
  clientctx.cc
  compression.cc
  directory_entry.cc
  dns.cc
  download.cc
  file_chunk.cc
  gateway_util.cc
  globals.cc
  hash.cc
  history_sql.cc
  history_sqlite.cc
  ingestion/chunk_detector.cc
  ingestion/item.cc
  ingestion/item_mem.cc
  ingestion/pipeline.cc
  ingestion/task_chunk.cc
  ingestion/task_compress.cc
  ingestion/task_hash.cc
  ingestion/task_read.cc
  ingestion/task_register.cc
  ingestion/task_write.cc
  json_document.cc
  letter.cc
  logging.cc
  malloc_arena.cc
  manifest.cc
  manifest_fetch.cc
  monitor.cc
  notify/cmd_pub.cc
  notify/cmd_sub.cc
  notify/messages.cc
  notify/publisher.cc
  notify/publisher_http.cc
  notify/subscriber_supervisor.cc
  notify/subscriber_sse.cc
  options.cc
  pack.cc
  path_filters/dirtab.cc
  path_filters/relaxed_path_filter.cc
  pathspec/pathspec.cc
  pathspec/pathspec_pattern.cc
  reflog.cc
  reflog_sql.cc
  repository_tag.cc
  s3fanout.cc
  sanitizer.cc
  server_tool.cc
  session_context.cc
  signature.cc
  signing_tool.cc
  sql.cc
  sqlitemem.cc
  statistics_database.cc
  statistics.cc
  supervisor.cc
  swissknife.cc
  swissknife_assistant.cc
  swissknife_capabilities.cc
  swissknife_check.cc
  swissknife_gc.cc
  swissknife_graft.cc
  swissknife_history.cc
  swissknife_info.cc
  swissknife_ingest.cc
  swissknife_lease.cc
  swissknife_lease_curl.cc
  swissknife_lease_json.cc
  swissknife_letter.cc
  swissknife_list_reflog.cc
  swissknife_lsrepo.cc
  swissknife_main.cc
  swissknife_migrate.cc
  swissknife_notify.cc
  swissknife_pull.cc
  swissknife_reflog.cc
  swissknife_filestats.cc
  swissknife_scrub.cc
  swissknife_sign.cc
  swissknife_sync.cc
  swissknife_zpipe.cc
  sync_item.cc
  sync_item_tar.cc
  sync_mediator.cc
  sync_union.cc
  sync_union_aufs.cc
  sync_union_overlayfs.cc
  sync_union_tarball.cc
  upload.cc
  upload_facility.cc
  upload_gateway.cc
  upload_local.cc
  upload_s3.cc
  upload_spooler_definition.cc
  url.cc
  util/algorithm.cc
  util/exception.cc
  util/file_backed_buffer.cc
  util/mmap_file.cc
  util/posix.cc
  util/string.cc
  util/raii_temp_dir.cc
  util_concurrency.cc
  whitelist.cc
  xattr.cc
)

set (CVMFS_SUID_HELPER_SOURCES
  cvmfs_suid_helper.cc
  cvmfs_suid_util.cc
  sanitizer.cc
)

set (CVMFS_PRELOADER_SOURCES
  backoff.cc
  catalog.cc
  catalog_sql.cc
  compression.cc
  dns.cc
  download.cc
  gateway_util.cc
  globals.cc
  hash.cc
  history_sql.cc
  history_sqlite.cc
  ingestion/chunk_detector.cc
  ingestion/item.cc
  ingestion/item_mem.cc
  ingestion/pipeline.cc
  ingestion/task_chunk.cc
  ingestion/task_compress.cc
  ingestion/task_hash.cc
  ingestion/task_read.cc
  ingestion/task_register.cc
  ingestion/task_write.cc
  json_document.cc
  logging.cc
  malloc_arena.cc
  manifest.cc
  manifest_fetch.cc
  options.cc
  pack.cc
  path_filters/dirtab.cc
  path_filters/relaxed_path_filter.cc
  pathspec/pathspec.cc
  pathspec/pathspec_pattern.cc
  preload.cc
  reflog.cc
  reflog_sql.cc
  s3fanout.cc
  sanitizer.cc
  server_tool.cc
  session_context.cc
  signature.cc
  sql.cc
  sqlitemem.cc
  statistics.cc
  swissknife.cc
  swissknife_lease_curl.cc
  swissknife_pull.cc
  upload.cc
  upload_facility.cc
  upload_gateway.cc
  upload_local.cc
  upload_s3.cc
  upload_spooler_definition.cc
  util/algorithm.cc
  util/exception.cc
  util/file_backed_buffer.cc
  util/mmap_file.cc
  util/posix.cc
  util/string.cc
  util_concurrency.cc
  uuid.cc
  whitelist.cc
  xattr.cc
)

if(NOT BUILD_RECEIVER)
  if(BUILD_RECEIVER_DEBUG)
    message( FATAL_ERROR "BUILD_RECEIVER needs to be on with BUILD_RECEIVER_DEBUG set. Exiting." )
  endif(BUILD_RECEIVER_DEBUG)
endif(NOT BUILD_RECEIVER)

if(BUILD_RECEIVER)
  set (CVMFS_RECEIVER_SOURCES
    receiver/commit_processor.cc
    receiver/lease_path_util.cc
    receiver/params.cc
    receiver/payload_processor.cc
    receiver/reactor.cc
    receiver/receiver.cc
    receiver/session_token.cc
    backoff.cc
    catalog.cc
    catalog_rw.cc
    catalog_counters.cc
    catalog_sql.cc
    catalog_mgr_ro.cc
    catalog_mgr_rw.cc
    compression.cc
    directory_entry.cc
    dns.cc
    download.cc
    encrypt.cc
    gateway_util.cc
    globals.cc
    hash.cc
    history_sql.cc
    history_sqlite.cc
    ingestion/chunk_detector.cc
    ingestion/item.cc
    ingestion/item_mem.cc
    ingestion/pipeline.cc
    ingestion/task_chunk.cc
    ingestion/task_compress.cc
    ingestion/task_hash.cc
    ingestion/task_read.cc
    ingestion/task_register.cc
    ingestion/task_write.cc
    json_document.cc
    logging.cc
    malloc_arena.cc
    manifest.cc
    manifest_fetch.cc
    monitor.cc
    options.cc
    pack.cc
    reflog.cc
    reflog_sql.cc
    repository_tag.cc
    s3fanout.cc
    sanitizer.cc
    server_tool.cc
    session_context.cc
    signature.cc
    signing_tool.cc
    statistics.cc
    statistics_database.cc
    swissknife.cc
    swissknife_history.cc
    swissknife_lease_curl.cc
    sql.cc
    sqlitemem.cc
    sync_item.cc
    upload.cc
    upload_facility.cc
    upload_gateway.cc
    upload_local.cc
    upload_s3.cc
    upload_spooler_definition.cc
    util/algorithm.cc
    util/exception.cc
    util/file_backed_buffer.cc
    util/mmap_file.cc
    util/posix.cc
    util/string.cc
    util/raii_temp_dir.cc
    util_concurrency.cc
    uuid.cc
    whitelist.cc
    xattr.cc
    )

  add_executable(cvmfs_receiver ${CVMFS_RECEIVER_SOURCES})
  target_link_libraries(cvmfs_receiver ${CURL_LIBRARIES} ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                        ${SHA3_LIBRARIES} ${SQLITE3_LIBRARY} ${VJSON_LIBRARIES}
                        ${OPENSSL_LIBRARIES} ${UUID_LIBRARIES} ${ZLIB_LIBRARIES}
                        ${RT_LIBRARY} ${LibArchive_LIBRARY} pthread dl)

  set (CVMFS_RECEIVER_CFLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -DCVMFS_LIBCVMFS -DCVMFS_RAISE_EXCEPTIONS -fexceptions")
  set_target_properties (cvmfs_receiver PROPERTIES COMPILE_FLAGS "${CVMFS_RECEIVER_CFLAGS}")

  if(BUILD_RECEIVER_DEBUG)
    add_executable(cvmfs_receiver_debug ${CVMFS_RECEIVER_SOURCES})
    target_link_libraries(cvmfs_receiver_debug
                          ${CURL_LIBRARIES} ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                          ${SHA3_LIBRARIES} ${SQLITE3_LIBRARY} ${VJSON_LIBRARIES}
                          ${OPENSSL_LIBRARIES} ${UUID_LIBRARIES} ${ZLIB_LIBRARIES}
                          ${RT_LIBRARY} ${LibArchive_LIBRARY} pthread dl)

    set (CVMFS_RECEIVER_DEBUG_CFLAGS "${CVMFS_RECEIVER_CFLAGS} -DDEBUGMSG -g -O0")
    set_target_properties (cvmfs_receiver_debug PROPERTIES COMPILE_FLAGS "${CVMFS_RECEIVER_DEBUG_CFLAGS}")
  endif(BUILD_RECEIVER_DEBUG)
endif(BUILD_RECEIVER)

#
# configure some compiler flags for proper build
#

set (CVMFS2_BINARY_CFLAGS "-DCVMFS_NAMESPACE_GUARD=stub -DCVMFS_FUSE_MODULE")
set (CVMFS_FUSE_CFLAGS "-DCVMFS_CLIENT -DCVMFS_FUSE_MODULE -DCVMFS_USE_LIBFUSE=2 -D_FILE_OFFSET_BITS=64 -fexceptions")
set (CVMFS_FUSE3_CFLAGS "-DCVMFS_CLIENT -DCVMFS_FUSE_MODULE -DCVMFS_USE_LIBFUSE=3 -D_FILE_OFFSET_BITS=64 -fexceptions")
set (CVMFS_FUSE_DEBUG_CFLAGS "${CVMFS_FUSE_CFLAGS} -O0 -DDEBUGMSG")
set (CVMFS_FUSE3_DEBUG_CFLAGS "${CVMFS_FUSE3_CFLAGS} -O0 -DDEBUGMSG")
set (CVMFS_STUB_CFLAGS "-DCVMFS_CLIENT -DCVMFS_FUSE_MODULE -DCVMFS_NAMESPACE_GUARD=loader -DCVMFS_USE_LIBFUSE=2 -D_FILE_OFFSET_BITS=64 -fexceptions")
set (CVMFS_STUB3_CFLAGS "-DCVMFS_CLIENT -DCVMFS_FUSE_MODULE -DCVMFS_NAMESPACE_GUARD=loader -DCVMFS_USE_LIBFUSE=3 -D_FILE_OFFSET_BITS=64 -fexceptions")
set (CVMFS_FSCK_CFLAGS "-DCVMFS_CLIENT")
set (CVMFS_TALK_CFLAGS "-DCVMFS_CLIENT")
set (LIBCVMFS_CFLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_CLIENT -DCVMFS_LIBCVMFS -fexceptions -fPIC")
set (LIBCVMFS_CACHE_CFLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_CLIENT -DCVMFS_LIBCVMFS -fexceptions")
set (CVMFS_SHRINKWRAP_CFLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_CLIENT -DCVMFS_LIBCVMFS -fexceptions")
set (CVMFS_SWISSKNIFE_CFLAGS "-DCVMFS_SWISSKNIFE")

#
# set some include and linking stuff
#
set (CVMFS2_BINARY_LD_FLAGS "-ldl")
set (CVMFS_FUSE_LD_FLAGS "-ldl -lm")
set (CVMFS_STUB_LD_FLAGS "-ldl -lm")
include_directories (cvmfs ${CMAKE_CURRENT_BINARY_DIR} ${INCLUDE_DIRECTORIES})

#
# do the job!
#
# for every target you can define a <targetname>_CFLAGS and <targetname>_LD_FLAGS
# variable to pass some target specific build options.
#

if (BUILD_CVMFS)
  add_executable (cvmfs2 ${CVMFS2_BINARY_SOURCES})
  add_library (cvmfs_fuse_stub SHARED ${CVMFS_STUB_SOURCES})
  add_library (cvmfs_fuse_debug SHARED ${CVMFS_FUSE_SOURCES})
  add_library (cvmfs_fuse SHARED ${CVMFS_FUSE_SOURCES})
  add_dependencies (cvmfs_fuse_debug cache.pb.generated)
  add_dependencies (cvmfs_fuse cache.pb.generated)
  if (FUSE3_FOUND)
    add_library (cvmfs_fuse3_stub SHARED ${CVMFS_STUB_SOURCES})
    add_library (cvmfs_fuse3_debug SHARED ${CVMFS_FUSE_SOURCES})
    add_library (cvmfs_fuse3 SHARED ${CVMFS_FUSE_SOURCES})
    add_dependencies (cvmfs_fuse3_debug cache.pb.generated)
    add_dependencies (cvmfs_fuse3 cache.pb.generated)
  endif()

  add_executable (cvmfs_fsck ${CVMFS_FSCK_SOURCES})
  add_executable (cvmfs_talk ${CVMFS_TALK_SOURCES})

  set_target_properties (cvmfs2 PROPERTIES COMPILE_FLAGS "${CVMFS2_BINARY_CFLAGS}"
                                           LINK_FLAGS "${CVMFS2_BINARY_LD_FLAGS}")
  set_target_properties (cvmfs_fuse_stub PROPERTIES
                                         COMPILE_FLAGS "${CVMFS_STUB_CFLAGS}"
                                         LINK_FLAGS "${CVMFS_STUB_LD_FLAGS}")
  set_target_properties (cvmfs_fuse_debug PROPERTIES
                                          COMPILE_FLAGS "${CVMFS_FUSE_DEBUG_CFLAGS}"
                                          LINK_FLAGS "${CVMFS_FUSE_DEBUG_LD_FLAGS}")
  set_target_properties (cvmfs_fuse PROPERTIES COMPILE_FLAGS "${CVMFS_FUSE_CFLAGS}"
                                               LINK_FLAGS "${CVMFS_FUSE_LD_FLAGS}")
  if (FUSE3_FOUND)
    set_target_properties (cvmfs_fuse3_stub PROPERTIES
                                            COMPILE_FLAGS "${CVMFS_STUB3_CFLAGS}"
                                            LINK_FLAGS "${CVMFS_STUB_LD_FLAGS}")
    set_target_properties (cvmfs_fuse3_debug PROPERTIES
                                             COMPILE_FLAGS "${CVMFS_FUSE3_DEBUG_CFLAGS}"
                                             LINK_FLAGS "${CVMFS_FUSE_DEBUG_LD_FLAGS}")
    set_target_properties (cvmfs_fuse3 PROPERTIES COMPILE_FLAGS "${CVMFS_FUSE3_CFLAGS}"
                                                  LINK_FLAGS "${CVMFS_FUSE_LD_FLAGS}")
  endif()
  set_target_properties (cvmfs_fsck PROPERTIES COMPILE_FLAGS "${CVMFS_FSCK_CFLAGS}"
                                               LINK_FLAGS "${CVMFS_FSCK_LD_FLAGS}")
  set_target_properties (cvmfs_talk PROPERTIES COMPILE_FLAGS "${CVMFS_TALK_CFLAGS}"
                                               LINK_FLAGS "${CVMFS_TALK_LD_FLAGS}")

  set_target_properties (cvmfs_fuse_stub PROPERTIES VERSION ${CernVM-FS_VERSION_STRING})
  set_target_properties (cvmfs_fuse PROPERTIES VERSION ${CernVM-FS_VERSION_STRING})
  set_target_properties (cvmfs_fuse_debug PROPERTIES VERSION ${CernVM-FS_VERSION_STRING})
  if (FUSE3_FOUND)
    set_target_properties (cvmfs_fuse3_stub PROPERTIES VERSION ${CernVM-FS_VERSION_STRING})
    set_target_properties (cvmfs_fuse3 PROPERTIES VERSION ${CernVM-FS_VERSION_STRING})
    set_target_properties (cvmfs_fuse3_debug PROPERTIES VERSION ${CernVM-FS_VERSION_STRING})
  endif()

  set (CVMFS_FUSE_LINK_LIBRARIES "")
  list(APPEND CVMFS_FUSE_LINK_LIBRARIES ${CURL_LIBRARIES}
                                        ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                                        ${OPENSSL_LIBRARIES}
                                        ${PACPARSER_LIBRARIES}
                                        ${SQLITE3_LIBRARY}
                                        ${ZLIB_LIBRARIES}
                                        ${SPARSEHASH_LIBRARIES}
                                        ${LEVELDB_LIBRARIES}
                                        ${SHA3_LIBRARIES}
                                        ${SHA2_LIBRARIES}
                                        ${PROTOBUF_LITE_LIBRARY}
                                        ${VJSON_LIBRARIES}
                                        ${RT_LIBRARY}
                                        ${UUID_LIBRARIES})
  list (APPEND CVMFS_FUSE_LINK_LIBRARIES pthread dl)

  target_link_libraries(cvmfs2 pthread dl)
  target_link_libraries (cvmfs_fuse_stub   ${OPENSSL_LIBRARIES} ${SHA3_LIBRARIES}
                                           ${FUSE_LIBRARIES} pthread dl)
  target_link_libraries (cvmfs_fuse_debug  ${FUSE_LIBRARIES} ${CVMFS_FUSE_LINK_LIBRARIES})
  target_link_libraries (cvmfs_fuse        ${FUSE_LIBRARIES} ${CVMFS_FUSE_LINK_LIBRARIES})
  if (FUSE3_FOUND)
    target_link_libraries (cvmfs_fuse3_stub  ${OPENSSL_LIBRARIES} ${SHA3_LIBRARIES}
                                             ${FUSE3_LIBRARIES} pthread dl)
    target_link_libraries (cvmfs_fuse3_debug ${FUSE3_LIBRARIES} ${CVMFS_FUSE_LINK_LIBRARIES})
    target_link_libraries (cvmfs_fuse3       ${FUSE3_LIBRARIES} ${CVMFS_FUSE_LINK_LIBRARIES})
  endif()
  target_link_libraries (cvmfs_fsck        ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES}
                                           ${ZLIB_LIBRARIES} ${SHA3_LIBRARIES}
                                           ${RT_LIBRARY} pthread)
  target_link_libraries (cvmfs_talk        ${CVMFS_TALK_LIBS}
                                           ${RT_LIBRARY} pthread)


  set (CVMFS_ALLOW_HELPER_SOURCES
    authz/helper_allow.cc
    authz/helper_log.cc
    authz/helper_util.cc
  )
  add_executable (cvmfs_allow_helper ${CVMFS_ALLOW_HELPER_SOURCES})
  target_link_libraries (cvmfs_allow_helper ${VJSON_LIBRARIES})

  set (CVMFS_DENY_HELPER_SOURCES
    authz/helper_deny.cc
    authz/helper_log.cc
    authz/helper_util.cc
  )
  add_executable (cvmfs_deny_helper ${CVMFS_DENY_HELPER_SOURCES})
  target_link_libraries (cvmfs_deny_helper ${VJSON_LIBRARIES})
endif (BUILD_CVMFS)


if (BUILD_LIBCVMFS_CACHE)
  add_library (cvmfs_cache_only STATIC ${LIBCVMFS_CACHE_SOURCES})
  add_dependencies (cvmfs_cache_only cache.pb.generated)
  set (LIBCVMFS_CACHE_LIBS ${SHA3_LIBRARIES} ${PROTOBUF_LITE_LIBRARY}
    ${LIBCVMFS_CACHE_LIBS} ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache_only.a)
  if (MACOSX)
    list(APPEND LIBCVMFS_CACHE_LIBS ${OPENSSL_LIBRARIES})
  endif (MACOSX)

  set_target_properties (cvmfs_cache_only PROPERTIES COMPILE_FLAGS
                         "${LIBCVMFS_CACHE_CFLAGS}")
  target_link_libraries(cvmfs_cache_only ${LIBCVMFS_CACHE_LIBS})

  # Note: it would be a good idea to try to unify the next block for macOS and Linux.
  #       It may be possible to use libtool on both platforms, including the ability
  #       to hide private symbols in the library - which is currently done by the
  #       `combine_libs` script, on Linux only.
  if (MACOSX)
    add_custom_command(
        OUTPUT libcvmfs_cache.a
        COMMAND libtool -static -o libcvmfs_cache.a ${LIBCVMFS_CACHE_LIBS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        DEPENDS ${LIBCVMFS_CACHE_LIBS}
      )
  else (MACOSX)
    set (LIBCVMFS_CACHE_PUBLIC_SYMS "-public" "${CMAKE_CURRENT_SOURCE_DIR}/cache_plugin/libcvmfs_cache_public_syms.txt")
    add_custom_command(
        OUTPUT libcvmfs_cache.a
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/combine_libs ${LIBCVMFS_CACHE_PUBLIC_SYMS} libcvmfs_cache.a ${LIBCVMFS_CACHE_LIBS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        DEPENDS ${LIBCVMFS_CACHE_LIBS}
      )
  endif (MACOSX)

  # dummy target to cause merged libcvmfs_cache to be produced
  add_custom_target (libcvmfs_cache ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a)
  add_dependencies (libcvmfs_cache cvmfs_cache_only)


  add_executable(cvmfs_cache_null ${CVMFS_CACHE_NULL_SOURCES})
  target_link_libraries(cvmfs_cache_null
                        ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
                        ${OPENSSL_LIBRARIES} ${RT_LIBRARY} pthread)
  add_dependencies(cvmfs_cache_null libcvmfs_cache)

  add_executable(cvmfs_cache_ram ${CVMFS_CACHE_RAM_SOURCES})
  set (CVMFS_CACHE_RAM_CFLAGS "${CMAKE_CXX_FLAGS} -DDEBUGMSG")
  set_target_properties (cvmfs_cache_ram PROPERTIES COMPILE_FLAGS
                         "${CVMFS_CACHE_RAM_CFLAGS}")
  target_link_libraries(cvmfs_cache_ram
                        ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
                        ${OPENSSL_LIBRARIES} ${RT_LIBRARY} pthread)
  add_dependencies(cvmfs_cache_ram libcvmfs_cache)

  add_executable(cvmfs_cache_posix ${CVMFS_CACHE_POSIX_SOURCES})
  set (CVMFS_CACHE_POSIX_CFLAGS "${CMAKE_CXX_FLAGS} -DDEBUGMSG")
  set_target_properties (cvmfs_cache_posix PROPERTIES COMPILE_FLAGS
                         "${CVMFS_CACHE_POSIX_CFLAGS}")
  target_link_libraries(cvmfs_cache_posix
                        ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
                        ${OPENSSL_LIBRARIES} ${SHA3_LIBRARIES}
                        ${ZLIB_LIBRARIES} pthread)
  add_dependencies(cvmfs_cache_posix libcvmfs_cache)
endif (BUILD_LIBCVMFS_CACHE)


if (BUILD_LIBCVMFS)
  add_library (cvmfs_only STATIC ${LIBCVMFS_SOURCES})
  add_dependencies (cvmfs_only cache.pb.generated)

  set (LIBCVMFS_LIBS ${CARES_LIBRARIES} ${CURL_LIBRARIES}
                     ${PACPARSER_LIBRARIES}
                     ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES} ${SPARSEHASH_LIBRARIES}
                     ${SHA3_LIBRARIES} ${VJSON_LIBRARIES} ${PROTOBUF_LITE_LIBRARY}
                     ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_only.a)

  set_target_properties (cvmfs_only PROPERTIES COMPILE_FLAGS "${LIBCVMFS_CFLAGS}")
  target_link_libraries(cvmfs_only ${LIBCVMFS_LIBS})

  # Note: it would be a good idea to try to unify the next block for macOS and Linux.
  #       It may be possible to use libtool on both platforms, including the ability
  #       to hide private symbols in the library - which is currently done by the
  #       `combine_libs` script, on Linux only.
  if (MACOSX)
    add_custom_command(
        OUTPUT libcvmfs.a
        COMMAND libtool -static -o libcvmfs.a ${LIBCVMFS_LIBS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        DEPENDS ${LIBCVMFS_LIBS}
    )
  else (MACOSX)
    set (LIBCVMFS_PUBLIC_SYMS "-public" "${CMAKE_CURRENT_SOURCE_DIR}/libcvmfs_public_syms.txt")
    add_custom_command(
        OUTPUT libcvmfs.a
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/combine_libs ${LIBCVMFS_PUBLIC_SYMS} libcvmfs.a ${LIBCVMFS_LIBS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        DEPENDS ${LIBCVMFS_LIBS}
    )
  endif (MACOSX)

  # dummy target to cause merged libcvmfs to be produced
  add_custom_target (libcvmfs ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a)
  add_dependencies (libcvmfs cvmfs_only)

  add_executable( test_libcvmfs ${TEST_LIBCVMFS_SOURCES} )
  add_dependencies(test_libcvmfs libcvmfs)

  list(APPEND TEST_LIBCVMFS_LIBS ${SQLITE3_LIBRARY} ${CURL_LIBRARIES}
                                 ${CARES_LIBRARIES} ${CARES_LDFLAGS} ${PACPARSER_LIBRARIES} ${ZLIB_LIBRARIES}
                                 ${OPENSSL_LIBRARIES} ${RT_LIBRARY} ${UUID_LIBRARIES} pthread dl)

  target_link_libraries(test_libcvmfs ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a ${TEST_LIBCVMFS_LIBS})

  if (BUILD_CVMFS_SHRINKWRAP)
    add_executable (cvmfs_shrinkwrap ${CVMFS_SHRINKWRAP_SOURCES})
    add_dependencies (cvmfs_shrinkwrap cache.pb.generated)
    add_dependencies (cvmfs_shrinkwrap libcvmfs)

    set (CVMFS_SHRINKWRAP_LIBS
                          ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a
                          ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                          ${PACPARSER_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES}
                          ${SHA3_LIBRARIES} ${VJSON_LIBRARIES}
                          ${PROTOBUF_LITE_LIBRARY}
                          ${RT_LIBRARY} ${UUID_LIBRARIES} pthread dl
                          )

    set_target_properties (cvmfs_shrinkwrap PROPERTIES COMPILE_FLAGS "${CVMFS_SHRINKWRAP_CFLAGS}" LINK_FLAGS "${CVMFS_SHRINKWRAP_FLAGS}")
    target_link_libraries(cvmfs_shrinkwrap ${CVMFS_SHRINKWRAP_LIBS})
  endif(BUILD_CVMFS_SHRINKWRAP)
endif(BUILD_LIBCVMFS)

if (BUILD_SHRINKWRAP)
  add_executable (cvmfs_shrinkwrap ${CVMFS_SHRINKWRAP_SOURCES})
  add_dependencies (cvmfs_shrinkwrap libcvmfs)

  set (CVMFS_SHRINKWRAP_LIBS
                        ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a
                        ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                        ${PACPARSER_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES}
                        ${SHA3_LIBRARIES} ${VJSON_LIBRARIES}
                        ${PROTOBUF_LITE_LIBRARY}
                        ${RT_LIBRARY} ${UUID_LIBRARIES} pthread dl )

  set_target_properties (cvmfs_shrinkwrap PROPERTIES COMPILE_FLAGS "${CVMFS_SHRINKWRAP_CFLAGS}" LINK_FLAGS "${CVMFS_SHRINKWRAP_FLAGS}")
  target_link_libraries(cvmfs_shrinkwrap ${CVMFS_SHRINKWRAP_LIBS})
endif(BUILD_SHRINKWRAP)


if(NOT BUILD_SERVER)
  if(BUILD_SERVER_DEBUG)
    message( FATAL_ERROR "BUILD_SERVER needs to be on with BUILD_SERVER_DEBUG set. Exiting." )
  endif(BUILD_SERVER_DEBUG)
endif(NOT BUILD_SERVER)

if (BUILD_SERVER)
  set (CVMFS_PUBLISH_CFLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -DCVMFS_RAISE_EXCEPTIONS -fexceptions")
  set (LIBCVMFS_SERVER_CFLAGS "${CMAKE_CXX_FLAGS} -D_FILE_OFFSET_BITS=64 -DCVMFS_RAISE_EXCEPTIONS -fexceptions")

  # create targets with corresponding sources and static link libraries
  add_executable (cvmfs_swissknife ${CVMFS_SWISSKNIFE_SOURCES})
  add_executable (cvmfs_suid_helper ${CVMFS_SUID_HELPER_SOURCES})
  add_library (cvmfs_server SHARED ${LIBCVMFS_SERVER_SOURCES})
  add_executable (cvmfs_publish ${CVMFS_PUBLISH_SOURCES})

  set_target_properties (cvmfs_swissknife PROPERTIES COMPILE_FLAGS "${CVMFS_SWISSKNIFE_CFLAGS}" LINK_FLAGS "${CVMFS_SWISSKNIFE_LD_FLAGS}")
  set_target_properties (cvmfs_server PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "${LIBCVMFS_SERVER_CFLAGS}")
  set_target_properties (cvmfs_publish PROPERTIES COMPILE_FLAGS "${CVMFS_PUBLISH_CFLAGS}" LINK_FLAGS "${CVMFS_PUBLISH_LD_FLAGS}")

  # link the stuff (*_LIBRARIES are dynamic link libraries)
  set (LIBCVMFS_SERVER_LINK_LIBRARIES "")
  list(APPEND LIBCVMFS_SERVER_LINK_LIBRARIES ${CURL_LIBRARIES}
                                             ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                                             ${OPENSSL_LIBRARIES}
                                             ${SQLITE3_LIBRARY}
                                             ${ZLIB_LIBRARIES}
                                             ${SHA3_LIBRARIES}
                                             ${VJSON_LIBRARIES}
                                             ${CAP_LIBRARIES}
                                             ${LibArchive_LIBRARY}
                                             ${RT_LIBRARY}
                                             pthread dl)
  target_link_libraries (cvmfs_swissknife
                         ${SQLITE3_LIBRARY} ${CURL_LIBRARIES}
                         ${CARES_LIBRARIES} ${CARES_LDFLAGS} ${ZLIB_LIBRARIES}
                         ${OPENSSL_LIBRARIES} ${RT_LIBRARY} ${VJSON_LIBRARIES}
                         ${SHA3_LIBRARIES} ${CAP_LIBRARIES} ${LibArchive_LIBRARY}
                         pthread dl)
  target_link_libraries (cvmfs_server ${LIBCVMFS_SERVER_LIBS} ${LIBCVMFS_SERVER_LINK_LIBRARIES})
  target_link_libraries (cvmfs_publish cvmfs_server)
  if (BUILD_SERVER_DEBUG)
    add_executable (cvmfs_swissknife_debug ${CVMFS_SWISSKNIFE_SOURCES})
    set_target_properties (cvmfs_swissknife_debug PROPERTIES COMPILE_FLAGS "${CVMFS_SWISSKNIFE_CFLAGS} -DDEBUGMSG -g -O0" LINK_FLAGS "${CVMFS_SWISSKNIFE_DEBUG_LD_FLAGS}")
    target_link_libraries (cvmfs_swissknife_debug
                           ${SQLITE3_LIBRARY}  ${CURL_LIBRARIES}
                           ${CARES_LIBRARIES} ${CARES_LDFLAGS} ${ZLIB_LIBRARIES}
                           ${OPENSSL_LIBRARIES} ${RT_LIBRARY} ${VJSON_LIBRARIES}
                           ${SHA3_LIBRARIES} ${CAP_LIBRARIES} ${LibArchive_LIBRARY}
                           pthread dl)

    add_library (cvmfs_server_debug SHARED ${LIBCVMFS_SERVER_SOURCES})
    set_target_properties (cvmfs_server_debug PROPERTIES
      VERSION ${CernVM-FS_VERSION_STRING}
      COMPILE_FLAGS "${LIBCVMFS_SERVER_CFLAGS} -DDEBUGMSG -g -O0")
    target_link_libraries (cvmfs_server_debug ${LIBCVMFS_SERVER_LIBS} ${LIBCVMFS_SERVER_LINK_LIBRARIES})

    add_executable (cvmfs_publish_debug ${CVMFS_PUBLISH_SOURCES})
    set_target_properties (cvmfs_publish_debug
      PROPERTIES COMPILE_FLAGS "${CVMFS_PUBLISH_CFLAGS} -DDEBUGMSG -g -O0"
      LINK_FLAGS "${CVMFS_PUBLISH_LD_FLAGS}")
    target_link_libraries (cvmfs_publish_debug cvmfs_server_debug)
  endif (BUILD_SERVER_DEBUG)
endif (BUILD_SERVER)

if (BUILD_PRELOADER)
  add_executable (cvmfs_preload_bin ${CVMFS_PRELOADER_SOURCES})

  target_link_libraries(cvmfs_preload_bin ${SQLITE3_LIBRARY} ${CURL_LIBRARIES}
                        ${CARES_LIBRARIES} ${CARES_LDFLAGS} ${ZLIB_LIBRARIES}
                        ${OPENSSL_LIBRARIES} ${CARES_LIBRARIS} ${RT_LIBRARY}
                        ${UUID_LIBRARIES} ${VJSON_LIBRARIES} ${SHA3_LIBRARIES}
                        pthread dl)
endif (BUILD_PRELOADER)

#
# Generate a bash self-extracting script for the cvmfs_preload target
#
if (BUILD_PRELOADER)
  add_custom_command (
    TARGET cvmfs_preload_bin POST_BUILD
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/cvmfs_preload_builder.sh ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Generating the cvmfs_preload self-extracting script"
  )
endif (BUILD_PRELOADER)


#
# Generate the "cvmfs_server" script using "make_cvmfs_server.sh"
#
add_custom_target (
  cvmfs_server_script
  ALL
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} && ./make_cvmfs_server.sh ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_server
  COMMENT "Generating the cvmfs_server script"
  )

#
# installation
#

if (BUILD_CVMFS)
  install (
    TARGETS      cvmfs2 cvmfs_fsck cvmfs_talk
    RUNTIME
    DESTINATION    bin
  )

  install (
    TARGETS      cvmfs_allow_helper cvmfs_deny_helper
    RUNTIME
    DESTINATION  ${CVMFS_LIBEXEC_DIR}/authz
  )

  install (
    TARGETS      cvmfs_fuse_stub cvmfs_fuse cvmfs_fuse_debug
    LIBRARY
    DESTINATION    ${CMAKE_INSTALL_LIBDIR}
  )
  if (FUSE3_FOUND)
    install (
      TARGETS      cvmfs_fuse3_stub cvmfs_fuse3 cvmfs_fuse3_debug
      LIBRARY
      DESTINATION    ${CMAKE_INSTALL_LIBDIR}
    )
  endif ()

  install (
    FILES      cvmfs_config
    DESTINATION    bin
    PERMISSIONS    OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )


  if(INSTALL_BASH_COMPLETION)
    install (
        FILES         bash_completion/cvmfs.bash_completion
        RENAME        cvmfs
        DESTINATION   ${SYSCONF_INSTALL_DIR}/bash_completion.d
        PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )
  endif (INSTALL_BASH_COMPLETION)

endif (BUILD_CVMFS)

if (BUILD_LIBCVMFS)
  install (
    FILES ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a
    DESTINATION    ${CMAKE_INSTALL_LIBDIR}
  )
  install (
    FILES                   libcvmfs.h
    DESTINATION             include
  )
endif (BUILD_LIBCVMFS)


if (BUILD_SHRINKWRAP)
  install (
    TARGETS      cvmfs_shrinkwrap
    RUNTIME
    DESTINATION    bin
  )

  install (
    FILES          shrinkwrap/scripts/spec_builder.py
    DESTINATION    ${CVMFS_LIBEXEC_DIR}/shrinkwrap
    PERMISSIONS    OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )
endif (BUILD_SHRINKWRAP)

if (BUILD_LIBCVMFS_CACHE)
  install (
    FILES          ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
    DESTINATION    ${CMAKE_INSTALL_LIBDIR}
  )
  install (
    FILES                   cache_plugin/libcvmfs_cache.h
    DESTINATION             include
  )
  install (
    TARGETS      cvmfs_cache_ram
    RUNTIME
    DESTINATION  ${CVMFS_LIBEXEC_DIR}/cache
  )
  install (
    TARGETS     cvmfs_cache_posix
    RUNTIME
    DESTINATION  ${CVMFS_LIBEXEC_DIR}/cache
  )
endif (BUILD_LIBCVMFS_CACHE)

if (BUILD_RECEIVER)
  install (
    TARGETS cvmfs_receiver
    RUNTIME
    DESTINATION bin
    )

  if (BUILD_RECEIVER_DEBUG)
    install (
      TARGETS     cvmfs_receiver_debug
      RUNTIME
      DESTINATION   bin
    )
  endif (BUILD_RECEIVER_DEBUG)

endif(BUILD_RECEIVER)

if (BUILD_SERVER)
  install (
    TARGETS     cvmfs_swissknife cvmfs_publish
    RUNTIME
    DESTINATION    bin
  )

  install (
    TARGETS     cvmfs_server
    LIBRARY
    DESTINATION    ${CMAKE_INSTALL_LIBDIR}
  )

  install (
    TARGETS     cvmfs_suid_helper
    RUNTIME
    DESTINATION    bin
    PERMISSIONS    OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE SETUID
  )

  if (BUILD_SERVER_DEBUG)
    install (
      TARGETS     cvmfs_swissknife_debug
      RUNTIME
      DESTINATION   bin
    )

    install (
      TARGETS     cvmfs_publish_debug
      RUNTIME
      DESTINATION   bin
    )

    install (
      TARGETS     cvmfs_server_debug
      LIBRARY
      DESTINATION    ${CMAKE_INSTALL_LIBDIR}
    )
  endif (BUILD_SERVER_DEBUG)

  install (
    FILES      ${${PROJECT_NAME}_BINARY_DIR}/cvmfs/cvmfs_server
    DESTINATION    bin
    PERMISSIONS    OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  install(
    FILES      cvmfs_server_hooks.sh.demo
    DESTINATION    "${SYSCONF_INSTALL_DIR}/cvmfs"
    PERMISSIONS    OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )

  install (
    FILES      cvmfs_rsync
    DESTINATION    bin
    PERMISSIONS    OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  install(
    FILES      webapi/cvmfs-api.wsgi
    DESTINATION    "/var/www/wsgi-scripts/cvmfs-server"
    PERMISSIONS    OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  install(
    FILES      server/generate_stats_plots.C
    DESTINATION    "/usr/share/cvmfs-server"
    PERMISSIONS    OWNER_READ GROUP_READ WORLD_READ
  )

  install(
    FILES      server/upload_stats_plots.sh
    DESTINATION    "/usr/share/cvmfs-server"
    PERMISSIONS    OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  install(
    FILES      server/stats_index.html.tpl
    DESTINATION    "/usr/share/cvmfs-server"
    PERMISSIONS    OWNER_READ GROUP_READ WORLD_READ
  )

  install(
    CODE  "FILE(MAKE_DIRECTORY \$ENV{DESTDIR}/var/lib/cvmfs-server)"
  )

  if (BUILD_GEOAPI)
    install(
      CODE  "FILE(MAKE_DIRECTORY \$ENV{DESTDIR}/var/lib/cvmfs-server/geo)"
    )

    install(
      FILES          ${EXTERNALS_INSTALL_LOCATION}/ipaddress.py
      DESTINATION    "/usr/share/cvmfs-server/webapi"
      PERMISSIONS    OWNER_READ GROUP_READ WORLD_READ
    )

    install(
      DIRECTORY      ${EXTERNALS_INSTALL_LOCATION}/maxminddb
      DESTINATION    "/usr/share/cvmfs-server/webapi"
      FILE_PERMISSIONS       OWNER_READ GROUP_READ WORLD_READ
      DIRECTORY_PERMISSIONS  OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
    )

    install(
      FILES      webapi/cvmfs_api.py webapi/cvmfs_geo.py
      DESTINATION    "/usr/share/cvmfs-server/webapi"
      PERMISSIONS    OWNER_READ GROUP_READ WORLD_READ
    )
  endif (BUILD_GEOAPI)

  install(
    FILES          doc/README-spooler
    DESTINATION    "/var/spool/cvmfs"
    PERMISSIONS    OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    RENAME         README
  )

endif (BUILD_SERVER)

if (BUILD_PRELOADER)
  install(
    FILES      ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_preload
    DESTINATION    bin
    PERMISSIONS    OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )
endif (BUILD_PRELOADER)
