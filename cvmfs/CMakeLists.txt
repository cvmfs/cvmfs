#
# This file is part of the CernVM File System
#


#
# Common libraries
#
include (util/CMakeLists.txt)
include (crypto/CMakeLists.txt)




if (BUILD_CVMFS OR BUILD_LIBCVMFS OR BUILD_LIBCVMFS_CACHE)
  #
  # Protobuf generated sources
  #
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
  include_directories (${CMAKE_CURRENT_BINARY_DIR} ${INCLUDE_DIRECTORIES})
endif ()  # Protobuf generated sources




if (BUILD_CVMFS OR BUILD_LIBCVMFS)
  #
  # /usr/lib/libcvmfs_fuse[3] and /usr/lib/libcvmfs_client
  #
  set (CVMFS_COMMON_CLIENT_SOURCES
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
       cache_stream.cc
       cache_tiered.cc
       cache_transport.cc
       catalog.cc
       catalog_counters.cc
       catalog_mgr_client.cc
       catalog_sql.cc
       clientctx.cc
       compression/compression.cc
       directory_entry.cc
       duplex_fuse.cc
       fd_refcount_mgr.cc
       fetch.cc
       file_chunk.cc
       file_watcher.cc
       globals.cc
       glue_buffer.cc
       history_sql.cc
       history_sqlite.cc
       json_document.cc
       kvstore.cc
       magic_xattr.cc
       malloc_arena.cc
       malloc_heap.cc
       manifest.cc
       manifest_fetch.cc
       monitor.cc
       mountpoint.cc
       network/dns.cc
       network/download.cc
       network/jobinfo.cc
       network/sink_file.cc
       network/sink_mem.cc
       network/sink_path.cc
       options.cc
       quota.cc
       quota_posix.cc
       resolv_conf_event_handler.cc
       ring_buffer.cc
       sanitizer.cc
       sql.cc
       sqlitemem.cc
       sqlitevfs.cc
       ssl.cc
       statistics.cc
       telemetry_aggregator.cc
       telemetry_aggregator_influx.cc
       tracer.cc
       whitelist.cc
       wpad.cc
       xattr.cc
  ) # CVMFS_COMMON_CLIENT_SOURCES

  if (MACOSX)
    list(APPEND CVMFS_COMMON_CLIENT_SOURCES file_watcher_kqueue.cc)
  else ()
    if (CVMFS_ENABLE_INOTIFY)
      list (APPEND CVMFS_COMMON_CLIENT_SOURCES file_watcher_inotify.cc)
      add_definitions (-DCVMFS_ENABLE_INOTIFY)
    endif ()
  endif ()
endif (BUILD_CVMFS OR BUILD_LIBCVMFS)




if (BUILD_CVMFS)
  #
  # Trivial authz helper
  #
  add_executable (cvmfs_allow_helper
                  authz/helper_allow.cc
                  authz/helper_log.cc
                  authz/helper_util.cc
  )
  target_link_libraries (cvmfs_allow_helper ${VJSON_LIBRARIES})
  add_executable (cvmfs_deny_helper
                  authz/helper_deny.cc
                  authz/helper_log.cc
                  authz/helper_util.cc
  )
  target_link_libraries (cvmfs_deny_helper ${VJSON_LIBRARIES})


  #
  # /usr/bin/cvmfs_fsck
  #
  add_executable (cvmfs_fsck
                  compression/compression.cc
                  cvmfs_fsck.cc
                  statistics.cc
  )
  target_link_libraries (cvmfs_fsck
                         cvmfs_crypto
                         cvmfs_util
                         ${ZLIB_LIBRARIES}
                         ${RT_LIBRARY}
                         pthread
  )

  #
  # /usr/bin/cvmfs_talk`
  #
  add_executable (cvmfs_talk
                  cvmfs_talk.cc
                  options.cc
                  sanitizer.cc
  )
  target_link_libraries (cvmfs_talk cvmfs_util ${RT_LIBRARY} pthread)

  #
  # /usr/bin/cvmfs2
  # Note: we must _not_ depend on cvmfs-util. The utility functions are
  # compiled in the stub namespace so that their symbols don't conflict
  # with libcvmfs-fuse.
  #
  add_executable (cvmfs2
                  fuse_main.cc
                  util/exception.cc
                  util/logging.cc
                  util/posix.cc
                  util/string.cc
  )
  set_target_properties (cvmfs2 PROPERTIES
    COMPILE_FLAGS "-DCVMFS_NAMESPACE_GUARD=stub -DCVMFS_FUSE_MODULE"
    LINK_FLAGS    "-ldl"
  )
  target_link_libraries(cvmfs2 pthread dl)

  #
  # /usr/lib/libcvmfs_fuse_stub[3]
  # Note: we must _not_ depend on cvmfs-util. The utility functions are compiled
  # in the loader namespace so that their symbols don't conflict with
  # libcvmfs-fuse.
  #
  set (CVMFS_STUB_SOURCES
       globals.cc
       loader.cc
       loader_talk.cc
       options.cc
       sanitizer.cc
       statistics.cc
       util/exception.cc
       util/logging.cc
       util/posix.cc
       util/string.cc
  )
  set (CVMFS_STUB_CFLAGS "-DCVMFS_FUSE_MODULE -DCVMFS_NAMESPACE_GUARD=loader -D_FILE_OFFSET_BITS=64 -fexceptions")
  set (CVMFS_STUB_LDFLAGS "-ldl -lm")
  set (CVMFS_STUB_LINK_LIBRARIES pthread dl)
  add_library (cvmfs_fuse_stub SHARED ${CVMFS_STUB_SOURCES})
  set_target_properties (cvmfs_fuse_stub PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "${CVMFS_STUB_CFLAGS} -DCVMFS_USE_LIBFUSE=2"
    LINK_FLAGS "${CVMFS_STUB_LDFLAGS}"
  )
  target_link_libraries (cvmfs_fuse_stub
                         ${FUSE_LIBRARIES} ${CVMFS_STUB_LINK_LIBRARIES}
  )
  if (FUSE3_FOUND)
    add_library (cvmfs_fuse3_stub SHARED ${CVMFS_STUB_SOURCES})
    set (CVMFS_STUB3_CFLAGS "${CVMFS_STUB_CFLAGS}")
    if (CVMFS_ENABLE_FUSE3_LOOP_CONFIG)
      set (CVMFS_STUB3_CFLAGS "${CVMFS_STUB3_CFLAGS} -DCVMFS_ENABLE_FUSE3_LOOP_CONFIG")
    endif ()
    set_target_properties (cvmfs_fuse3_stub PROPERTIES
      VERSION ${CernVM-FS_VERSION_STRING}
      COMPILE_FLAGS "${CVMFS_STUB3_CFLAGS} -DCVMFS_USE_LIBFUSE=3"
      LINK_FLAGS "${CVMFS_STUB_LDFLAGS}"
    )
    target_link_libraries (cvmfs_fuse3_stub
                           ${FUSE3_LIBRARIES} ${CVMFS_STUB_LINK_LIBRARIES}
    )
  endif ()


  #
  # /usr/lib/libcvmfs_fuse[3]_debug
  #
  set (CVMFS_FUSE_SOURCES
       ${CVMFS_COMMON_CLIENT_SOURCES}
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
       shortstring.cc
       supervisor.cc
       talk.cc
       url.cc
  )
  set (CVMFS_FUSE_CFLAGS "-DCVMFS_FUSE_MODULE -D_FILE_OFFSET_BITS=64 -fexceptions")
  if (CVMFS_ENABLE_FUSE3_CACHE_READDIR)
    set (CVMFS_FUSE_CFLAGS "${CVMFS_FUSE_CFLAGS} -DCVMFS_ENABLE_FUSE3_CACHE_READDIR")
  endif ()
  if (CVMFS_ENABLE_FUSE3_LOOP_CONFIG)
    set (CVMFS_FUSE_CFLAGS "${CVMFS_FUSE_CFLAGS} -DCVMFS_ENABLE_FUSE3_LOOP_CONFIG")
  endif ()

  set (CVMFS_FUSE_LDFLAGS "-ldl -lm")
  set (CVMFS_FUSE_LINK_LIBRARIES "")
  list(APPEND CVMFS_FUSE_LINK_LIBRARIES
       ${CURL_LIBRARIES}
       ${CARES_LIBRARIES} ${CARES_LDFLAGS}
       ${OPENSSL_LIBRARIES}
       ${PACPARSER_LIBRARIES}
       ${SQLITE3_LIBRARY}
       ${ZLIB_LIBRARIES}
       ${SPARSEHASH_LIBRARIES}
       ${LEVELDB_LIBRARIES}
       ${PROTOBUF_LITE_LIBRARY}
       ${VJSON_LIBRARIES}
       ${RT_LIBRARY}
       ${UUID_LIBRARIES}
       pthread
       dl
  )
  add_library (cvmfs_fuse SHARED ${CVMFS_FUSE_SOURCES})
  add_library (cvmfs_fuse_debug SHARED ${CVMFS_FUSE_SOURCES})
  add_dependencies (cvmfs_fuse cache.pb.generated)
  add_dependencies (cvmfs_fuse_debug cache.pb.generated)
  set_target_properties (cvmfs_fuse PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "-DCVMFS_USE_LIBFUSE=2 ${CVMFS_FUSE_CFLAGS}"
    LINK_FLAGS "${CVMFS_FUSE_LDFLAGS}"
  )
  set_target_properties (cvmfs_fuse_debug PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "-DCVMFS_USE_LIBFUSE=2 ${CVMFS_FUSE_CFLAGS} -O0 -DDEBUGMSG"
    LINK_FLAGS "${CVMFS_FUSE_LDFLAGS}"
  )
  target_link_libraries (cvmfs_fuse
                         cvmfs_crypto
                         cvmfs_util
                         ${FUSE_LIBRARIES} ${CVMFS_FUSE_LINK_LIBRARIES}
  )
  target_link_libraries (cvmfs_fuse_debug
                         cvmfs_crypto_debug
                         cvmfs_util_debug
                         ${FUSE_LIBRARIES} ${CVMFS_FUSE_LINK_LIBRARIES}
  )
  if (FUSE3_FOUND)
    add_library (cvmfs_fuse3 SHARED ${CVMFS_FUSE_SOURCES})
    add_library (cvmfs_fuse3_debug SHARED ${CVMFS_FUSE_SOURCES})
    add_dependencies (cvmfs_fuse3 cache.pb.generated)
    add_dependencies (cvmfs_fuse3_debug cache.pb.generated)
    set_target_properties (cvmfs_fuse3 PROPERTIES
      VERSION ${CernVM-FS_VERSION_STRING}
      COMPILE_FLAGS "-DCVMFS_USE_LIBFUSE=3 ${CVMFS_FUSE_CFLAGS}"
      LINK_FLAGS "${CVMFS_FUSE_LDFLAGS}"
    )
    set_target_properties (cvmfs_fuse3_debug PROPERTIES
      VERSION ${CernVM-FS_VERSION_STRING}
      COMPILE_FLAGS "-DCVMFS_USE_LIBFUSE=3 ${CVMFS_FUSE_CFLAGS} -O0 -DDEBUGMSG"
      LINK_FLAGS "${CVMFS_FUSE_LDFLAGS}"
    )
    target_link_libraries (cvmfs_fuse3
                           cvmfs_crypto
                           cvmfs_util
                           ${FUSE3_LIBRARIES} ${CVMFS_FUSE_LINK_LIBRARIES}
    )
    target_link_libraries (cvmfs_fuse3_debug
                           cvmfs_crypto_debug
                           cvmfs_util_debug
                           ${FUSE3_LIBRARIES} ${CVMFS_FUSE_LINK_LIBRARIES}
    )
  endif ()


  #
  # Installation: BUILD_CVMFS
  #
  install (
    TARGETS      cvmfs2 cvmfs_fsck cvmfs_talk
    RUNTIME
    DESTINATION  bin
  )
  install (
    TARGETS      cvmfs_allow_helper cvmfs_deny_helper
    RUNTIME
    DESTINATION  ${CVMFS_LIBEXEC_DIR}/authz
  )
  install (
    TARGETS      cvmfs_fuse_stub cvmfs_fuse cvmfs_fuse_debug
    LIBRARY
    DESTINATION  ${CMAKE_INSTALL_LIBDIR}
  )
  if (FUSE3_FOUND)
    install (
      TARGETS      cvmfs_fuse3_stub cvmfs_fuse3 cvmfs_fuse3_debug
      LIBRARY
      DESTINATION  ${CMAKE_INSTALL_LIBDIR}
    )
  endif ()
  install (
    FILES        cvmfs_config
    DESTINATION  bin
    PERMISSIONS  OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )
  if (INSTALL_BASH_COMPLETION)
    install (
        FILES         bash_completion/cvmfs.bash_completion
        RENAME        cvmfs
        DESTINATION   ${SYSCONF_INSTALL_DIR}/bash_completion.d
        PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )
  endif ()
  if (NOT MACOSX)
  # Systemd service unit for reloads after package upgrades
  install (
    FILES pkg/cvmfs-reload.service
    DESTINATION /usr/lib/systemd/system
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )
  # Apparmor fusermount3 profile extension
  install (
    FILES pkg/apparmor.local.fusermount3
    DESTINATION /etc/apparmor.d/local
    RENAME fusermount3
    PERMISSIONS   OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )
  endif()

endif (BUILD_CVMFS)




if (BUILD_LIBCVMFS)
  #
  # /usr/lib/libcvmfs_client
  #
  add_library (cvmfs_client SHARED
               ${CVMFS_COMMON_CLIENT_SOURCES}
               libcvmfs.cc
               libcvmfs_int.cc
               libcvmfs_legacy.cc
               libcvmfs_options.cc
  )
  add_dependencies (cvmfs_client cache.pb.generated)
  set_target_properties (cvmfs_client PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_LIBCVMFS -fexceptions -fPIC"
  )

  # Make the libcvmfs_client interface public
  set_property (SOURCE libcvmfs.cc PROPERTY COMPILE_FLAGS -fvisibility=default)
  set_property (SOURCE libcvmfs_legacy.cc PROPERTY COMPILE_FLAGS -fvisibility=default)
  set_property (SOURCE libcvmfs_options.cc PROPERTY COMPILE_FLAGS -fvisibility=default)

  # Make static libraries private to libcvmfs_client
  set_property (TARGET cvmfs_client PROPERTY INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/cvmfs")
  set_property (TARGET cvmfs_client APPEND PROPERTY INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}")
  set_property (TARGET cvmfs_client APPEND PROPERTY INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}")
  target_include_directories (cvmfs_client PRIVATE ${GTEST_INCLUDE_DIRS})
  target_include_directories (cvmfs_client PRIVATE ${SQLITE3_INCLUDE_DIR})
  target_include_directories (cvmfs_client PRIVATE ${CARES_INCLUDE_DIRS})
  target_include_directories (cvmfs_client PRIVATE ${CURL_INCLUDE_DIRS})
  target_include_directories (cvmfs_client PRIVATE ${LEVELDB_INCLUDE_DIR})
  target_include_directories (cvmfs_client PRIVATE ${PACPARSER_INCLUDE_DIR})
  target_include_directories (cvmfs_client PRIVATE ${ZLIB_INCLUDE_DIRS})
  target_include_directories (cvmfs_client PRIVATE ${VJSON_INCLUDE_DIRS})
  target_include_directories (cvmfs_client PRIVATE ${PROTOBUF_INCLUDE_DIRS})

  target_link_libraries (cvmfs_client
                         PUBLIC cvmfs_crypto
                                cvmfs_util
                                ${CARES_LDFLAGS}
                                ${OPENSSL_LIBRARIES}
                         PRIVATE ${SQLITE3_LIBRARY}
                                 ${CURL_LIBRARIES}
                                 ${CARES_LIBRARIES}
                                 ${LEVELDB_LIBRARIES}
                                 ${PACPARSER_LIBRARIES}
                                 ${ZLIB_LIBRARIES}
                                 ${VJSON_LIBRARIES}
                                 ${PROTOBUF_LITE_LIBRARY}
  )

  install (
    TARGETS      cvmfs_client
    LIBRARY
    DESTINATION  ${CMAKE_INSTALL_LIBDIR}
  )
  install (
    FILES        libcvmfs.h
    DESTINATION  include
  )
endif (BUILD_LIBCVMFS)




if (BUILD_LIBCVMFS_CACHE)
  #
  # /usr/lib/libcvmfs_cache
  #
  add_library (cvmfs_cache SHARED
               cache.pb.cc cache.pb.h
               cache_plugin/libcvmfs_cache.cc
               cache_plugin/libcvmfs_cache_options.cc
               cache_plugin/channel.cc
               cache_transport.cc
               monitor.cc
               options.cc
               sanitizer.cc
  )
  add_dependencies (cvmfs_cache cache.pb.generated)
  set_target_properties (cvmfs_cache PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_LIBCVMFS -fexceptions -fPIC"
  )

  # Make the libcvmfs_cache interface public
  set_property (SOURCE cache_plugin/libcvmfs_cache.cc PROPERTY
                COMPILE_FLAGS -fvisibility=default
  )
  set_property (SOURCE cache_plugin/libcvmfs_cache_options.cc PROPERTY
                COMPILE_FLAGS -fvisibility=default
  )

  # Make static libraries private to libcvmfs_cache
  set_property (TARGET cvmfs_cache PROPERTY INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/cvmfs")
  set_property (TARGET cvmfs_cache APPEND PROPERTY INCLUDE_DIRECTORIES "${CMAKE_BINARY_DIR}")
  set_property (TARGET cvmfs_cache APPEND PROPERTY INCLUDE_DIRECTORIES "${CMAKE_CURRENT_BINARY_DIR}")
  target_include_directories (cvmfs_cache PRIVATE ${GTEST_INCLUDE_DIRS})
  target_include_directories (cvmfs_cache PRIVATE ${PROTOBUF_INCLUDE_DIRS})

  target_link_libraries(cvmfs_cache
                        PUBLIC cvmfs_crypto
                               cvmfs_util
                        PRIVATE ${PROTOBUF_LITE_LIBRARY}
  )

  #
  # Cache plugins
  #
  add_executable (cvmfs_cache_null cache_plugin/cvmfs_cache_null.cc)
  target_link_libraries (cvmfs_cache_null
                         cvmfs_cache
                         ${RT_LIBRARY}
                         pthread
  )

  add_executable (cvmfs_cache_ram
                  cache_plugin/cvmfs_cache_ram.cc
                  malloc_heap.cc
                  statistics.cc
  )
  set_target_properties (cvmfs_cache_ram PROPERTIES COMPILE_FLAGS "-DDEBUGMSG")
  target_link_libraries (cvmfs_cache_ram
                         cvmfs_cache
                         cvmfs_util_debug
                         ${RT_LIBRARY}
                         pthread)

  add_executable (cvmfs_cache_posix
                  cache_plugin/cvmfs_cache_posix.cc
                  cache.cc
                  cache_posix.cc
                  compression/compression.cc
                  fd_refcount_mgr.cc
                  manifest.cc
                  quota.cc
  )
  set_target_properties (cvmfs_cache_posix PROPERTIES COMPILE_FLAGS "-DDEBUGMSG")
  target_link_libraries (cvmfs_cache_posix
                         cvmfs_cache
                         cvmfs_crypto
                         cvmfs_util_debug
                         ${ZLIB_LIBRARIES}
                         pthread
  )


  install (
    TARGETS      cvmfs_cache
    LIBRARY
    DESTINATION  ${CMAKE_INSTALL_LIBDIR}
  )
  install (
    FILES        cache_plugin/libcvmfs_cache.h
    DESTINATION  include
  )
  install (
    TARGETS      cvmfs_cache_ram cvmfs_cache_posix
    RUNTIME
    DESTINATION  ${CVMFS_LIBEXEC_DIR}/cache
  )
endif (BUILD_LIBCVMFS_CACHE)




if (NOT BUILD_SERVER)
  if (BUILD_SERVER_DEBUG)
    message (FATAL_ERROR
             "BUILD_SERVER needs to be on with BUILD_SERVER_DEBUG set. Exiting."
    )
  endif ()
endif ()

if (BUILD_SERVER)
  #
  # /usr/bin/cvmfs_suid_helper
  #
  add_executable (cvmfs_suid_helper
                  cvmfs_suid_helper.cc
                  cvmfs_suid_util.cc
                  sanitizer.cc
  )


  #
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
  # /usr/bin/cvmfs_swissknife[_debug]
  #
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
       compression/compression.cc
       directory_entry.cc
       file_chunk.cc
       gateway_util.cc
       globals.cc
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
       malloc_arena.cc
       manifest.cc
       manifest_fetch.cc
       monitor.cc
       network/dns.cc
       network/download.cc
       network/jobinfo.cc
       network/s3fanout.cc
       network/sink_file.cc
       network/sink_mem.cc
       network/sink_path.cc
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
       sanitizer.cc
       server_tool.cc
       session_context.cc
       signing_tool.cc
       shortstring.cc
       sql.cc
       sqlitemem.cc
       ssl.cc
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
       sync_item_dummy.cc
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
       whitelist.cc
       xattr.cc
  )  # CVMFS_SWISSKNIFE_SOURCES
  set (CVMFS_SWISSKNIFE_LINK_LIBRARIES "")
  list (APPEND CVMFS_SWISSKNIFE_LINK_LIBRARIES
        ${SQLITE3_LIBRARY}
        ${CURL_LIBRARIES}
        ${CARES_LIBRARIES} ${CARES_LDFLAGS}
        ${ZLIB_LIBRARIES}
        ${OPENSSL_LIBRARIES}
        ${RT_LIBRARY}
        ${VJSON_LIBRARIES}
        ${CAP_LIBRARIES}
        ${LibArchive_LIBRARY}
        pthread
        dl
  )
  add_executable (cvmfs_swissknife ${CVMFS_SWISSKNIFE_SOURCES})
  target_link_libraries (cvmfs_swissknife
                         cvmfs_crypto
                         cvmfs_util
                         ${CVMFS_SWISSKNIFE_LINK_LIBRARIES}
  )
  if (BUILD_SERVER_DEBUG)
    add_executable (cvmfs_swissknife_debug ${CVMFS_SWISSKNIFE_SOURCES})
    set_target_properties (cvmfs_swissknife_debug PROPERTIES
      COMPILE_FLAGS "-O0 -DDEBUGMSG"
    )
    target_link_libraries (cvmfs_swissknife_debug
                           cvmfs_crypto_debug
                           cvmfs_util_debug
                           ${CVMFS_SWISSKNIFE_LINK_LIBRARIES}
    )
  endif ()


  #
  # /usr/lib64/libcvmfs_server[_debug].so
  #
  set (LIBCVMFS_SERVER_SOURCES
       backoff.cc
       catalog.cc
       catalog_counters.cc
       catalog_rw.cc
       catalog_sql.cc
       catalog_mgr_ro.cc
       catalog_mgr_rw.cc
       catalog_virtual.cc
       compression/compression.cc
       directory_entry.cc
       gateway_util.cc
       globals.cc
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
       manifest.cc
       manifest_fetch.cc
       malloc_arena.cc
       network/dns.cc
       network/download.cc
       network/jobinfo.cc
       network/s3fanout.cc
       network/sink_file.cc
       network/sink_mem.cc
       network/sink_path.cc
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
       sanitizer.cc
       session_context.cc
       shortstring.cc
       sql.cc
       sqlitemem.cc
       ssl.cc
       statistics.cc
       swissknife_assistant.cc
       swissknife_lease_curl.cc
       sync_item.cc
       sync_item_dummy.cc
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
       whitelist.cc
       xattr.cc
  )
  set (LIBCVMFS_SERVER_CFLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_RAISE_EXCEPTIONS -fexceptions")
  set (LIBCVMFS_SERVER_LINK_LIBRARIES "")
  list (APPEND LIBCVMFS_SERVER_LINK_LIBRARIES
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
  add_library (cvmfs_server SHARED ${LIBCVMFS_SERVER_SOURCES})
  set_target_properties (cvmfs_server PROPERTIES
    VERSION ${CernVM-FS_VERSION_STRING}
    COMPILE_FLAGS "${LIBCVMFS_SERVER_CFLAGS}")
  target_link_libraries (cvmfs_server
                         cvmfs_crypto
                         cvmfs_util
                         ${LIBCVMFS_SERVER_LINK_LIBRARIES}
  )
  if (BUILD_SERVER_DEBUG)
    add_library (cvmfs_server_debug SHARED ${LIBCVMFS_SERVER_SOURCES})
    set_target_properties (cvmfs_server_debug PROPERTIES
      VERSION ${CernVM-FS_VERSION_STRING}
      COMPILE_FLAGS "${LIBCVMFS_SERVER_CFLAGS} -O0 -DDEBUGMSG")
    target_link_libraries (cvmfs_server_debug
                           cvmfs_crypto_debug
                           cvmfs_util_debug
                           ${LIBCVMFS_SERVER_LINK_LIBRARIES}
    )
  endif ()


  #
  # /usr/bin/cvmfs_publish[_debug]
  # TODO(jblomer) Rename to cvmfs_server
  #
  set (CVMFS_PUBLISH_SOURCES
       backoff.cc
       compression/compression.cc
       directory_entry.cc
       manifest.cc
       network/dns.cc
       network/download.cc
       network/jobinfo.cc
       network/sink_file.cc
       network/sink_mem.cc
       network/sink_path.cc
       options.cc
       publish/cmd_abort.cc
       publish/cmd_commit.cc
       publish/cmd_diff.cc
       publish/cmd_enter.cc
       publish/cmd_hash.cc
       publish/cmd_help.cc
       publish/cmd_info.cc
       publish/cmd_lsof.cc
       publish/cmd_mkfs.cc
       publish/cmd_transaction.cc
       publish/cmd_util.cc
       publish/cmd_zpipe.cc
       publish/command.cc
       publish/except.cc
       publish/main.cc
       publish/settings.cc
       sanitizer.cc
       ssl.cc
       statistics.cc
       upload_spooler_definition.cc
       whitelist.cc
  )  # CVMFS_PUBLISH_SOURCES
  set (CVMFS_PUBLISH_CFLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_RAISE_EXCEPTIONS -fexceptions")
  add_executable (cvmfs_publish ${CVMFS_PUBLISH_SOURCES})
  set_target_properties (cvmfs_publish PROPERTIES
    COMPILE_FLAGS "${CVMFS_PUBLISH_CFLAGS}"
  )
  target_link_libraries (cvmfs_publish
                         cvmfs_crypto
                         cvmfs_util
                         cvmfs_server
  )
  if (BUILD_SERVER_DEBUG)
    add_executable (cvmfs_publish_debug ${CVMFS_PUBLISH_SOURCES})
    set_target_properties (cvmfs_publish_debug PROPERTIES
      COMPILE_FLAGS "${CVMFS_PUBLISH_CFLAGS} -O0 -DDEBUGMSG"
    )
    target_link_libraries (cvmfs_publish_debug
                           cvmfs_crypto_debug
                           cvmfs_util_debug
                           cvmfs_server_debug)
  endif ()


  #
  # BUILD_SERVER: install
  #
  install(
    CODE  "FILE(MAKE_DIRECTORY \$ENV{DESTDIR}/var/lib/cvmfs-server)"
  )
  install (
    FILES        ${${PROJECT_NAME}_BINARY_DIR}/cvmfs/cvmfs_server
    DESTINATION  bin
    PERMISSIONS  OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )
  install(
    FILES        doc/README-spooler
    DESTINATION  "/var/spool/cvmfs"
    PERMISSIONS  OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    RENAME       README
  )
  install (
    TARGETS      cvmfs_suid_helper
    RUNTIME
    DESTINATION  bin
    PERMISSIONS  OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE SETUID
  )
  install (
    TARGETS      cvmfs_swissknife cvmfs_publish
    RUNTIME
    DESTINATION  bin
  )
  install (
    TARGETS      cvmfs_server
    LIBRARY
    DESTINATION  ${CMAKE_INSTALL_LIBDIR}
  )
  if (BUILD_SERVER_DEBUG)
    install (
      TARGETS      cvmfs_swissknife_debug cvmfs_publish_debug
      RUNTIME
      DESTINATION  bin
    )
    install (
      TARGETS      cvmfs_server_debug
      LIBRARY
      DESTINATION  ${CMAKE_INSTALL_LIBDIR}
    )
  endif (BUILD_SERVER_DEBUG)

  install(
    FILES        cvmfs_server_hooks.sh.demo
    DESTINATION  "${SYSCONF_INSTALL_DIR}/cvmfs"
    PERMISSIONS  OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
  )
  install (
    FILES        cvmfs_rsync
    DESTINATION  bin
    PERMISSIONS  OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  install(
    FILES        server/generate_stats_plots.C
    DESTINATION  "/usr/share/cvmfs-server"
    PERMISSIONS  OWNER_READ GROUP_READ WORLD_READ
  )
  install(
    FILES        server/upload_stats_plots.sh
    DESTINATION  "/usr/share/cvmfs-server"
    PERMISSIONS  OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )
  install(
    FILES        server/fix_stats_db.sh
    DESTINATION  "/usr/share/cvmfs-server"
    PERMISSIONS  OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )
  install(
    FILES        server/stats_index.html.tpl
    DESTINATION  "/usr/share/cvmfs-server"
    PERMISSIONS  OWNER_READ GROUP_READ WORLD_READ
  )

  if (BUILD_GEOAPI)
    install(
      CODE "FILE(MAKE_DIRECTORY \$ENV{DESTDIR}/var/lib/cvmfs-server/geo)"
    )

    install(
      FILES        webapi/cvmfs-api.wsgi
      DESTINATION  "/var/www/wsgi-scripts/cvmfs-server"
      PERMISSIONS  OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
    )
    install(
      FILES        webapi/cvmfs_api.py webapi/cvmfs_geo.py
      DESTINATION  "/usr/share/cvmfs-server/webapi"
      PERMISSIONS  OWNER_READ GROUP_READ WORLD_READ
    )
    install(
      DIRECTORY              ${EXTERNALS_INSTALL_LOCATION}/maxminddb
      DESTINATION            "/usr/share/cvmfs-server/webapi"
      FILE_PERMISSIONS       OWNER_READ GROUP_READ WORLD_READ
      DIRECTORY_PERMISSIONS  OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
    )
  endif (BUILD_GEOAPI)
endif (BUILD_SERVER)




if (NOT BUILD_RECEIVER)
  if (BUILD_RECEIVER_DEBUG)
    message (FATAL_ERROR
             "BUILD_RECEIVER needs to be on with BUILD_RECEIVER_DEBUG set. Exiting.")
  endif ()
endif ()

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
       compression/compression.cc
       directory_entry.cc
       gateway_util.cc
       globals.cc
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
       malloc_arena.cc
       manifest.cc
       manifest_fetch.cc
       monitor.cc
       network/dns.cc
       network/download.cc
       network/jobinfo.cc
       network/s3fanout.cc
       network/sink_file.cc
       network/sink_mem.cc
       network/sink_path.cc
       options.cc
       pack.cc
       reflog.cc
       reflog_sql.cc
       repository_tag.cc
       sanitizer.cc
       server_tool.cc
       session_context.cc
       signing_tool.cc
       shortstring.cc
       ssl.cc
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
       whitelist.cc
       xattr.cc
  )  # CVMFS_RECEIVER_SOURCES
  set (CVMFS_RECEIVER_CFLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_RAISE_EXCEPTIONS -fexceptions")
  set (CVMFS_RECEIVER_LINK_LIBRARIES "")
  list (APPEND CVMFS_RECEIVER_LINK_LIBRARIES
        ${CURL_LIBRARIES}
        ${CARES_LIBRARIES} ${CARES_LDFLAGS}
        ${SQLITE3_LIBRARY}
        ${VJSON_LIBRARIES}
        ${OPENSSL_LIBRARIES}
        ${ZLIB_LIBRARIES}
        ${RT_LIBRARY}
        ${LibArchive_LIBRARY}
        pthread
        dl
  )
  add_executable(cvmfs_receiver ${CVMFS_RECEIVER_SOURCES})
  set_target_properties (cvmfs_receiver PROPERTIES
    COMPILE_FLAGS "${CVMFS_RECEIVER_CFLAGS}"
  )
  target_link_libraries(cvmfs_receiver
                        cvmfs_crypto
                        cvmfs_util
                        ${CVMFS_RECEIVER_LINK_LIBRARIES}
  )

  if (BUILD_RECEIVER_DEBUG)
    add_executable(cvmfs_receiver_debug ${CVMFS_RECEIVER_SOURCES})
    set_target_properties (cvmfs_receiver_debug PROPERTIES
      COMPILE_FLAGS "${CVMFS_RECEIVER_CFLAGS} -O0 -DDEBUGMSG")
    target_link_libraries(cvmfs_receiver_debug
                          cvmfs_crypto_debug
                          cvmfs_util_debug
                          ${CVMFS_RECEIVER_LINK_LIBRARIES}
    )
  endif ()


  install (
    TARGETS      cvmfs_receiver
    RUNTIME
    DESTINATION  bin
  )
  if (BUILD_RECEIVER_DEBUG)
    install (
      TARGETS      cvmfs_receiver_debug
      RUNTIME
      DESTINATION  bin
    )
  endif (BUILD_RECEIVER_DEBUG)
endif (BUILD_RECEIVER)




if (BUILD_SHRINKWRAP)
  if (NOT BUILD_LIBCVMFS)
    message (FATAL_ERROR
             "BUILD_SHRINKWRAP needs to be on with BUILD_LIBCVMFS set. Exiting."
    )
  endif ()

  add_executable (cvmfs_shrinkwrap
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
                  xattr.cc
  )
  set_target_properties (cvmfs_shrinkwrap PROPERTIES
    COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_LIBCVMFS -fexceptions"
  )
  target_link_libraries(cvmfs_shrinkwrap
                        cvmfs_client
                        cvmfs_crypto
                        cvmfs_util
                        pthread
                        dl
  )


  install (
    TARGETS      cvmfs_shrinkwrap
    RUNTIME
    DESTINATION  bin
  )
  install (
    FILES        shrinkwrap/scripts/spec_builder.py
    DESTINATION  ${CVMFS_LIBEXEC_DIR}/shrinkwrap
    PERMISSIONS  OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )
endif(BUILD_SHRINKWRAP)




if (BUILD_PRELOADER)
  add_executable (cvmfs_preload_bin
                  backoff.cc
                  catalog.cc
                  catalog_sql.cc
                  compression/compression.cc
                  gateway_util.cc
                  globals.cc
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
                  malloc_arena.cc
                  manifest.cc
                  manifest_fetch.cc
                  network/dns.cc
                  network/download.cc
                  network/jobinfo.cc
                  network/s3fanout.cc
                  network/sink_file.cc
                  network/sink_mem.cc
                  network/sink_path.cc
                  options.cc
                  pack.cc
                  path_filters/dirtab.cc
                  path_filters/relaxed_path_filter.cc
                  pathspec/pathspec.cc
                  pathspec/pathspec_pattern.cc
                  preload.cc
                  reflog.cc
                  reflog_sql.cc
                  sanitizer.cc
                  server_tool.cc
                  session_context.cc
                  sql.cc
                  sqlitemem.cc
                  ssl.cc
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
                  whitelist.cc
                  xattr.cc
  )  # preloader sources

  target_link_libraries(cvmfs_preload_bin
                        cvmfs_crypto
                        cvmfs_util
                        ${SQLITE3_LIBRARY}
                        ${CURL_LIBRARIES}
                        ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                        ${ZLIB_LIBRARIES}
                        ${OPENSSL_LIBRARIES}
                        ${RT_LIBRARY}
                        ${UUID_LIBRARIES}
                        ${VJSON_LIBRARIES}
                        pthread
                        dl
  )

  #
  # Generate a bash self-extracting script for the cvmfs_preload target
  #
  add_custom_command (
    TARGET cvmfs_preload_bin POST_BUILD
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/cvmfs_preload_builder.sh ${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Generating the cvmfs_preload self-extracting script"
  )


  install (
    FILES        ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_preload
    DESTINATION  bin
    PERMISSIONS  OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )
endif (BUILD_PRELOADER)
