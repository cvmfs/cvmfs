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
  # /usr/lib/libcvmfs_fuse[3] and /usr/lib/libcvmfs.a
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
       cache_tiered.cc
       cache_transport.cc
       catalog.cc
       catalog_counters.cc
       catalog_mgr_client.cc
       catalog_sql.cc
       clientctx.cc
       compression.cc
       crypto/crypto_util.cc
       crypto/hash.cc
       crypto/signature.cc
       directory_entry.cc
       duplex_fuse.cc
       dns.cc
       download.cc
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
       options.cc
       quota.cc
       quota_posix.cc
       resolv_conf_event_handler.cc
       sanitizer.cc
       sql.cc
       sqlitemem.cc
       sqlitevfs.cc
       ssl.cc
       statistics.cc
       tracer.cc
       util/algorithm.cc
       util/concurrency.cc
       util/exception.cc
       util/logging.cc
       util/posix.cc
       util/string.cc
       util/uuid.cc
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
                  compression.cc
                  crypto/hash.cc
                  cvmfs_fsck.cc
                  statistics.cc
                  util/exception.cc
                  util/logging.cc
                  util/posix.cc
                  util/string.cc
  )
  target_link_libraries (cvmfs_fsck
                         ${ZLIB_LIBRARIES}
                         ${OPENSSL_LIBRARIES}
                         ${SHA3_LIBRARIES}
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
                  util/exception.cc
                  util/logging.cc
                  util/posix.cc
                  util/string.cc
  )
  target_link_libraries (cvmfs_talk ${RT_LIBRARY} pthread)

  #
  # /usr/bin/cvmfs2
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
  #
  set (CVMFS_STUB_SOURCES
       crypto/crypto_util.cc
       crypto/hash.cc
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
  set (CVMFS_STUB_LINK_LIBRARIES "")
  list (APPEND CVMFS_STUB_LINK_LIBRARIES
        ${OPENSSL_LIBRARIES}
        ${SHA3_LIBRARIES}
        pthread
        dl
  )
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
    set_target_properties (cvmfs_fuse3_stub PROPERTIES
      VERSION ${CernVM-FS_VERSION_STRING}
      COMPILE_FLAGS "${CVMFS_STUB_CFLAGS} -DCVMFS_USE_LIBFUSE=3"
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
       ${SHA3_LIBRARIES}
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
                         ${FUSE_LIBRARIES} ${CVMFS_FUSE_LINK_LIBRARIES}
  )
  target_link_libraries (cvmfs_fuse_debug
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
                           ${FUSE3_LIBRARIES} ${CVMFS_FUSE_LINK_LIBRARIES}
    )
    target_link_libraries (cvmfs_fuse3_debug
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
endif (BUILD_CVMFS)




if (BUILD_LIBCVMFS)
  add_library (cvmfs_only STATIC
               ${CVMFS_COMMON_CLIENT_SOURCES}
               libcvmfs.cc
               libcvmfs_int.cc
               libcvmfs_legacy.cc
               libcvmfs_options.cc
  )
  add_dependencies (cvmfs_only cache.pb.generated)
  set_target_properties (cvmfs_only PROPERTIES
    COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_LIBCVMFS -fexceptions -fPIC"
  )
  set (LIBCVMFS_LIBS
       ${CARES_LIBRARIES}
       ${CURL_LIBRARIES}
       ${PACPARSER_LIBRARIES}
       ${SQLITE3_LIBRARY}
       ${ZLIB_LIBRARIES}
       ${SPARSEHASH_LIBRARIES}
       ${SHA3_LIBRARIES}
       ${VJSON_LIBRARIES}
       ${PROTOBUF_LITE_LIBRARY}
       ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_only.a
  )
  # Note: it would be a good idea to try to unify the next block for macOS and
  #       Linux. It may be possible to use libtool on both platforms, including
  #       the ability to hide private symbols in the library - which is
  #       currently done by the `combine_libs` script, on Linux only.
  if (MACOSX)
    add_custom_command(
        OUTPUT libcvmfs.a
        COMMAND libtool -static -o libcvmfs.a ${LIBCVMFS_LIBS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        DEPENDS ${LIBCVMFS_LIBS}
    )
  else (MACOSX)
    set (LIBCVMFS_PUBLIC_SYMS "-public"
         "${CMAKE_CURRENT_SOURCE_DIR}/libcvmfs_public_syms.txt")
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


  add_executable (test_libcvmfs test_libcvmfs.cc)
  add_dependencies(test_libcvmfs libcvmfs)
  target_link_libraries(test_libcvmfs
                        ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a
                        ${SQLITE3_LIBRARY}
                        ${CURL_LIBRARIES}
                        ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                        ${PACPARSER_LIBRARIES}
                        ${ZLIB_LIBRARIES}
                        ${OPENSSL_LIBRARIES}
                        ${RT_LIBRARY}
                        ${UUID_LIBRARIES}
                        pthread
                        dl)


  install (
    FILES        ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a
    DESTINATION  ${CMAKE_INSTALL_LIBDIR}
  )
  install (
    FILES        libcvmfs.h
    DESTINATION  include
  )
endif (BUILD_LIBCVMFS)




if (BUILD_LIBCVMFS_CACHE)
  #
  # /usr/lib/libcvmfs_cache.a
  #
  add_library (cvmfs_cache_only STATIC
               cache.pb.cc cache.pb.h
               cache_plugin/libcvmfs_cache.cc
               cache_plugin/libcvmfs_cache_options.cc
               cache_plugin/channel.cc
               cache_transport.cc
               crypto/hash.cc
               monitor.cc
               options.cc
               sanitizer.cc
               util/concurrency.cc
               util/exception.cc
               util/logging.cc
               util/posix.cc
               util/string.cc
  )
  add_dependencies (cvmfs_cache_only cache.pb.generated)
  set (LIBCVMFS_CACHE_LIBS
       ${SHA3_LIBRARIES}
       ${PROTOBUF_LITE_LIBRARY}
       ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache_only.a
  )
  if (MACOSX)
    list (APPEND LIBCVMFS_CACHE_LIBS ${OPENSSL_LIBRARIES})
  endif ()
  set_target_properties (cvmfs_cache_only PROPERTIES
    COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_LIBCVMFS -fexceptions"
  )

  # See libcvmfs.a for details
  if (MACOSX)
    add_custom_command(
        OUTPUT libcvmfs_cache.a
        COMMAND libtool -static -o libcvmfs_cache.a ${LIBCVMFS_CACHE_LIBS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        DEPENDS ${LIBCVMFS_CACHE_LIBS}
      )
  else (MACOSX)
    set (LIBCVMFS_CACHE_PUBLIC_SYMS "-public"
         "${CMAKE_CURRENT_SOURCE_DIR}/cache_plugin/libcvmfs_cache_public_syms.txt"
    )
    add_custom_command(
        OUTPUT libcvmfs_cache.a
        COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/combine_libs ${LIBCVMFS_CACHE_PUBLIC_SYMS} libcvmfs_cache.a ${LIBCVMFS_CACHE_LIBS}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        DEPENDS ${LIBCVMFS_CACHE_LIBS}
      )
  endif (MACOSX)

  # dummy target to cause merged libcvmfs_cache to be produced
  add_custom_target (libcvmfs_cache ALL DEPENDS
                     ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a)
  add_dependencies (libcvmfs_cache cvmfs_cache_only)

  #
  # Cache plugins
  #
  add_executable (cvmfs_cache_null cache_plugin/cvmfs_cache_null.cc)
  add_dependencies (cvmfs_cache_null libcvmfs_cache)
  target_link_libraries (cvmfs_cache_null
                         ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
                         ${OPENSSL_LIBRARIES}
                         ${RT_LIBRARY}
                         pthread
  )

  add_executable (cvmfs_cache_ram
                  cache_plugin/cvmfs_cache_ram.cc
                  malloc_heap.cc
                  statistics.cc
                  util/concurrency.cc
                  util/exception.cc
                  util/logging.cc
                  util/posix.cc
                  util/string.cc
  )
  add_dependencies (cvmfs_cache_ram libcvmfs_cache)
  set_target_properties (cvmfs_cache_ram PROPERTIES COMPILE_FLAGS "-DDEBUGMSG")
  target_link_libraries (cvmfs_cache_ram
                         ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
                         ${OPENSSL_LIBRARIES}
                         ${RT_LIBRARY}
                         pthread)

  add_executable (cvmfs_cache_posix
                  cache_plugin/cvmfs_cache_posix.cc
                  cache.cc
                  cache_posix.cc
                  compression.cc
                  crypto/hash.cc
                  manifest.cc
                  quota.cc
                  util/exception.cc
                  util/logging.cc
                  util/posix.cc
                  util/string.cc
  )
  add_dependencies (cvmfs_cache_posix libcvmfs_cache)
  set_target_properties (cvmfs_cache_posix PROPERTIES COMPILE_FLAGS "-DDEBUGMSG")
  target_link_libraries (cvmfs_cache_posix
                         ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
                         ${OPENSSL_LIBRARIES}
                         ${SHA3_LIBRARIES}
                         ${ZLIB_LIBRARIES}
                         pthread
  )


  install (
    FILES        ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
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
       compression.cc
       directory_entry.cc
       dns.cc
       download.cc
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
       compression.cc
       directory_entry.cc
       dns.cc
       download.cc
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
       shortstring.cc
       sql.cc
       sqlitemem.cc
       ssl.cc
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
       compression.cc
       directory_entry.cc
       dns.cc
       download.cc
       manifest.cc
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
      FILES        ${EXTERNALS_INSTALL_LOCATION}/ipaddress.py
      DESTINATION  "/usr/share/cvmfs-server/webapi"
      PERMISSIONS  OWNER_READ GROUP_READ WORLD_READ
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
       compression.cc
       directory_entry.cc
       dns.cc
       download.cc
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
       options.cc
       pack.cc
       reflog.cc
       reflog_sql.cc
       repository_tag.cc
       s3fanout.cc
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
  add_executable (cvmfs_shrinkwrap
                  crypto/hash.cc
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
                  util/concurrency.cc
                  util/exception.cc
                  util/logging.cc
                  util/posix.cc
                  util/string.cc
                  xattr.cc
  )
  add_dependencies (cvmfs_shrinkwrap libcvmfs)
  set_target_properties (cvmfs_shrinkwrap PROPERTIES
    COMPILE_FLAGS "-D_FILE_OFFSET_BITS=64 -DCVMFS_LIBCVMFS -fexceptions"
  )
  target_link_libraries(cvmfs_shrinkwrap
                        ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a
                        ${SQLITE3_LIBRARY}
                        ${CURL_LIBRARIES}
                        ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                        ${PACPARSER_LIBRARIES}
                        ${ZLIB_LIBRARIES}
                        ${OPENSSL_LIBRARIES}
                        ${SHA3_LIBRARIES}
                        ${VJSON_LIBRARIES}
                        ${RT_LIBRARY}
                        ${UUID_LIBRARIES}
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
                  compression.cc
                  crypto/hash.cc
                  crypto/signature.cc
                  dns.cc
                  download.cc
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
                  util/algorithm.cc
                  util/concurrency.cc
                  util/exception.cc
                  util/file_backed_buffer.cc
                  util/logging.cc
                  util/mmap_file.cc
                  util/posix.cc
                  util/string.cc
                  util/uuid.cc
                  whitelist.cc
                  xattr.cc
  )  # preloader sources

  target_link_libraries(cvmfs_preload_bin
                        ${SQLITE3_LIBRARY}
                        ${CURL_LIBRARIES}
                        ${CARES_LIBRARIES} ${CARES_LDFLAGS}
                        ${ZLIB_LIBRARIES}
                        ${OPENSSL_LIBRARIES}
                        ${RT_LIBRARY}
                        ${UUID_LIBRARIES}
                        ${VJSON_LIBRARIES}
                        ${SHA3_LIBRARIES}
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
