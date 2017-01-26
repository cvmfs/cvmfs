#
# Define the sources needed to compile the different
# executables in this directory
#

set (CVMFS_LOADER_SOURCES
  globals.cc
  hash.cc
  loader.cc
  loader_talk.cc
  logging.cc
  options.cc
  sanitizer.cc
  statistics.cc
  util/posix.cc
  util/string.cc
)

set (CVMFS_CLIENT_SOURCES
  authz/authz.cc
  authz/authz_curl.cc
  authz/authz_fetch.cc
  authz/authz_session.cc
  backoff.cc
  cache.cc
  cache.pb.cc
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
  dns.cc
  download.cc
  file_chunk.cc
  globals.cc
  glue_buffer.cc
  hash.cc
  history_sql.cc
  history_sqlite.cc
  json_document.cc
  kvstore.cc
  logging.cc
  malloc_arena.cc
  malloc_heap.cc
  manifest.cc
  manifest_fetch.cc
  monitor.cc
  mountpoint.cc
  options.cc
  quota.cc
  quota_posix.cc
  sanitizer.cc
  signature.cc
  sql.cc
  sqlitemem.cc
  sqlitevfs.cc
  statistics.cc
  tracer.cc
  uuid.cc
  util/algorithm.cc
  util/posix.cc
  util/string.cc
  util_concurrency.cc
  whitelist.cc
  wpad.cc
  xattr.cc
)

set_source_files_properties(cache.pb.cc cache.pb.h
                            PROPERTIES GENERATED true)

add_custom_command(OUTPUT cache.pb.cc cache.pb.h
                   COMMAND ${PROTOBUF_C} --cpp_out=.
                           ${CMAKE_CURRENT_SOURCE_DIR}/cache.proto
                           -I${CMAKE_CURRENT_SOURCE_DIR}
                   DEPENDS ${PROTOBUF_ARCHIVE}
                           ${CMAKE_CURRENT_SOURCE_DIR}/cache.proto
                   COMMENT "Generating protobuf sources")


set (CVMFS2_DEBUG_SOURCES
  ${CVMFS_CLIENT_SOURCES}

  auto_umount.cc
  compat.cc
  cvmfs.cc
  nfs_maps.cc
  nfs_shared_maps.cc
  quota_listener.cc
  talk.cc
)

set (CVMFS2_SOURCES
  ${CVMFS2_DEBUG_SOURCES}
)

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

set (LIBCVMFS_CACHE_SOURCES
  cache.pb.cc
  cache_plugin/libcvmfs_cache.cc
  cache_plugin/libcvmfs_cache_options.cc
  cache_plugin/channel.cc
  cache_transport.cc
  hash.cc
  logging.cc
  monitor.cc
  options.cc
  sanitizer.cc
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
  util/string.cc
)

set (CVMFS_FSCK_SOURCES
  compression.cc
  cvmfs_fsck.cc
  hash.cc
  logging.cc
  statistics.cc
  util/posix.cc
  util/string.cc
)


set (CVMFS_SWISSKNIFE_SOURCES
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
  file_processing/async_reader.cc
  file_processing/chunk.cc
  file_processing/chunk_detector.cc
  file_processing/file.cc
  file_processing/file_processor.cc
  file_processing/io_dispatcher.cc
  file_processing/processor.cc
  globals.cc
  hash.cc
  history_sql.cc
  history_sqlite.cc
  json_document.cc
  letter.cc
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
  reflog.cc
  reflog_sql.cc
  s3fanout.cc
  sanitizer.cc
  signature.cc
  sql.cc
  sqlitemem.cc
  statistics.cc
  swissknife.cc
  swissknife_assistant.cc
  swissknife_check.cc
  swissknife_gc.cc
  swissknife_graft.cc
  swissknife_hash.cc
  swissknife_history.cc
  swissknife_info.cc
  swissknife_letter.cc
  swissknife_lsrepo.cc
  swissknife_main.cc
  swissknife_migrate.cc
  swissknife_pull.cc
  swissknife_reflog.cc
  swissknife_scrub.cc
  swissknife_sign.cc
  swissknife_sync.cc
  swissknife_zpipe.cc
  swissknife_lease.cc
  swissknife_lease_curl.cc
  swissknife_lease_json.cc
  sync_item.cc
  sync_mediator.cc
  sync_union.cc
  upload.cc
  upload_facility.cc
  upload_local.cc
  upload_s3.cc
  upload_spooler_definition.cc
  util/algorithm.cc
  util/mmap_file.cc
  util/posix.cc
  util/string.cc
  util_concurrency.cc
  whitelist.cc
  xattr.cc
)

set (CVMFS_SWISSKNIFE_DEBUG_SOURCES
  ${CVMFS_SWISSKNIFE_SOURCES}
)

set (CVMFS_SUID_HELPER_SOURCES
  cvmfs_suid_helper.cc sanitizer.cc
)

set (CVMFS_PRELOADER_SOURCES
  catalog.cc
  catalog_sql.cc
  compression.cc
  dns.cc
  download.cc
  file_processing/async_reader.cc
  file_processing/chunk.cc
  file_processing/chunk_detector.cc
  file_processing/file.cc
  file_processing/file_processor.cc
  file_processing/io_dispatcher.cc
  file_processing/processor.cc
  globals.cc
  hash.cc
  history_sql.cc
  history_sqlite.cc
  logging.cc
  malloc_arena.cc
  manifest.cc
  manifest_fetch.cc
  options.cc
  path_filters/dirtab.cc
  path_filters/relaxed_path_filter.cc
  pathspec/pathspec.cc
  pathspec/pathspec_pattern.cc
  preload.cc
  reflog.cc
  reflog_sql.cc
  s3fanout.cc
  sanitizer.cc
  signature.cc
  sql.cc
  sqlitemem.cc
  statistics.cc
  swissknife.cc
  swissknife_pull.cc
  upload.cc
  upload_facility.cc
  upload_local.cc
  upload_s3.cc
  upload_spooler_definition.cc
  util/algorithm.cc
  util/mmap_file.cc
  util/posix.cc
  util/string.cc
  util_concurrency.cc
  uuid.cc
  whitelist.cc
  xattr.cc
)


#
# configure some compiler flags for proper build
#

set (CVMFS_LOADER_CFLAGS "${CVMFS_LOADER_CFLAGS} -DCVMFS_CLIENT -DCVMFS_NAMESPACE_GUARD=loader")

set (CVMFS2_CFLAGS "${CVMFS2_CFLAGS} -DCVMFS_CLIENT -D_FILE_OFFSET_BITS=64 -fexceptions")
set (CVMFS2_DEBUG_CFLAGS "${CVMFS2_DEBUG_CFLAGS} ${CVMFS2_CFLAGS} -D_FILE_OFFSET_BITS=64 -O0 -DDEBUGMSG")

set (CVMFS_SWISSKNIFE_DEBUG_CFLAGS "${CVMFS_SWISSKNIFE_DEBUG_CFLAGS} -DDEBUGMSG -g")

set (CVMFS_FSCK_CFLAGS "${CVMFS_FSCK_CFLAGS} -DCVMFS_CLIENT")

set (LIBCVMFS_CFLAGS "${LIBCVMFS_CFLAGS} -D_FILE_OFFSET_BITS=64 -DCVMFS_CLIENT -DCVMFS_LIBCVMFS -fexceptions")

set (LIBCVMFS_CACHE_CFLAGS "${LIBCVMFS_CACHE_CFLAGS} -D_FILE_OFFSET_BITS=64 -DCVMFS_CLIENT -DCVMFS_LIBCVMFS -fexceptions")

#
# set some include and linking stuff
#
set (CVMFS2_LD_FLAGS "${CVMFS2_LD_FLAGS} -ldl -lm")
include_directories (cvmfs ${CMAKE_CURRENT_BINARY_DIR} ${INCLUDE_DIRECTORIES})

#
# do the job!
#
# for every target you can define a <targetname>_CFLAGS and <targetname>_LD_FLAGS
# variable to pass some target specific build options.
#

if (BUILD_CVMFS)
  add_executable (cvmfs2 ${CVMFS_LOADER_SOURCES})
  add_library (cvmfs_fuse_debug SHARED  ${CVMFS2_DEBUG_SOURCES})
  add_library (cvmfs_fuse SHARED ${CVMFS2_SOURCES})
  add_executable (cvmfs_fsck ${CVMFS_FSCK_SOURCES})

  if (LIBFUSE_BUILTIN)
    add_dependencies (cvmfs_fuse_debug libfuse) # here it does not matter if libfuse or osxfuse
    add_dependencies (cvmfs_fuse libfuse)       # at this point only the appropriate one is defined
  endif (LIBFUSE_BUILTIN)

  if (LIBCURL_BUILTIN)
    add_dependencies (cvmfs_fuse_debug libcares libcurl)
    add_dependencies (cvmfs_fuse libcares libcurl)
  endif (LIBCURL_BUILTIN)

  if (PACPARSER_BUILTIN)
    add_dependencies (cvmfs_fuse_debug libpacparser)
    add_dependencies (cvmfs_fuse libpacparser)
  endif (PACPARSER_BUILTIN)

  if (SQLITE3_BUILTIN)
    add_dependencies (cvmfs_fuse_debug sqlite3)
    add_dependencies (cvmfs_fuse sqlite3)
  endif (SQLITE3_BUILTIN)

  if (ZLIB_BUILTIN)
    add_dependencies (cvmfs_fuse_debug zlib)
    add_dependencies (cvmfs_fuse zlib)
    add_dependencies (cvmfs_fsck zlib)
  endif (ZLIB_BUILTIN)

  if (SPARSEHASH_BUILTIN)
    add_dependencies (cvmfs_fuse_debug sparsehash)
    add_dependencies (cvmfs_fuse sparsehash)
  endif (SPARSEHASH_BUILTIN)

  if (LEVELDB_BUILTIN)
    add_dependencies (cvmfs_fuse_debug libleveldb)
    add_dependencies (cvmfs_fuse libleveldb)
  endif (LEVELDB_BUILTIN)

  add_dependencies (cvmfs_fuse_debug libsha3)
  add_dependencies (cvmfs_fuse libsha3)
  add_dependencies (cvmfs2 libsha3)
  add_dependencies (cvmfs_fsck libsha3)

  add_dependencies (cvmfs_fuse_debug libvjson)
  add_dependencies (cvmfs_fuse libvjson)

  add_dependencies (cvmfs_fuse_debug protobuf)
  add_dependencies (cvmfs_fuse protobuf)

  # set some properties
  set (CVMFS_LOADER_LD_FLAGS "${CVMFS_LOADER_LD_FLAGS} -ldl -lm")
  set_target_properties (cvmfs2 PROPERTIES COMPILE_FLAGS "${CVMFS_LOADER_CFLAGS}" LINK_FLAGS "${CVMFS_LOADER_LD_FLAGS}")
  set_target_properties (cvmfs_fuse_debug PROPERTIES COMPILE_FLAGS "${CVMFS2_DEBUG_CFLAGS}" LINK_FLAGS "${CVMFS2_LD_FLAGS} ${CVMFS2_DEBUG_LD_FLAGS}")
  set_target_properties (cvmfs_fuse PROPERTIES COMPILE_FLAGS "${CVMFS2_CFLAGS}" LINK_FLAGS "${CVMFS2_LD_FLAGS}")
  set_target_properties (cvmfs_fsck PROPERTIES COMPILE_FLAGS "${CVMFS_FSCK_CFLAGS}" LINK_FLAGS "${CVMFS_FSCK_LD_FLAGS}")

  set_target_properties (cvmfs_fuse PROPERTIES VERSION ${CernVM-FS_VERSION_STRING})
  set_target_properties (cvmfs_fuse_debug PROPERTIES VERSION ${CernVM-FS_VERSION_STRING})

  # link the stuff (*_LIBRARIES are dynamic link libraries *_archive are static link libraries ... one of them will be empty for each dependency)
  set (CVMFS_FUSE_LINK_LIBRARIES ${SQLITE3_LIBRARY} ${CARES_LIBRARIES}
         ${CURL_LIBRARIES} ${LIBCURL_ARCHIVE} ${PACPARSER_LIBRARIES}
         ${LEVELDB_LIBRARIES} ${OPENSSL_LIBRARIES} ${FUSE_LIBRARIES}
         ${LIBFUSE_ARCHIVE} ${SQLITE3_ARCHIVE} ${ZLIB_LIBRARIES}
         ${PACPARSER_ARCHIVE} ${LEVELDB_ARCHIVE} ${CARES_ARCHIVE}
         ${ZLIB_ARCHIVE} ${RT_LIBRARY} ${UUID_LIBRARIES}
         ${SHA3_ARCHIVE} ${VJSON_ARCHIVE} ${PROTOBUF_ARCHIVE} pthread dl)

  target_link_libraries (cvmfs2 ${CVMFS_LOADER_LIBS} ${OPENSSL_LIBRARIES}
                                ${LIBFUSE} ${RT_LIBRARY} ${UUID_LIBRARIES}
                                ${SHA3_ARCHIVE} pthread dl)
  target_link_libraries (cvmfs_fuse_debug ${CVMFS2_DEBUG_LIBS} ${CVMFS_FUSE_LINK_LIBRARIES})
  target_link_libraries (cvmfs_fuse       ${CVMFS2_LIBS} ${CVMFS_FUSE_LINK_LIBRARIES})
  target_link_libraries (cvmfs_fsck       ${CVMFS_FSCK_LIBS} ${ZLIB_LIBRARIES}
                                          ${OPENSSL_LIBRARIES} ${ZLIB_ARCHIVE}
                                          ${SHA3_ARCHIVE} ${RT_LIBRARY} pthread)


  set (CVMFS_ALLOW_HELPER_SOURCES
    authz/helper_allow.cc
    authz/helper_log.h authz/helper_log.cc
    authz/helper_util.h authz/helper_util.cc
  )
  add_executable (cvmfs_allow_helper ${CVMFS_ALLOW_HELPER_SOURCES})
  add_dependencies (cvmfs_allow_helper libvjson)
  target_link_libraries (cvmfs_allow_helper ${VJSON_ARCHIVE})

  set (CVMFS_DENY_HELPER_SOURCES
    authz/helper_deny.cc
    authz/helper_log.h authz/helper_log.cc
    authz/helper_util.h authz/helper_util.cc
  )
  add_executable (cvmfs_deny_helper ${CVMFS_DENY_HELPER_SOURCES})
  add_dependencies (cvmfs_deny_helper libvjson)
  target_link_libraries (cvmfs_deny_helper ${VJSON_ARCHIVE})
endif (BUILD_CVMFS)


if (BUILD_LIBCVMFS_CACHE)
  # libcvmfs_cache_only.a is a static lib of cvmfs without externals
  # libcvmfs_cache.a includes the externals as well
  add_library (cvmfs_cache_only STATIC ${LIBCVMFS_CACHE_SOURCES})
  set (LIBCVMFS_CACHE_LIBS ${LIBCVMFS_CACHE_LIBS} ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache_only.a)
  add_dependencies (cvmfs_cache_only libsha3)
  add_dependencies (cvmfs_cache_only protobuf)
  set (LIBCVMFS_CACHE_LIBS ${SHA3_ARCHIVE} ${PROTOBUF_ARCHIVE}
                           ${LIBCVMFS_CACHE_LIBS})
  set_target_properties (cvmfs_cache_only PROPERTIES COMPILE_FLAGS
                         "${LIBCVMFS_CACHE_CFLAGS}")
  if (NOT MACOSX)
    # We do not (yet) have a recipe for localizing private symbols
    # in Mac OS X, so skip that step on Mac
    set (LIBCVMFS_CACHE_PUBLIC_SYMS "-public" "${CMAKE_CURRENT_SOURCE_DIR}/cache_plugin/libcvmfs_cache_public_syms.txt")
  endif (NOT MACOSX)

  add_custom_command(
    OUTPUT libcvmfs_cache.a
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/combine_libs ${LIBCVMFS_CACHE_PUBLIC_SYMS} libcvmfs_cache.a ${LIBCVMFS_CACHE_LIBS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS ${LIBCVMFS_CACHE_LIBS}
  )

  # dummy target to cause merged libcvmfs_cache to be produced
  add_custom_target (libcvmfs_cache ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a)
  add_dependencies (libcvmfs_cache cvmfs_cache_only)

  add_executable(cvmfs_cache_null ${CVMFS_CACHE_NULL_SOURCES})
  target_link_libraries(cvmfs_cache_null
                        ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
                        ${OPENSSL_LIBRARIES} ${RT_LIBRARY} pthread)
  add_dependencies (cvmfs_cache_null libcvmfs_cache)

  add_executable(cvmfs_cache_ram ${CVMFS_CACHE_RAM_SOURCES})
  target_link_libraries(cvmfs_cache_ram
                        ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
                        ${OPENSSL_LIBRARIES} ${RT_LIBRARY} pthread)
  add_dependencies (cvmfs_cache_ram libcvmfs_cache)
endif (BUILD_LIBCVMFS_CACHE)


if (BUILD_LIBCVMFS)
  # libcvmfs_only.a is a static lib of cvmfs without externals
  # libcvmfs.a includes the externals as well
  add_library (cvmfs_only STATIC ${LIBCVMFS_SOURCES})
  set (LIBCVMFS_LIBS ${LIBCVMFS_LIBS} ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_only.a)

  if (LIBCURL_BUILTIN)
    add_dependencies (cvmfs_only libcares libcurl)
    set (LIBCVMFS_LIBS ${CARES_ARCHIVE} ${LIBCVMFS_LIBS} )
    set (LIBCVMFS_LIBS ${LIBCURL_ARCHIVE} ${LIBCVMFS_LIBS} )
  endif (LIBCURL_BUILTIN)

  if (PACPARSER_BUILTIN)
    add_dependencies (cvmfs_only libpacparser)
    set (LIBCVMFS_LIBS ${PACPARSER_ARCHIVE} ${LIBCVMFS_LIBS} )
  endif (PACPARSER_BUILTIN)

  if (SQLITE3_BUILTIN)
    add_dependencies (cvmfs_only sqlite3)
    set (LIBCVMFS_LIBS ${SQLITE3_ARCHIVE}  ${LIBCVMFS_LIBS} )
  endif (SQLITE3_BUILTIN)

  if (ZLIB_BUILTIN)
    add_dependencies (cvmfs_only zlib)
    set (LIBCVMFS_LIBS ${ZLIB_ARCHIVE} ${LIBCVMFS_LIBS} )
  endif (ZLIB_BUILTIN)

  if (SPARSEHASH_BUILTIN)
    add_dependencies (cvmfs_only sparsehash)
  endif (SPARSEHASH_BUILTIN)

  add_dependencies (cvmfs_only libsha3)
  add_dependencies (cvmfs_only libvjson)
  add_dependencies (cvmfs_only protobuf)
  set (LIBCVMFS_LIBS ${SHA3_ARCHIVE} ${VJSON_ARCHIVE} ${PROTOBUF_ARCHIVE}
                     ${LIBCVMFS_LIBS})

  set_target_properties (cvmfs_only PROPERTIES COMPILE_FLAGS "${LIBCVMFS_CFLAGS}")

  if (NOT MACOSX)
    # We do not (yet) have a recipe for localizing private symbols
    # in Mac OS X, so skip that step on Mac
    set (LIBCVMFS_PUBLIC_SYMS "-public" "${CMAKE_CURRENT_SOURCE_DIR}/libcvmfs_public_syms.txt")
  endif (NOT MACOSX)

  add_custom_command(
    OUTPUT libcvmfs.a
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/combine_libs ${LIBCVMFS_PUBLIC_SYMS} libcvmfs.a ${LIBCVMFS_LIBS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    DEPENDS ${LIBCVMFS_LIBS}
  )

  # dummy target to cause merged libcvmfs to be produced
  add_custom_target (libcvmfs ALL DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a)
  add_dependencies (libcvmfs cvmfs_only)

  add_executable( test_libcvmfs ${TEST_LIBCVMFS_SOURCES} )
  target_link_libraries(test_libcvmfs ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a
                        ${SQLITE3_LIBRARY} ${CARES_LIBRARIES} ${CURL_LIBRARIES}
                        ${PACPARSER_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES}
                        ${RT_LIBRARY} ${UUID_LIBRARIES} pthread dl )
  add_dependencies (test_libcvmfs libcvmfs)
endif (BUILD_LIBCVMFS)


if (BUILD_OCTOPUS)
  set (CVMFS_OCTOPUS_SOURCES
    encrypt.cc encrypt.h
    hash.cc hash.h
    json_document.cc json_document.h
    logging.cc logging.h logging_internal.h
    pathspec/pathspec.cc pathspec/pathspec.h
    pathspec/pathspec_pattern.cc pathspec/pathspec_pattern.h
    platform.h platform_linux.h platform_osx.h
	  sanitizer.cc sanitizer.h
    smalloc.h
    util/pointer.h
    util/posix.cc util/posix.h
    util/single_copy.h
    util/string.cc util/string.h
    uuid.cc uuid.h
		webapi/fcgi.cc webapi/fcgi.h
    webapi/macaroon.cc webapi/macaroon.h
    webapi/octopus.cc
    webapi/uri_map.cc webapi/uri_map.h
  )

  add_executable (cvmfs_octopus.fcgi ${CVMFS_OCTOPUS_SOURCES})
  add_dependencies (cvmfs_octopus.fcgi libsha2)
  add_dependencies (cvmfs_octopus.fcgi libvjson)
  target_link_libraries(cvmfs_octopus.fcgi
                        ${SHA3_ARCHIVE} ${VJSON_ARCHIVE} ${OPENSSL_LIBRARIES}
                        ${RT_LIBRARY} ${UUID_LIBRARIES} pthread)
endif (BUILD_OCTOPUS)


if (BUILD_SERVER)
  # create targets with corresponding sources and static link libraries
  add_executable (cvmfs_swissknife ${CVMFS_SWISSKNIFE_SOURCES})
  add_executable (cvmfs_suid_helper ${CVMFS_SUID_HELPER_SOURCES})

  if (GEOIP_BUILTIN)
    add_custom_target (geoip ALL)
    add_dependencies (python-geoip libgeoip)
    add_dependencies (geoip python-geoip)
  endif (GEOIP_BUILTIN)

  add_dependencies (cvmfs_swissknife libvjson)

  if (LIBCURL_BUILTIN)
    add_dependencies (cvmfs_swissknife libcares libcurl)
  endif (LIBCURL_BUILTIN)

  if (SQLITE3_BUILTIN)
    add_dependencies (cvmfs_swissknife sqlite3)
  endif (SQLITE3_BUILTIN)

  if (ZLIB_BUILTIN)
    add_dependencies (cvmfs_swissknife zlib)
  endif (ZLIB_BUILTIN)

  if (TBB_PRIVATE_LIB)
    add_dependencies (cvmfs_swissknife libtbb)
  endif (TBB_PRIVATE_LIB)

  add_dependencies (cvmfs_swissknife libsha3)

  set_target_properties (cvmfs_swissknife PROPERTIES COMPILE_FLAGS "${CVMFS_SWISSKNIFE_CFLAGS}" LINK_FLAGS "${CVMFS_SWISSKNIFE_LD_FLAGS}")

  # link the stuff (*_LIBRARIES are dynamic link libraries)
  target_link_libraries (cvmfs_swissknife  ${CVMFS_SWISSKNIFE_LIBS}
                         ${SQLITE3_LIBRARY}  ${CURL_LIBRARIES} ${LIBCURL_ARCHIVE}
                         ${CARES_LIBRARIES} ${CARES_ARCHIVE} ${TBB_LIBRARIES}
                         ${ZLIB_LIBRARIES} ${ZLIB_ARCHIVE} ${OPENSSL_LIBRARIES}
                         ${SQLITE3_ARCHIVE} ${RT_LIBRARY} ${VJSON_ARCHIVE}
                         ${SHA3_ARCHIVE} ${CAP_LIBRARIES} pthread dl)

  if (BUILD_SERVER_DEBUG)
    add_executable (cvmfs_swissknife_debug ${CVMFS_SWISSKNIFE_DEBUG_SOURCES})

    if (LIBCURL_BUILTIN)
      add_dependencies (cvmfs_swissknife_debug libcares libcurl)
    endif (LIBCURL_BUILTIN)

    if (SQLITE3_BUILTIN)
      add_dependencies (cvmfs_swissknife_debug sqlite3)
    endif (SQLITE3_BUILTIN)

    if (ZLIB_BUILTIN)
      add_dependencies (cvmfs_swissknife_debug zlib)
    endif (ZLIB_BUILTIN)

    if (TBB_PRIVATE_LIB)
      add_dependencies (cvmfs_swissknife_debug libtbb)
    endif (TBB_PRIVATE_LIB)

    add_dependencies (cvmfs_swissknife_debug libsha3)

    set_target_properties (cvmfs_swissknife_debug PROPERTIES COMPILE_FLAGS "${CVMFS_SWISSKNIFE_DEBUG_CFLAGS}" LINK_FLAGS "${CVMFS_SWISSKNIFE_DEBUG_LD_FLAGS}")

    # link the stuff (*_LIBRARIES are dynamic link libraries)
    if (NOT TBB_DEBUG_LIBRARIES)
      message (WARNING "Debug libraries of TBB were not found. Using the release versions instead.")
      set (TBB_DEBUG_LIBRARIES ${TBB_LIBRARIES})
    endif (NOT TBB_DEBUG_LIBRARIES)
    target_link_libraries (cvmfs_swissknife_debug  ${CVMFS_SWISSKNIFE_LIBS}
                           ${SQLITE3_LIBRARY}  ${CURL_LIBRARIES} ${LIBCURL_ARCHIVE}
                           ${CARES_LIBRARIES} ${CARES_ARCHIVE} ${TBB_DEBUG_LIBRARIES}
                           ${ZLIB_LIBRARIES} ${ZLIB_ARCHIVE} ${OPENSSL_LIBRARIES}
                           ${SQLITE3_ARCHIVE} ${RT_LIBRARY} ${VJSON_ARCHIVE}
                           ${SHA3_ARCHIVE} ${CAP_LIBRARIES} pthread dl)
  endif (BUILD_SERVER_DEBUG)
endif (BUILD_SERVER)

if (BUILD_PRELOADER)
  add_executable (cvmfs_preload_bin ${CVMFS_PRELOADER_SOURCES})

  if (LIBCURL_BUILTIN)
    add_dependencies (cvmfs_preload_bin libcares libcurl)
    set (PRELOADER_LIBS ${CARES_ARCHIVE} ${PRELOADER_LIBS} )
    set (PRELOADER_LIBS ${LIBCURL_ARCHIVE} ${PRELOADER_LIBS} )
  endif (LIBCURL_BUILTIN)

  if (PACPARSER_BUILTIN)
    add_dependencies (cvmfs_preload_bin libpacparser)
    set (PRELOADER_LIBS ${PACPARSER_ARCHIVE} ${PRELOADER_LIBS} )
  endif (PACPARSER_BUILTIN)

  if (SQLITE3_BUILTIN)
    add_dependencies (cvmfs_preload_bin sqlite3)
    set (PRELOADER_LIBS ${SQLITE3_ARCHIVE}  ${PRELOADER_LIBS} )
  endif (SQLITE3_BUILTIN)

  if (ZLIB_BUILTIN)
    add_dependencies (cvmfs_preload_bin zlib)
    set (PRELOADER_LIBS ${ZLIB_ARCHIVE} ${PRELOADER_LIBS} )
  endif (ZLIB_BUILTIN)

  if (SPARSEHASH_BUILTIN)
    add_dependencies (cvmfs_preload_bin sparsehash)
  endif (SPARSEHASH_BUILTIN)

  if (TBB_PRIVATE_LIB)
    add_dependencies (cvmfs_preload_bin libtbb)
  endif (TBB_PRIVATE_LIB)

  add_dependencies (cvmfs_preload_bin libsha3)
  add_dependencies (cvmfs_preload_bin libvjson)

  target_link_libraries(cvmfs_preload_bin ${SQLITE3_LIBRARY} ${CARES_LIBRARIES}
                        ${CURL_LIBRARIES} ${LIBCURL_ARCHIVE} ${ZLIB_LIBRARIES}
                        ${OPENSSL_LIBRARIES} ${CARES_ARCHIVE} ${SQLITE3_ARCHIVE}
                        ${ZLIB_ARCHIVE} ${TBB_LIBRARIES} ${RT_LIBRARY}
                        ${UUID_LIBRARIES} ${VJSON_ARCHIVE} ${SHA3_ARCHIVE}
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
  cvmfs_server
  ALL
  COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR} && ./make_cvmfs_server.sh ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_server
  COMMENT "Generating the cvmfs_server script"
  )

#
# installation
#

if (BUILD_CVMFS)
  install (
    TARGETS      cvmfs2 cvmfs_fsck
    RUNTIME
    DESTINATION    bin
  )

  install (
    TARGETS      cvmfs_allow_helper cvmfs_deny_helper
    RUNTIME
    DESTINATION  ${CVMFS_LIBEXEC_DIR}/authz
  )

  install (
    TARGETS      cvmfs_fuse cvmfs_fuse_debug
    LIBRARY
    DESTINATION    ${CMAKE_INSTALL_LIBDIR}
  )

  install (
    FILES      cvmfs_talk cvmfs_config
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
    FILES      ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a
    DESTINATION    ${CMAKE_INSTALL_LIBDIR}
  )
  install (
    FILES                   libcvmfs.h
    DESTINATION             include
  )
endif (BUILD_LIBCVMFS)

if (BUILD_LIBCVMFS_CACHE)
  install (
    FILES      ${CMAKE_CURRENT_BINARY_DIR}/libcvmfs_cache.a
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
endif (BUILD_LIBCVMFS_CACHE)

if (BUILD_SERVER)
  install (
    TARGETS     cvmfs_swissknife
    RUNTIME
    DESTINATION    bin
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
    DESTINATION    "/var/www/wsgi-scripts"
    PERMISSIONS    OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )

  if (BUILD_OCTOPUS)
    install(
      TARGETS      cvmfs_octopus.fcgi
      RUNTIME
      DESTINATION  "/usr/share/cvmfs-server/webapi"
      PERMISSIONS  OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
    )
  endif (BUILD_OCTOPUS)

  if (GEOIP_BUILTIN)
    install(
      FILES      ${PYTHON_GEOIP_BUILTIN_LOCATION}/dist/GeoIP.so
      DESTINATION    "/usr/share/cvmfs-server/webapi"
      PERMISSIONS    OWNER_READ GROUP_READ WORLD_READ
    )
  endif (GEOIP_BUILTIN)

  install(
    FILES      webapi/cvmfs_api.py webapi/cvmfs_geo.py
    DESTINATION    "/usr/share/cvmfs-server/webapi"
    PERMISSIONS    OWNER_READ GROUP_READ WORLD_READ
  )

  install(
    FILES          doc/README-spooler
    DESTINATION    "/var/spool/cvmfs"
    PERMISSIONS    OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    RENAME         README
  )

  install(
    CODE  "FILE(MAKE_DIRECTORY \$ENV{DESTDIR}/var/lib/cvmfs-server/geo)"
  )

endif (BUILD_SERVER)

if (BUILD_PRELOADER)
  install(
    FILES      ${CMAKE_CURRENT_BINARY_DIR}/cvmfs_preload
    DESTINATION    bin
    PERMISSIONS    OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  )
endif (BUILD_PRELOADER)
