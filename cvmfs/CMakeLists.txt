#
# Define the sources needed to compile the different
# executables in this directory
#

set (CVMFS_CLIENT_SOURCES
  smalloc.h
	logging.cc logging.h logging_internal.h
	tracer.h tracer.cc atomic.h
	duplex_sqlite3.h duplex_curl.h
	signature.h signature.cc
	quota.h quota.cc
	hash.h hash.cc
	cache.h cache.cc
	platform.h platform_osx.h platform_linux.h
	monitor.h monitor.cc
	util.cc util.h
	duplex_zlib.h compression.h compression.cc
	download.cc download.h
	lru.h lru.cc
	globals.h globals.cc
	peers.h peers.cc
	catalog_queries.h catalog_queries.cc
	catalog.h catalog.cc
	catalog_mgr.h catalog_mgr.cc
	shortstring.h dirent.h
)

set (CVMFS2_DEBUG_SOURCES
  ${CVMFS_CLIENT_SOURCES}

  talk.h talk.cc
  nfs_maps.h nfs_maps.cc
  cvmfs.h cvmfs.cc
)

set (CVMFS2_SOURCES
  ${CVMFS2_DEBUG_SOURCES}
)

set (LIBCVMFS_SOURCES
  ${CVMFS_CLIENT_SOURCES}
  libcvmfs_int.h libcvmfs_int.cc
  libcvmfs.h libcvmfs.cc
)

set (CVMFS_FSCK_SOURCES
	platform.h platform_linux.h platform_osx.h
  logging_internal.h logging.h logging.cc
  smalloc.h
  atomic.h
	duplex_zlib.h compression.cc compression.h
	hash.cc hash.h
	util.cc util.h
  cvmfs_fsck.cc)

set (CVMFS_SYNC_BIN_SOURCES
	smalloc.h atomic.h
  globals.h globals.cc
	duplex_zlib.h compression.h compression.cc
  hash.h hash.cc
	duplex_sqlite3.h
	util.cc util.h
	logging.cc logging.h logging_internal.h
	monitor.cc monitor.h
	fs_traversal.h
	sync_item.h sync_item.cc
	sync_union.h sync_union.cc
	sync_mediator.h sync_mediator.cc
  duplex_curl.h download.h download.cc
  upload.h upload.cc
  manifest.h manifest.cc

  dirent.h
  catalog_queries.h catalog_queries.cc
	catalog.h catalog.cc
	catalog_rw.h catalog_rw.cc
	catalog_mgr.h catalog_mgr.cc
	catalog_mgr_rw.h catalog_mgr_rw.cc

  cvmfs_sync.h cvmfs_sync.cc)

#set (CVMFS_LSCAT_SOURCES
#	smalloc.h
#	duplex_zlib.h compression.cc compression.h
#	catalog_tree.h catalog_tree.cc
#	duplex_sqlite3.h catalog.cc catalog.h
#	hash.cc hash.h
#	util.cc util.h cvmfs_lscat.cc)

set (CVMFS_SIGN_SOURCES
	smalloc.h atomic.h
  logging.h logging_internal.h logging.cc
	duplex_zlib.h compression.h compression.cc
  upload.h upload.cc
	hash.cc hash.h
	util.h util.cc
	signature.cc signature.h
  manifest.h manifest.cc
  cvmfs_sign.cc)

#set (CVMFS_PULL_SOURCES
#	smalloc.h
#	logging.cc logging.h logging_internal.h
#	duplex_zlib.h compression.h compression.cc
#	duplex_curl.h download.cc download.h
#	hash.h hash.cc
#	util.h util.cc
#	catalog_tree.h catalog_tree.cc
#	duplex_sqlite3.h catalog.h catalog.cc
#	signature.h signature.cc
#	cvmfs_pull.cc)

#set (CVMFS_SCRUB_SOURCES
#	smalloc.h
#	hash.h hash.cc
#	util.h util.cc
#	cvmfs_scrub.cc)

set (CVMFS_ZPIPE_SOURCES duplex_zlib.h zpipe.c)

#
# configure some compiler flags for proper build
#

set (CVMFS2_CFLAGS "${CVMFS2_CFLAGS} -DCVMFS_CLIENT -D_FILE_OFFSET_BITS=64 -fexceptions")
set (CVMFS2_DEBUG_CFLAGS "${CVMFS2_DEBUG_CFLAGS} ${CVMFS2_CFLAGS} -D_FILE_OFFSET_BITS=64 -O0 -DDEBUGMSG")
set (CVMFS2_CFLAGS "${CVMFS2_CFLAGS}")

set (CVMFS_FSCK_CFLAGS "${CVMFS_FSCK_CFLAGS} -DCVMFS_CLIENT")

#
# set properties for configurable libraries
#
# some libraries are statically linked and has to be compiled from source
# therefore we have to include the specific CVMFS-CMakeLists.txt from the
# different 3rd-party locations.
# If dynamic linking is desired we have to look for the presence of the libs
# in the system and set the LIBRARY and INCLUDE variables properly
#
# either ***_LIBRARIES or ***_ARCHIVE has to be defined for all externals
# in order to successfully link the targets.
#

if (MACOSX)
  find_package (Fuse4x REQUIRED)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${FUSE4X_INCLUDE_DIR})
	set (FUSE_LIBRARIES ${FUSE4X_LIBRARIES}) # just abstract the difference here... they are compatible
else (MACOSX)
  find_package (FUSE REQUIRED)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${FUSE_INCLUDE_DIR})
endif (MACOSX)

if (LIBCURL_BUILTIN)
	include (${CARES_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
	include (${LIBCURL_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
	set (CURL_LIBRARIES "")
else (LIBCURL_BUILTIN)
	find_package (CURL REQUIRED)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${CURL_INCLUDE_DIRS})
endif (LIBCURL_BUILTIN)

if (SQLITE3_BUILTIN)
	include (${SQLITE3_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
	set (SQLITE3_LIBRARY "")
else (SQLITE3_BUILTIN)
	find_package (SQLite3 REQUIRED)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SQLITE3_INCLUDE_DIR})
endif (SQLITE3_BUILTIN)

if (ZLIB_BUILTIN)
	include (${ZLIB_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
	set (ZLIB_LIBRARIES "")
else (ZLIB_BUILTIN)
	find_package (ZLIB REQUIRED)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${ZLIB_INCLUDE_DIRS})
endif (ZLIB_BUILTIN)

if (SPARSEHASH_BUILTIN)
	include (${SPARSEHASH_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SPARSEHASH_BUILTIN_LOCATION}/src)
else (SPARSEHASH_BUILTIN)
	find_package(Sparsehash)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SPARSEHASH_INCLUDE_DIR})
endif (SPARSEHASH_BUILTIN)

if (LEVELDB_BUILTIN)
	include (${LEVELDB_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${LEVELDB_BUILTIN_LOCATION}/src)
else (LEVELDB_BUILTIN)
  find_package(leveldb)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${LEVELDB_INCLUDE_DIR})
endif (LEVELDB_BUILTIN)

if (NOT MACOSX)
	set (HAVE_LIB_RT TRUE)
	set (RT_LIBRARY "rt")
else (NOT MACOSX)
	set (HAVE_LIB_RT FALSE)
	set (RT_LIBRARY "")
endif (NOT MACOSX)

find_package (OpenSSL REQUIRED)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${OPENSSL_INCLUDE_DIR})


#
# set some include and linking stuff
#
set (CVMFS2_LD_FLAGS "${CVMFS2_LD_FLAGS} -ldl -lm")
include_directories (${INCLUDE_DIRECTORIES})

#
# do the job!
#
# for every target you can define a <targetname>_CFLAGS and <targetname>_LD_FLAGS
# variable to pass some target specific build options.
#

if (BUILD_CVMFS OR BUILD_LIBCVMFS)
  include (${MURMUR_LOCATION}/CVMFS-CMakeLists.txt)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${MURMUR_LOCATION}/src)
endif (BUILD_CVMFS OR BUILD_LIBCVMFS)

if (BUILD_CVMFS)

	# create targets with according sources and static link libraries
	add_executable (cvmfs2_debug	${CVMFS2_DEBUG_SOURCES} ${LIBFUSE_ARCHIVE} ${SQLITE3_ARCHIVE} ${MURMUR_ARCHIVE} ${LIBCURL_ARCHIVE} ${CARES_ARCHIVE} ${ZLIB_ARCHIVE})
	add_executable (cvmfs2			${CVMFS2_SOURCES} ${LIBFUSE_ARCHIVE} ${SQLITE3_ARCHIVE} ${MURMUR_ARCHIVE} ${LIBCURL_ARCHIVE} ${CARES_ARCHIVE} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_fsck		${CVMFS_FSCK_SOURCES} ${ZLIB_ARCHIVE})

	if (LIBFUSE_BUILTIN)
		add_dependencies (cvmfs2_debug libfuse) # here it does not matter if libfuse or libfuse4x
		add_dependencies (cvmfs2 libfuse)       # at this point only the appropriate one is defined
	endif (LIBFUSE_BUILTIN)

	if (LIBCURL_BUILTIN)
		add_dependencies (cvmfs2_debug libcares libcurl)
		add_dependencies (cvmfs2 libcares libcurl)
	endif (LIBCURL_BUILTIN)

	if (SQLITE3_BUILTIN)
		add_dependencies (cvmfs2_debug sqlite3)
		add_dependencies (cvmfs2 sqlite3)
	endif (SQLITE3_BUILTIN)

	if (ZLIB_BUILTIN)
		add_dependencies (cvmfs2_debug zlib)
		add_dependencies (cvmfs2 zlib)
		add_dependencies (cvmfs_fsck zlib)
	endif (ZLIB_BUILTIN)

	if (SPARSEHASH_BUILTIN)
		add_dependencies (cvmfs2_debug sparsehash)
		add_dependencies (cvmfs2 sparsehash)
	endif (SPARSEHASH_BUILTIN)

  if (LEVELDB_BUILTIN)
		add_dependencies (cvmfs2_debug libleveldb)
		add_dependencies (cvmfs2 libleveldb)
	endif (LEVELDB_BUILTIN)

  add_dependencies (cvmfs2_debug libmurmur)
  add_dependencies (cvmfs2 libmurmur)

	# set some properties
	set_target_properties (cvmfs2_debug PROPERTIES COMPILE_FLAGS "${CVMFS2_DEBUG_CFLAGS}" LINK_FLAGS "${CVMFS2_DEBUG_LD_FLAGS}")
	set_target_properties (cvmfs2 PROPERTIES COMPILE_FLAGS "${CVMFS2_CFLAGS}" LINK_FLAGS "${CVMFS2_LD_FLAGS}")
	set_target_properties (cvmfs_fsck PROPERTIES COMPILE_FLAGS "${CVMFS_FSCK_CFLAGS}" LINK_FLAGS "${CVMFS_FSCK_LD_FLAGS}")

	# link the stuff (*_LIBRARIES are dynamic link libraries *_archive are static link libraries ... one of them will be empty for each dependency)
	target_link_libraries (cvmfs2_debug		${CVMFS2_DEBUG_LIBS} ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${LEVELDB_LIBRARIES} ${OPENSSL_LIBRARIES} ${RT_LIBRARY} ${FUSE_LIBRARIES} ${LIBFUSE_ARCHIVE} ${SQLITE3_ARCHIVE} ${MURMUR_ARCHIVE} ${LIBCURL_ARCHIVE} ${LEVELDB_ARCHIVE} ${CARES_ARCHIVE} ${ZLIB_ARCHIVE} pthread -ldl -lrt)
	target_link_libraries (cvmfs2			${CVMFS2_LIBS} ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${LEVELDB_LIBRARIES} ${OPENSSL_LIBRARIES} ${RT_LIBRARY} ${FUSE_LIBRARIES} ${LIBFUSE_ARCHIVE} ${SQLITE3_ARCHIVE} ${MURMUR_ARCHIVE} ${LIBCURL_ARCHIVE} ${LEVELDB_ARCHIVE} ${CARES_ARCHIVE} ${ZLIB_ARCHIVE} pthread -ldl -lrt)
	target_link_libraries (cvmfs_fsck		${CVMFS_FSCK_LIBS} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${ZLIB_ARCHIVE} pthread)

endif (BUILD_CVMFS)

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

	add_dependencies (cvmfs_only murmur)
	set (LIBCVMFS_LIBS ${MURMUR_ARCHIVE} ${LIBCVMFS_LIBS} )

	set_target_properties (cvmfs_only PROPERTIES COMPILE_FLAGS "${CVMFS2_CFLAGS}")

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

endif (BUILD_LIBCVMFS)

if (BUILD_SERVER)

	# create targets with according sources and static link libraries
#	add_executable (cvmfs_lscat		${CVMFS_LSCAT_SOURCES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE})
#	add_executable (cvmfs_pull		${CVMFS_PULL_SOURCES} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${CARES_ARCHIVE} ${ZLIB_ARCHIVE})
#	add_executable (cvmfs_scrub		${CVMFS_SCRUB_SOURCES})
	add_executable (cvmfs_sign		${CVMFS_SIGN_SOURCES} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_zpipe		${CVMFS_ZPIPE_SOURCES} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_sync		${CVMFS_SYNC_BIN_SOURCES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE})

	if (LIBCURL_BUILTIN)
    add_dependencies (cvmfs_sync libcares libcurl)
		#add_dependencies (cvmfs_pull libcares libcurl)
	endif (LIBCURL_BUILTIN)

	if (SQLITE3_BUILTIN)
#		add_dependencies (cvmfs_lscat sqlite3)
#		add_dependencies (cvmfs_pull sqlite3)
		add_dependencies (cvmfs_sync sqlite3)
	endif (SQLITE3_BUILTIN)

	if (ZLIB_BUILTIN)
#		add_dependencies (cvmfs_lscat zlib)
#		add_dependencies (cvmfs_pull zlib)
		add_dependencies (cvmfs_sign zlib)
		add_dependencies (cvmfs_zpipe zlib)
		add_dependencies (cvmfs_sync zlib)
	endif (ZLIB_BUILTIN)

	# set some properties
#	set_target_properties (cvmfs_lscat PROPERTIES COMPILE_FLAGS "${CVMFS_LSCAT_CFLAGS}" LINK_FLAGS "${CVMFS_LSCAT_LD_FLAGS}")
#	set_target_properties (cvmfs_pull PROPERTIES COMPILE_FLAGS "${CVMFS_PULL_CFLAGS}" LINK_FLAGS "${CVMFS_PULL_LD_FLAGS}")
#	set_target_properties (cvmfs_scrub PROPERTIES COMPILE_FLAGS "${CVMFS_SCRUB_CFLAGS}" LINK_FLAGS "${CVMFS_SCRUB_LD_FLAGS}")
	set_target_properties (cvmfs_sign PROPERTIES COMPILE_FLAGS "${CVMFS_SIGN_CFLAGS}" LINK_FLAGS "${CVMFS_SIGN_LD_FLAGS}")
	set_target_properties (cvmfs_zpipe PROPERTIES COMPILE_FLAGS "${CVMFS_ZPIPE_CFLAGS}" LINK_FLAGS "${CVMFS_ZPIPE_LD_FLAGS}")
	set_target_properties (cvmfs_sync PROPERTIES COMPILE_FLAGS "${CVMFS_SYNC_BIN_CFLAGS}" LINK_FLAGS "${CVMFS_SYNC_BIN_LD_FLAGS}")

	# link the stuff (*_LIBRARIES are dynamic link libraries)
#	target_link_libraries (cvmfs_lscat		${CVMFS_LSCAT_LIBS} ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE} pthread)
#	target_link_libraries (cvmfs_pull		${CVMFS_PULL_LIBS} ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${RT_LIBRARY} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${CARES_ARCHIVE} ${ZLIB_ARCHIVE} pthread)
#	target_link_libraries (cvmfs_scrub		${CVMFS_SCRUB_LIBS} ${OPENSSL_LIBRARIES})
	target_link_libraries (cvmfs_sign		${CVMFS_SIGN_LIBS} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${ZLIB_ARCHIVE} pthread)
	target_link_libraries (cvmfs_zpipe		${CVMFS_ZPIPE_LIBS} ${ZLIB_LIBRARIES} ${ZLIB_ARCHIVE})
	target_link_libraries (cvmfs_sync	${CVMFS_SYNC_BIN_LIBS} ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES}  ${RT_LIBRARY} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${CARES_ARCHIVE} ${ZLIB_ARCHIVE} pthread -lrt)
endif (BUILD_SERVER)

#
# installation
#

if (BUILD_CVMFS)
	install (
		TARGETS			cvmfs2_debug cvmfs2 cvmfs_fsck
		RUNTIME
		DESTINATION		bin
	)

	install (
		FILES			cvmfs_talk cvmfs_config
		DESTINATION		bin
		PERMISSIONS		OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	)

endif (BUILD_CVMFS)

if (BUILD_LIBCVMFS)
	install (
		FILES			${CMAKE_CURRENT_BINARY_DIR}/libcvmfs.a
		DESTINATION		lib
	)
	install (
		FILES                   libcvmfs.h
		DESTINATION             include
	)
endif (BUILD_LIBCVMFS)

if (BUILD_SERVER)
	install (
#		TARGETS			cvmfs_zpipe cvmfs_sign cvmfs_lscat cvmfs_pull cvmfs_scrub cvmfs_sync
    TARGETS     cvmfs_zpipe cvmfs_sign cvmfs_sync
		RUNTIME
		DESTINATION		bin
	)

	install (
		FILES			cvmfs_server
		DESTINATION		bin
		PERMISSIONS		OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	)
endif (BUILD_SERVER)
