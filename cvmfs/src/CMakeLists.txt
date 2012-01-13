#
# Define the sources needed to compile the different 
# executables in this directory
#

set (CVMFS2_DEBUG_SOURCES 
	smalloc.h smalloc.c 
	log.c log.h
	logging.cc logging.h
	tracer.h tracer.cc atomic.h 
	sqlite3-duplex.h
	curl-duplex.h fuse-duplex.h 
	signature.h signature.cc 
	lru.h lru.cc 
	hash.h hash.cc 
	cache.h cache.cc 
	talk.h talk.cc 
	
	catalog.h catalog.cc
	catalog_tree.h catalog_tree.cc
	compat.h 
	compat_macosx.h 
	monitor.h monitor.cc 
	util.cc util.h 
	zlib-duplex.h compression.h compression.c 
	sha1.h sha1.c 
	md5.h 
	debug.h debug.c 
	http_curl.h http_curl.c 
	lru_cache.h
	inode_cache.h inode_cache.cc
	path_cache.h path_cache.cc
	md5path_cache.h md5path_cache.cc
	cvmfs.h cvmfs.cc
	
	catalog_queries.h           catalog_queries.cc
	thread_safe.h 
	Catalog.h                   Catalog.cc
	AbstractCatalogManager.h    AbstractCatalogManager.cc
	RemoteCatalogManager.h      RemoteCatalogManager.cc
	DirectoryEntry.h
	
)

set (CVMFS2_SOURCES ${CVMFS2_DEBUG_SOURCES})

set (CVMFS_FSCK_SOURCES
	smalloc.h smalloc.c 
	debug.c debug.h 
	md5.h 
	sha1.c sha1.h 
	zlib-duplex.h compression.c compression.h 
	hash.cc hash.h 
	util.cc util.h cvmfs_fsck.cc)

set (CVMFS_PROXY_RTT_SOURCES smalloc.c smalloc.h cvmfs_proxy_rtt.c)

set (TESTTRACER_SOURCES atomic.h tracer.cc tracer.h test_tracer.cc)

set (CVMFS_SYNC_BIN_SOURCES
	smalloc.h smalloc.c 
	debug.h debug.c 
	zlib-duplex.h compression.h compression.c 
	md5.h sha1.h sha1.c hash.h hash.cc 
	catalog_tree.h catalog_tree.cc
	sqlite3-duplex.h 
	util.cc util.h 
	log.c log.h 
	logging.cc logging.h
	monitor.cc monitor.h 
	cvmfs_sync.h cvmfs_sync.cc
	cvmfs_sync_recursion.h cvmfs_sync_recursion.cc
	SyncItem.h SyncItem.cc
	SyncUnion.h SyncUnion.cc
	SyncUnionAufs.h SyncUnionAufs.cc
	SyncMediator.h SyncMediator.cc
	
	
	catalog_queries.h           catalog_queries.cc
	thread_safe.h

	Catalog.h                   Catalog.cc
	WritableCatalog.h           WritableCatalog.cc

	AbstractCatalogManager.h    AbstractCatalogManager.cc
	WritableCatalogManager.h    WritableCatalogManager.cc
	
	DirectoryEntry.h)

set (CVMFS_CLGCMP_SOURCES
	smalloc.h smalloc.c 
	debug.c debug.h 
	zlib-duplex.h compression.c compression.h 
	catalog_tree.h catalog_tree.cc
	catalog.cc catalog.h 
	md5.h 
	sha1.c sha1.h 
	hash.cc hash.h 
	util.cc util.h cvmfs_clgcmp.cc)

set (CVMFS_LSCAT_SOURCES
	smalloc.h smalloc.c 
	debug.c debug.h 
	zlib-duplex.h compression.c compression.h 
	catalog_tree.h catalog_tree.cc
	sqlite3-duplex.h catalog.cc catalog.h 
	md5.h sha1.c sha1.h 
	hash.cc hash.h 
	util.cc util.h cvmfs_lscat.cc)

set (CVMFS_SIGN_SOURCES
	smalloc.h smalloc.c 
	debug.c debug.h 
	zlib-duplex.h compression.h compression.c 
	md5.h 
	sha1.c sha1.h 
	hash.cc hash.h 
	util.h util.cc 
	signature.cc signature.h cvmfs_sign.cc)

set (CVMFS_PULL_SOURCES
	smalloc.h smalloc.c 
	debug.h debug.c
	log.h log.c 
	zlib-duplex.h compression.h compression.c 
	curl-duplex.h http_curl.h http_curl.c 
	md5.h sha1.h sha1.c 
	hash.h hash.cc 
	util.h util.cc 
	catalog_tree.h catalog_tree.cc
	sqlite3-duplex.h catalog.h catalog.cc 
	signature.h signature.cc 
	cvmfs_pull.cc)

set (CVMFS_SCRUB_SOURCES
	smalloc.h smalloc.c 
	debug.h debug.c 
	md5.h sha1.h sha1.c 
	hash.h hash.cc 
	util.h util.cc 
	cvmfs_scrub.cc)

set (CVMFS_ZPIPE_SOURCES zlib-duplex.h zpipe.c)

#
# configure some compiler flags for proper build
#

set (CVMFS2_CFLAGS "${CVMFS2_CFLAGS} -DCVMFS_CLIENT -fexceptions")
set (CVMFS2_DEBUG_CFLAGS "${CVMFS2_DEBUG_CFLAGS} ${CVMFS2_CFLAGS} -O0")
set (CVMFS2_CFLAGS "${CVMFS2_CFLAGS} -DNDEBUGMSG")

set (CVMFS_FSCK_CFLAGS "${CVMFS_FSCK_CFLAGS} -DCVMFS_CLIENT -DNDEBUGMSG")

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

if (USE_JEMALLOC)
	include (${JEMALLOC_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
endif (USE_JEMALLOC)

if (MACOSX)
	if (LIBFUSE_BUILTIN)
		include (${LIBFUSE4X_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
		set (FUSE_LIBRARIES "")
	else (LIBFUSE_BUILTIN)
		find_package (Fuse4x REQUIRED)
		set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${FUSE4X_INCLUDE_DIR})
		set (FUSE_LIBRARIES ${FUSE4X_LIBRARIES}) # just abstract the difference here... they are compatible
	endif (LIBFUSE_BUILTIN)
else (MACOSX)
	if (LIBFUSE_BUILTIN)
		include (${LIBFUSE_BUILTIN_LOCATION}/CVMFS-CMakeLists.txt)
		set (FUSE_LIBRARIES "")
	else (LIBFUSE_BUILTIN)	
		find_package (FUSE REQUIRED)
		set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${FUSE_INCLUDE_DIR})
	endif (LIBFUSE_BUILTIN)
endif (MACOSX)

if (LIBCURL_BUILTIN)
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

if (NOT MACOSX)
	set (HAVE_LIB_RT TRUE)
	set (RT_LIBRARY "rt")
else (NOT MACOSX)
	set (HAVE_LIB_RT FALSE)
	set (RT_LIBRARY "")
endif (NOT MACOSX)

find_package (OpenSSL REQUIRED)
set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${OPENSSL_INCLUDE_DIR})

find_package (OpenMP REQUIRED)
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")

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


if (BUILD_CVMFS)

	# create targets with according sources and static link libraries
	add_executable (cvmfs2_debug	${CVMFS2_DEBUG_SOURCES} ${LIBFUSE_ARCHIVE} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE} ${JEMALLOC_ARCHIVE})
	add_executable (cvmfs2			${CVMFS2_SOURCES} ${LIBFUSE_ARCHIVE} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE} ${JEMALLOC_ARCHIVE})
	add_executable (cvmfs_fsck		${CVMFS_FSCK_SOURCES} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_proxy_rtt	${CVMFS_PROXY_RTT_SOURCES} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE})
	add_executable (testtracer		${TESTTRACER_SOURCES})
		
	if (USE_JEMALLOC)
		add_dependencies (cvmfs2_debug jemalloc)
		add_dependencies (cvmfs2 jemalloc)
	endif (USE_JEMALLOC)

	if (LIBFUSE_BUILTIN)
		add_dependencies (cvmfs2_debug libfuse) # here it does not matter if libfuse or libfuse4x
		add_dependencies (cvmfs2 libfuse)       # at this point only the appropriate one is defined
	endif (LIBFUSE_BUILTIN)
	
	if (LIBCURL_BUILTIN)
		add_dependencies (cvmfs2_debug libcurl)
		add_dependencies (cvmfs2 libcurl)
		add_dependencies (cvmfs_proxy_rtt libcurl)
	endif (LIBCURL_BUILTIN)
	
	if (SQLITE3_BUILTIN)
		add_dependencies (cvmfs2_debug sqlite3)
		add_dependencies (cvmfs2 sqlite3)
	endif (SQLITE3_BUILTIN)
	
	if (ZLIB_BUILTIN)
		add_dependencies (cvmfs2_debug zlib)
		add_dependencies (cvmfs2 zlib)
		add_dependencies (cvmfs_fsck zlib)
		add_dependencies (cvmfs_proxy_rtt zlib)
	endif (ZLIB_BUILTIN)
	
	if (SPARSEHASH_BUILTIN)
		add_dependencies (cvmfs2_debug sparsehash)
		add_dependencies (cvmfs2 sparsehash)
	endif (SPARSEHASH_BUILTIN)
	
	# set some properties
	set_target_properties (cvmfs2_debug PROPERTIES COMPILE_FLAGS "${CVMFS2_DEBUG_CFLAGS}" LINK_FLAGS "${CVMFS2_DEBUG_LD_FLAGS}")
	set_target_properties (cvmfs2 PROPERTIES COMPILE_FLAGS "${CVMFS2_CFLAGS}" LINK_FLAGS "${CVMFS2_LD_FLAGS}")
	set_target_properties (cvmfs_fsck PROPERTIES COMPILE_FLAGS "${CVMFS_FSCK_CFLAGS}" LINK_FLAGS "${CVMFS_FSCK_LD_FLAGS}")
	set_target_properties (cvmfs_proxy_rtt PROPERTIES COMPILE_FLAGS "${CVMFS_PROXY_RTT_CFLAGS}" LINK_FLAGS "${CVMFS_PROXY_RTT_LD_FLAGS}")
	set_target_properties (testtracer PROPERTIES COMPILE_FLAGS "${TESTTRACER_CFLAGS}" LINK_FLAGS "${TESTTRACER_LD_FLAGS}")
	
	# link the stuff (*_LIBRARIES are dynamic link libraries *_archive are static link libraries ... one of them will be empty for each dependency)
	target_link_libraries (cvmfs2_debug		${CVMFS2_DEBUG_LIBS} ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${RT_LIBRARY} ${FUSE_LIBRARIES} ${LIBFUSE_ARCHIVE} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE} ${JEMALLOC_ARCHIVE} pthread)
	target_link_libraries (cvmfs2			${CVMFS2_LIBS} ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${RT_LIBRARY} ${FUSE_LIBRARIES} ${LIBFUSE_ARCHIVE} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE} ${JEMALLOC_ARCHIVE} pthread)
	target_link_libraries (cvmfs_fsck		${CVMFS_FSCK_LIBS} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${ZLIB_ARCHIVE} pthread)
	target_link_libraries (cvmfs_proxy_rtt	${CVMFS_PROXY_RTT_LIBS} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${RT_LIBRARY} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE})
	target_link_libraries (testtracer		${TESTTRACER_LIBS} pthread)
	
endif (BUILD_CVMFS)

if (BUILD_SERVER)

	# create targets with according sources and static link libraries
	add_executable (cvmfs_clgcmp	${CVMFS_CLGCMP_SOURCES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_lscat		${CVMFS_LSCAT_SOURCES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_pull		${CVMFS_PULL_SOURCES} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_scrub		${CVMFS_SCRUB_SOURCES})
	add_executable (cvmfs_sign		${CVMFS_SIGN_SOURCES} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_zpipe		${CVMFS_ZPIPE_SOURCES} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_sync.bin	${CVMFS_SYNC_BIN_SOURCES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE})
	
	if (LIBCURL_BUILTIN)
		add_dependencies (cvmfs_pull libcurl)
	endif (LIBCURL_BUILTIN)
	
	if (SQLITE3_BUILTIN)
		add_dependencies (cvmfs_clgcmp sqlite3)
		add_dependencies (cvmfs_lscat sqlite3)
		add_dependencies (cvmfs_pull sqlite3)
		add_dependencies (cvmfs_sync.bin sqlite3)
	endif (SQLITE3_BUILTIN)
	
	if (ZLIB_BUILTIN)
		add_dependencies (cvmfs_clgcmp zlib)
		add_dependencies (cvmfs_lscat zlib)
		add_dependencies (cvmfs_pull zlib)
		add_dependencies (cvmfs_sign zlib)
		add_dependencies (cvmfs_zpipe zlib)
		add_dependencies (cvmfs_sync.bin zlib)
	endif (ZLIB_BUILTIN)
	
	# set some properties
	set_target_properties (cvmfs_clgcmp PROPERTIES COMPILE_FLAGS "${CVMFS_CLGCMP_CFLAGS}" LINK_FLAGS "${CVMFS_CLGCMP_LD_FLAGS}")
	set_target_properties (cvmfs_lscat PROPERTIES COMPILE_FLAGS "${CVMFS_LSCAT_CFLAGS}" LINK_FLAGS "${CVMFS_LSCAT_LD_FLAGS}")
	set_target_properties (cvmfs_pull PROPERTIES COMPILE_FLAGS "${CVMFS_PULL_CFLAGS}" LINK_FLAGS "${CVMFS_PULL_LD_FLAGS}")
	set_target_properties (cvmfs_scrub PROPERTIES COMPILE_FLAGS "${CVMFS_SCRUB_CFLAGS}" LINK_FLAGS "${CVMFS_SCRUB_LD_FLAGS}")
	set_target_properties (cvmfs_sign PROPERTIES COMPILE_FLAGS "${CVMFS_SIGN_CFLAGS}" LINK_FLAGS "${CVMFS_SIGN_LD_FLAGS}")
	set_target_properties (cvmfs_zpipe PROPERTIES COMPILE_FLAGS "${CVMFS_ZPIPE_CFLAGS}" LINK_FLAGS "${CVMFS_ZPIPE_LD_FLAGS}")
	set_target_properties (cvmfs_sync.bin PROPERTIES COMPILE_FLAGS "${CVMFS_SYNC_BIN_CFLAGS}" LINK_FLAGS "${CVMFS_SYNC_BIN_LD_FLAGS}")
	
	# link the stuff (*_LIBRARIES are dynamic link libraries)
	target_link_libraries (cvmfs_clgcmp		${CVMFS_CLGCMP_LIBS} ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE} pthread)
	target_link_libraries (cvmfs_lscat		${CVMFS_LSCAT_LIBS} ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE} pthread)
	target_link_libraries (cvmfs_pull		${CVMFS_PULL_LIBS} ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${RT_LIBRARY} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE} pthread)
	target_link_libraries (cvmfs_scrub		${CVMFS_SCRUB_LIBS} ${OPENSSL_LIBRARIES})
	target_link_libraries (cvmfs_sign		${CVMFS_SIGN_LIBS} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES} ${ZLIB_ARCHIVE})
	target_link_libraries (cvmfs_zpipe		${CVMFS_ZPIPE_LIBS} ${ZLIB_LIBRARIES} ${ZLIB_ARCHIVE})
	target_link_libraries (cvmfs_sync.bin	${CVMFS_SYNC_BIN_LIBS} ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES} ${SQLITE3_ARCHIVE} ${OPENSSL_LIBRARIES} ${ZLIB_ARCHIVE} ${RT_LIBRARY} pthread)
endif (BUILD_SERVER)

# 
# installation
#

if (BUILD_CVMFS)
	install (
		TARGETS			cvmfs2_debug cvmfs2 cvmfs_fsck cvmfs_proxy_rtt
		RUNTIME
		DESTINATION		bin
	)
	
	install (	
		FILES			cvmfs-talk cvmfs_config
		DESTINATION		bin
		PERMISSIONS		OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	)
endif (BUILD_CVMFS)
if (BUILD_SERVER)
	install (
		TARGETS			cvmfs_zpipe cvmfs_sign cvmfs_clgcmp cvmfs_lscat cvmfs_pull cvmfs_scrub cvmfs_sync.bin
		RUNTIME
		DESTINATION		bin
	)

	install (
		FILES			cvmfs_mkkey cvmfs_decrypt cvmfs_unsign cvmfs_server
		DESTINATION		bin
		PERMISSIONS		OWNER_READ OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	)
endif (BUILD_SERVER)
