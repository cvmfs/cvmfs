#
# Define the sources needed to compile the different 
# executables in this directory
#

set (CVMFS2_DEBUG_SOURCES 
	smalloc.h smalloc.c 
	log.c log.h 
	tracer.h tracer.cc atomic.h 
	catalog_tree.h catalog_tree.cc 
	sqlite3-duplex.h catalog.h catalog.cc 
	curl-duplex.h fuse-duplex.h 
	signature.h signature.cc 
	lru.h lru.cc 
	hash.h hash.cc 
	cache.h cache.cc 
	talk.h talk.cc 
	compat.h 
	compat_macosx.h 
	monitor.h monitor.cc 
	util.cc util.h 
	zlib-duplex.h compression.h compression.c 
	sha1.h sha1.c 
	md5.h 
	debug.h debug.c 
	http_curl.h http_curl.c 
	cvmfs.h cvmfs.cc)

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
	sqlite3-duplex.h catalog.h catalog.cc 
	util.cc util.h 
	log.c log.h 
	monitor.cc monitor.h 
	cvmfs_sync.cc)

set (CVMFS_CLGCMP_SOURCES
	smalloc.h smalloc.c 
	debug.c debug.h 
	zlib-duplex.h compression.c compression.h 
	catalog.cc catalog.h 
	md5.h 
	sha1.c sha1.h 
	hash.cc hash.h 
	util.cc util.h cvmfs_clgcmp.cc)

set (CVMFS_LSCAT_SOURCES
	smalloc.h smalloc.c 
	debug.c debug.h 
	zlib-duplex.h compression.c compression.h 
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
	zlib-duplex.h compression.h compression.c 
	curl-duplex.h http_curl.h http_curl.c 
	md5.h sha1.h sha1.c 
	hash.h hash.cc 
	util.h util.cc 
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

set (CVMFS2_CFLAGS "${CVMFS2_CFLAGS} -DCVMFS_CLIENT")
set (CVMFS2_DEBUG_CFLAGS "${CVMFS2_DEBUG_CFLAGS} ${CVMFS2_CFLAGS} -O0")
set (CVMFS2_CFLAGS "${CVMFS2_CFLAGS} -DNDEBUGMSG")

set (CVMFS_FSCK_CFLAGS "${CVMFS_FSCK_CFLAGS} -DCVMFS_CLIENT -DNDEBUGMSG")


#
# set properties for configurable libraries
#

if (USE_JEMALLOC)
	set (JEMALLOC_ARCHIVE "${JEMALLOC_BUILTIN_LOCATION}/lib/libjemalloc.a")
	
	add_custom_command (
		OUTPUT				${JEMALLOC_ARCHIVE}
		COMMAND				sh
		ARGS				"${PREBUILD_SCRIPT_LOCATION}/buildJemalloc.sh"
		WORKING_DIRECTORY	${JEMALLOC_BUILTIN_LOCATION}
		COMMENT				"Building jemalloc..."
	)
endif (USE_JEMALLOC)

if (LIBCURL_BUILTIN)
	set (LIBCURL_ARCHIVE "${LIBCURL_BUILTIN_LOCATION}/lib/.libs/libcurl.a")

	add_custom_command (
		OUTPUT				${LIBCURL_ARCHIVE}
		COMMAND				sh
		ARGS				"${PREBUILD_SCRIPT_LOCATION}/buildLibcurl.sh"
		WORKING_DIRECTORY	${LIBCURL_BUILTIN_LOCATION}
		COMMENT				"Building libcurl..."
	)

	set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I${LIBCURL_BUILTIN_LOCATION} -D_BUILT_IN_LIBCURL")
	set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${LIBCURL_BUILTIN_LOCATION} -D_BUILT_IN_LIBCURL")
	set (CURL_LIBRARIES "")
else (LIBCURL_BUILTIN)
	find_package (CURL REQUIRED)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${CURL_INCLUDE_DIRS})
endif (LIBCURL_BUILTIN)

if (SQLITE3_BUILTIN)
	set (SQLITE3_ARCHIVE "${SQLITE3_BUILTIN_LOCATION}/libsqlite3.a")

	add_custom_command (
		OUTPUT				${SQLITE3_ARCHIVE}
		COMMAND				sh
		ARGS				"${PREBUILD_SCRIPT_LOCATION}/buildSqlite3.sh"
		WORKING_DIRECTORY	${SQLITE3_BUILTIN_LOCATION}
		COMMENT				"Building SQLite3..."
	)
	
	set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I${SQLITE3_BUILTIN_LOCATION} -D_BUILT_IN_SQLITE3")
	set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${SQLITE3_BUILTIN_LOCATION} -D_BUILT_IN_SQLITE3")
	set (SQLITE3_LIBRARY "")
else (SQLITE3_BUILTIN)
	find_package (SQLite3 REQUIRED)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${SQLITE3_INCLUDE_DIR})
endif (SQLITE3_BUILTIN)

if (ZLIB_BUILTIN)
	set (ZLIB_ARCHIVE "${ZLIB_BUILTIN_LOCATION}/libz.a")

	add_custom_command (
		OUTPUT				${ZLIB_ARCHIVE}
		COMMAND				sh
		ARGS				"${PREBUILD_SCRIPT_LOCATION}/buildZlib.sh"
		WORKING_DIRECTORY	${ZLIB_BUILTIN_LOCATION}
		COMMENT				"Building zlib..."
	)

	set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I${ZLIB_BUILTIN_LOCATION} -D_BUILT_IN_ZLIB")
	set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I${ZLIB_BUILTIN_LOCATION} -D_BUILT_IN_ZLIB")
	set (ZLIB_LIBRARIES "")
else (ZLIB_BUILTIN)
	find_package (ZLIB REQUIRED)
	set (INCLUDE_DIRECTORIES ${INCLUDE_DIRECTORIES} ${ZLIB_INCLUDE_DIRS})
endif (ZLIB_BUILTIN)

set (CVMFS2_LD_FLAGS "-ldl -lm")

#
# do the job!
#

if (BUILD_CVMFS)
	add_executable (cvmfs2_debug	${CVMFS2_DEBUG_SOURCES} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE} ${JEMALLOC_ARCHIVE})
	add_executable (cvmfs2			${CVMFS2_SOURCES} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE} ${JEMALLOC_ARCHIVE})
	add_executable (cvmfs_fsck		${CVMFS_FSCK_SOURCES} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_proxy_rtt	${CVMFS_PROXY_RTT_SOURCES} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE})
	add_executable (testtracer		${TESTTRACER_SOURCES})
endif (BUILD_CVMFS)
if (BUILD_SERVER)
	add_executable (cvmfs_clgcmp	${CVMFS_CLGCMP_SOURCES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_lscat		${CVMFS_LSCAT_SOURCES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_pull		${CVMFS_PULL_SOURCES} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_scrub		${CVMFS_SCRUB_SOURCES})
	add_executable (cvmfs_sign		${CVMFS_SIGN_SOURCES} ${ZLIB_ARCHIVE})
	add_executable (cvmfs_zpipe		${CVMFS_ZPIPE_SOURCES} ${ZLIB_ARCHIVE})
	#add_SOMETHING (cvmfs_sync.bin	${CVMFS_SYNC_BIN_SOURCES} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE}) ??
endif (BUILD_SERVER)

if (BUILD_CVMFS)
	set_target_properties (cvmfs2_debug PROPERTIES 
								COMPILE_FLAGS	"${CVMFS2_DEBUG_CFLAGS}"
								LINK_FLAGS		"${CVMFS2_DEBUG_LD_FLAGS}  ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE} ${JEMALLOC_ARCHIVE}")
	set_target_properties (cvmfs2 PROPERTIES 
								COMPILE_FLAGS	"${CVMFS2_CFLAGS}"
								LINK_FLAGS		"${CVMFS2_LD_FLAGS} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE} ${JEMALLOC_ARCHIVE}")
	set_target_properties (cvmfs_fsck PROPERTIES 
								COMPILE_FLAGS	"${CVMFS_FSCK_CFLAGS}"
								LINK_FLAGS		"${CVMFS_FSCK_LD_FLAGS} ${ZLIB_ARCHIVE}")
	set_target_properties (cvmfs_proxy_rtt PROPERTIES 
								COMPILE_FLAGS	"${CVMFS_PROXY_RTT_CFLAGS}"
								LINK_FLAGS		"${CVMFS_PROXY_RTT_LD_FLAGS} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE}")
	set_target_properties (testtracer PROPERTIES 
								COMPILE_FLAGS	"${TESTTRACER_CFLAGS}"
								LINK_FLAGS		"${TESTTRACER_LD_FLAGS}")
endif (BUILD_CVMFS)					
if (BUILD_SERVER)
	set_target_properties (cvmfs_clgcmp PROPERTIES 
								COMPILE_FLAGS	"${CVMFS_CLGCMP_CFLAGS}"
								LINK_FLAGS		"${CVMFS_CLGCMP_LD_FLAGS} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE}")
	set_target_properties (cvmfs_lscat PROPERTIES 
								COMPILE_FLAGS	"${CVMFS_LSCAT_CFLAGS}"
								LINK_FLAGS		"${CVMFS_LSCAT_LD_FLAGS} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE}")
	set_target_properties (cvmfs_pull PROPERTIES 
								COMPILE_FLAGS	"${CVMFS_PULL_CFLAGS}"
								LINK_FLAGS		"${CVMFS_PULL_LD_FLAGS} ${SQLITE3_ARCHIVE} ${LIBCURL_ARCHIVE} ${ZLIB_ARCHIVE}")
	set_target_properties (cvmfs_scrub PROPERTIES
								COMPILE_FLAGS	"${CVMFS_SCRUB_CFLAGS}"
								LINK_FLAGS		"${CVMFS_SCRUB_LD_FLAGS}")
	set_target_properties (cvmfs_sign PROPERTIES 
								COMPILE_FLAGS	"${CVMFS_SIGN_CFLAGS}"
								LINK_FLAGS		"${CVMFS_SIGN_LD_FLAGS} ${ZLIB_ARCHIVE}")
	set_target_properties (cvmfs_zpipe PROPERTIES 
								COMPILE_FLAGS	"${CVMFS_ZPIPE_CFLAGS}"
								LINK_FLAGS		"${CVMFS_ZPIPE_LD_FLAGS} ${ZLIB_ARCHIVE}")
	#set_target_properties (cvmfs_sync.bin PROPERTIES
	#							COMPILE_FLAGS	"${CVMFS_SYNC_BIN_CFLAGS}"
	#							LINK_FLAGS		"${CVMFS_SYNC_BIN_LD_FLAGS} ${SQLITE3_ARCHIVE} ${ZLIB_ARCHIVE}") ??
endif (BUILD_SERVER)

if (BUILD_CVMFS)
	target_link_libraries (cvmfs2_debug		${CVMFS2_DEBUG_LIBS} ${PORTABLE_FUSE_LIBRARIES} ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES})
	target_link_libraries (cvmfs2			${CVMFS2_LIBS} ${PORTABLE_FUSE_LIBRARIES} ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES})
	target_link_libraries (cvmfs_fsck		${CVMFS_FSCK_LIBS} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES})
	target_link_libraries (cvmfs_proxy_rtt	${CVMFS_PROXY_RTT_LIBS} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES})
	target_link_libraries (testtracer		${TESTTRACER_LIBS})
endif (BUILD_CVMFS)
if (BUILD_SERVER)
	target_link_libraries (cvmfs_clgcmp		${CVMFS_CLGCMP_LIBS} ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES})
	target_link_libraries (cvmfs_lscat		${CVMFS_LSCAT_LIBS} ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES})
	target_link_libraries (cvmfs_pull		${CVMFS_PULL_LIBS} ${SQLITE3_LIBRARY} ${CURL_LIBRARIES} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES})
	target_link_libraries (cvmfs_scrub		${CVMFS_SCRUB_LIBS} ${OPENSSL_LIBRARIES})
	target_link_libraries (cvmfs_sign		${CVMFS_SIGN_LIBS} ${ZLIB_LIBRARIES} ${OPENSSL_LIBRARIES})
	target_link_libraries (cvmfs_zpipe		${CVMFS_ZPIPE_LIBS} ${ZLIB_LIBRARIES})
	#target_link_libraries (cvmfs_sync.bin	${CVMFS_SYNC_BIN_LIBS} ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES}) ??
endif (BUILD_SERVER)