bin_PROGRAMS = 
dist_bin_SCRIPTS =
if BUILD_CVMFS
bin_PROGRAMS += cvmfs2 cvmfs2_debug cvmfs_fsck cvmfs_proxy_rtt 
dist_bin_SCRIPTS += cvmfs-talk cvmfs_config
noinst_PROGRAMS = testtracer
endif
if BUILD_SERVER
bin_PROGRAMS += cvmfs_zpipe cvmfs_sync.bin cvmfs_sign cvmfs_clgcmp cvmfs_lscat cvmfs_pull cvmfs_scrub
dist_bin_SCRIPTS += cvmfs_mkkey cvmfs_decrypt cvmfs_unsign cvmfs_server
endif


cvmfs2_debug_SOURCES =
cvmfs2_SOURCES =
cvmfs_fsck_SOURCES =
cvmfs_proxy_rtt_SOURCES =
testtracer_SOURCES =
cvmfs_sync_bin_SOURCES =
cvmfs_clgcmp_SOURCES =
cvmfs_zpipe_SOURCES =
cvmfs_sign_SOURCES =
cvmfs_lscat_SOURCES =
cvmfs_pull_SOURCES =
cvmfs_scrub_SOURCES =

if ! SYSTEM_SQLITE3
cvmfs2_debug_SOURCES += ../../sqlite3/src/sqlite3.h
cvmfs_sync_bin_SOURCES += ../../sqlite3/src/sqlite3.h
cvmfs_clgcmp_SOURCES += ../../sqlite3/src/sqlite3.h
cvmfs_lscat_SOURCES += ../../sqlite3/src/sqlite3.h
cvmfs_pull_SOURCES += ../../sqlite3/src/sqlite3.h
endif

if ! SYSTEM_LIBCURL
cvmfs2_debug_SOURCES += ../../libcurl/src/include/curl/curl.h
cvmfs_pull_SOURCES += ../../libcurl/src/include/curl/curl.h
endif

if ! SYSTEM_LIBFUSE
cvmfs2_debug_SOURCES += ../../libfuse/src/include/fuse.h
cvmfs2_debug_SOURCES += ../../libfuse/src/include/fuse_opt.h
endif

if ! SYSTEM_ZLIB
cvmfs2_debug_SOURCES += ../../zlib/src/zlib.h
cvmfs_pull_SOURCES += ../../zlib/src/zlib.h
cvmfs_sync_bin_SOURCES += ../../zlib/src/zlib.h
cvmfs_fsck_SOURCES += ../../zlib/src/zlib.h
cvmfs_clgcmp_SOURCES += ../../zlib/src/zlib.h
cvmfs_lscat_SOURCES += ../../zlib/src/zlib.h
cvmfs_sign_SOURCES += ../../zlib/src/zlib.h
cvmfs_zpipe_SOURCES += ../../zlib/src/zlib.h
cvmfs_proxy_rtt_SOURCES += ../../zlib/src/zlib.h
endif

cvmfs2_debug_SOURCES += smalloc.h smalloc.c \
   log.c log.h \
   tracer.h tracer.cc atomic.h \
   catalog_tree.h catalog_tree.cc \
   sqlite3-duplex.h catalog.h catalog.cc \
   curl-duplex.h fuse-duplex.h \
   signature.h signature.cc \
   lru.h lru.cc \
   hash.h hash.cc \
   cache.h cache.cc \
   talk.h talk.cc \
   monitor.h monitor.cc \
   util.cc util.h \
   zlib-duplex.h compression.h compression.c \
   sha1.h sha1.c \
   md5.h \
   debug.h debug.c \
   http_curl.h http_curl.c \
   cvmfs.h cvmfs.cc
cvmfs2_SOURCES += $(cvmfs2_debug_SOURCES)
cvmfs_fsck_SOURCES += smalloc.h smalloc.c \
   debug.c debug.h \
   md5.h \
   sha1.c sha1.h \
   zlib-duplex.h compression.c compression.h \
   hash.cc hash.h \
   util.cc util.h cvmfs_fsck.cc
cvmfs_proxy_rtt_SOURCES += smalloc.c smalloc.h cvmfs_proxy_rtt.c
testtracer_SOURCES += atomic.h tracer.cc tracer.h test_tracer.cc
cvmfs_sync_bin_SOURCES += smalloc.h smalloc.c \
   debug.h debug.c \
   zlib-duplex.h compression.h compression.c \
   md5.h sha1.h sha1.c hash.h hash.cc \
   sqlite3-duplex.h catalog.h catalog.cc \
   util.cc util.h \
   log.c log.h \
   monitor.cc monitor.h \
   cvmfs_sync.cc
cvmfs_clgcmp_SOURCES += smalloc.h smalloc.c \
   debug.c debug.h \
   zlib-duplex.h compression.c compression.h \
   catalog.cc catalog.h \
   md5.h \
   sha1.c sha1.h \
   hash.cc hash.h \
   util.cc util.h cvmfs_clgcmp.cc
cvmfs_lscat_SOURCES += smalloc.h smalloc.c \
   debug.c debug.h \
   zlib-duplex.h compression.c compression.h \
   sqlite3-duplex.h catalog.cc catalog.h \
   md5.h sha1.c sha1.h \
   hash.cc hash.h \
   util.cc util.h cvmfs_lscat.cc
cvmfs_sign_SOURCES += smalloc.h smalloc.c \
   debug.c debug.h \
   zlib-duplex.h compression.h compression.c \
   md5.h \
   sha1.c sha1.h \
   hash.cc hash.h \
   util.h util.cc \
   signature.cc signature.h cvmfs_sign.cc
cvmfs_pull_SOURCES += smalloc.h smalloc.c \
   debug.h debug.c \
   zlib-duplex.h compression.h compression.c \
   curl-duplex.h http_curl.h http_curl.c \
   md5.h sha1.h sha1.c \
   hash.h hash.cc \
   util.h util.cc \
   sqlite3-duplex.h catalog.h catalog.cc \
   signature.h signature.cc \
   cvmfs_pull.cc
cvmfs_scrub_SOURCES += smalloc.h smalloc.c \
   debug.h debug.c \
   md5.h sha1.h sha1.c \
   hash.h hash.cc \
   util.h util.cc \
   cvmfs_scrub.cc
cvmfs_zpipe_SOURCES += zlib-duplex.h zpipe.c


cvmfs2_debug_LDADD =
cvmfs2_LDADD =
cvmfs_fsck_LDADD =
cvmfs_proxy_rtt_LDADD =
testtracer_LDADD =
cvmfs_sync_bin_LDADD =
cvmfs_clgcmp_LDADD =
cvmfs_zpipe_LDADD =
cvmfs_sign_LDADD =
cvmfs_lscat_LDADD =
cvmfs_pull_LDADD =
cvmfs_scrub_LDADD =

if ! SYSTEM_SQLITE3
cvmfs2_debug_LDADD += ../../sqlite3/src/libsqlite3.a
cvmfs2_LDADD += ../../sqlite3/src/libsqlite3.a
cvmfs_sync_bin_LDADD += ../../sqlite3/src/libsqlite3.a
cvmfs_clgcmp_LDADD += ../../sqlite3/src/libsqlite3.a
cvmfs_lscat_LDADD += ../../sqlite3/src/libsqlite3.a
cvmfs_pull_LDADD += ../../sqlite3/src/libsqlite3.a
endif

if ! SYSTEM_LIBCURL
LDFLAGS_CURL = ../../libcurl/src/lib/.libs/libcurl.a $(Z_LIBS) -lrt 
cvmfs2_debug_LDADD += $(LDFLAGS_CURL)
cvmfs2_LDADD += $(LDFLAGS_CURL)
cvmfs_proxy_rtt_LDADD += $(LDFLAGS_CURL)
cvmfs_pull_LDADD += $(LDFLAGS_CURL)
endif

if ! SYSTEM_LIBFUSE
LDFLAGS_FUSE = ../../libfuse/src/lib/.libs/libfuse.a -lrt
cvmfs2_debug_LDADD += $(LDFLAGS_FUSE)
cvmfs2_LDADD += $(LDFLAGS_FUSE)
endif

if ! SYSTEM_ZLIB
LDFLAGS_ZLIB = ../../zlib/src/libz.a
cvmfs2_debug_LDADD += $(LDFLAGS_ZLIB)
cvmfs2_LDADD += $(LDFLAGS_ZLIB)
cvmfs_pull_LDADD += $(LDFLAGS_ZLIB)
cvmfs_sync_bin_LDADD += $(LDFLAGS_ZLIB)
cvmfs_fsck_LDADD += $(LDFLAGS_ZLIB)
cvmfs_clgcmp_LDADD += $(LDFLAGS_ZLIB)
cvmfs_lscat_LDADD += $(LDFLAGS_ZLIB)
cvmfs_sign_LDADD += $(LDFLAGS_ZLIB)
cvmfs_zpipe_LDADD += $(LDFLAGS_ZLIB)
cvmfs_proxy_rtt_LDADD += $(LDFLAGS_ZLIB)
endif


CFLAGS_CVMFS2 =
if ! SYSTEM_SQLITE3
CFLAGS_CVMFS2 += -I../../sqlite3/src
endif
if ! SYSTEM_LIBCURL
CFLAGS_CVMFS2 += -I../../libcurl/src/include
endif
if ! SYSTEM_LIBFUSE
CFLAGS_CVMFS2 += -I../../libfuse/src/include
endif
if ! SYSTEM_ZLIB
CFLAGS_CVMFS2 += -I../../zlib/src
endif
cvmfs2_debug_LDADD += ../../jemalloc/src/lib/libjemalloc.a
cvmfs2_LDADD += ../../jemalloc/src/lib/libjemalloc.a
CFLAGS_CVMFS2 += $(PTHREAD_CFLAGS) $(CURL_CFLAGS) $(SQLITE_CFLAGS) $(OPENSSL_CFLAGS) $(FUSE_CFLAGS) -DCVMFS_CLIENT
LDFLAGS_CVMFS2 = $(CVMFS_LIBS) $(PTHREAD_CFLAGS) $(PTHREAD_LIBS) $(CURL_LIBS) $(SQLITE_LIBS) $(OPENSSL_LIBS) $(FUSE_LIBS) $(Z_LIBS) -lrt -rdynamic 
cvmfs2_debug_CXXFLAGS = $(CVMFS_CXXFLAGS) $(CFLAGS_CVMFS2) -O0
cvmfs2_debug_CFLAGS   = $(CVMFS_CFLAGS) $(CFLAGS_CVMFS2) -O0
cvmfs2_debug_LDFLAGS = $(LDFLAGS_CVMFS2)
cvmfs2_CXXFLAGS = $(CVMFS_CXXFLAGS) $(CFLAGS_CVMFS2) -DNDEBUGMSG
cvmfs2_CFLAGS   = $(CVMFS_CFLAGS) $(CFLAGS_CVMFS2) -DNDEBUGMSG
cvmfs2_LDFLAGS = $(LDFLAGS_CVMFS2)


CFLAGS_FSCK =
if ! SYSTEM_SQLITE3
CFLAGS_FSCK += -I../../sqlite3/src
endif
if ! SYSTEM_ZLIB
CFLAGS_FSCK += -I../../zlib/src
endif
CFLAGS_FSCK += $(PTHREAD_CFLAGS) $(OPENSSL_CFLAGS) -DCVMFS_CLIENT -DNDEBUGMSG
cvmfs_fsck_CXXFLAGS = $(CVMFS_CXXFLAGS) $(CFLAGS_FSCK)
cvmfs_fsck_CFLAGS = $(CVMFS_CXFLAGS) $(CFLAGS_FSCK)
cvmfs_fsck_LDFLAGS = $(CVMFS_LIBS) $(PTHREAD_CFLAGS) $(OPENSSL_LIBS) $(PTHREAD_LIBS) $(SQLITE_LIBS) $(Z_LIBS)


CFLAGS_PROXYRTT = 
if ! SYSTEM_LIBCURL
CFLAGS_PROXYRTT += -I../../libcurl/src/include
endif
CFLAGS_PROXYRTT += $(CURL_CFLAGS) -DCVMFS_CLIENT -DNDEBUGMSG
cvmfs_proxy_rtt_CFLAGS = $(CVMFS_CFLAGS) $(CFLAGS_PROXYRTT)
cvmfs_proxy_rtt_LDFLAGS = $(CVMFS_LIBS) $(CURL_LIBS)


testtracer_CXXFLAGS = $(CVMFS_CXXFLAGS) $(PTHREAD_CFLAGS)
testtracer_LDFLAGS  = $(CVMFS_LIBS) $(PTHREAD_CFLAGS) $(PTHREAD_LIBS)


CFLAGS_SYNC =
if ! SYSTEM_SQLITE3
CFLAGS_SYNC += -I../../sqlite3/src 
endif
if ! SYSTEM_ZLIB
CFLAGS_SYNC += -I../../zlib/src
endif
CFLAGS_SYNC += $(PTHREAD_CFLAGS) $(OPENMP_CFLAGS) $(SQLITE_CFLAGS) $(OPENSSL_CFLAGS) -DNDEBUGMSG 
cvmfs_sync_bin_CXXFLAGS = $(CVMFS_CXXFLAGS) $(CFLAGS_SYNC)
cvmfs_sync_bin_CFLAGS = $(CVMFS_CFLAGS) $(CFLAGS_SYNC)
cvmfs_sync_bin_LDFLAGS = $(CVMFS_LIBS) $(PTHREAD_CFLAGS) $(PTHREAD_LIBS) $(SQLITE_LIBS) $(OPENSSL_LIBS) $(Z_LIBS) $(GOMP_LIBS)


CFLAGS_CLGCMP =
if ! SYSTEM_SQLITE3
CFLAGS_CLGCMP += -I../../sqlite3/src
endif
if ! SYSTEM_ZLIB
CFLAGS_CLGCMP += -I../../zlib/src
endif
CFLAGS_CLGCMP += $(SQLITE_CFLAGS) $(PTHREAD_CFLAGS) $(OPENSSL_CFLAGS) -DNDEBUGMSG
cvmfs_clgcmp_CXXFLAGS = $(CVMFS_CXXFLAGS) $(CFLAGS_CLGCMP)
cvmfs_clgcmp_CFLAGS = $(CVMFS_CFLAGS) $(CFLAGS_CLGCMP)
cvmfs_clgcmp_LDFLAGS = $(CVMFS_LIBS) $(PTHREAD_CFLAGS) $(PTHREAD_LIBS) $(OPENSSL_LIBS) $(SQLITE_LIBS) $(Z_LIBS)


CFLAGS_LSCAT =
if ! SYSTEM_SQLITE3
CFLAGS_LSCAT += -I../../sqlite3/src
endif
if ! SYSTEM_ZLIB
CFLAGS_LSCAT += -I../../zlib/src
endif
CFLAGS_LSCAT += $(SQLITE_CFLAGS) $(PTHREAD_CFLAGS) $(OPENSSL_CFLAGS) -DNDEBUGMSG
cvmfs_lscat_CXXFLAGS = $(CVMFS_CXXFLAGS) $(CFLAGS_LSCAT)
cvmfs_lscat_CFLAGS = $(CVMFS_CFLAGS) $(CFLAGS_LSCAT)
cvmfs_lscat_LDFLAGS = $(CVMFS_LIBS) $(PTHREAD_CFLAGS) $(PTHREAD_LIBS) $(OPENSSL_LIBS) $(SQLITE_LIBS) $(Z_LIBS)

CFLAGS_SIGN =
if ! SYSTEM_ZLIB
CFLAGS_SIGN += -I../../zlib/src
endif
CFLAGS_SIGN += $(SQLITE_CFLAGS) $(PTHREAD_CFLAGS) $(OPENSSL_CFLAGS) -DNDEBUGMSG
cvmfs_sign_CXXFLAGS = $(CVMFS_CXXFLAGS) $(CFLAGS_SIGN)
cvmfs_sign_CFLAGS = $(CVMFS_CFLAGS) $(CFLAGS_SIGN)
cvmfs_sign_LDFLAGS = $(CVMFS_LIBS) $(PTHREAD_CFLAGS) $(PTHREAD_LIBS) $(SQLITE_LIBS) $(Z_LIBS) $(OPENSSL_LIBS)

CFLAGS_PULL =
if ! SYSTEM_SQLITE3
CFLAGS_PULL += -I../../sqlite3/src
endif
if ! SYSTEM_LIBCURL
CFLAGS_PULL += -I../../libcurl/src/include
endif
if ! SYSTEM_ZLIB
CFLAGS_PULL += -I../../zlib/src
endif
CFLAGS_PULL += $(SQLITE_CFLAGS) $(PTHREAD_CFLAGS) $(OPENSSL_CFLAGS) -DNDEBUGMSG  
cvmfs_pull_CXXFLAGS = $(CVMFS_CXXFLAGS) $(CFLAGS_PULL)
cvmfs_pull_CFLAGS = $(CVMFS_CFLAGS) $(CFLAGS_PULL)
cvmfs_pull_LDFLAGS = $(CVMFS_LIBS) $(PTHREAD_CFLAGS) $(PTHREAD_LIBS) $(OPENSSL_LIBS) $(SQLITE_LIBS) $(Z_LIBS)


CFLAGS_SCRUB = $(OPENSSL_CFLAGS) -DNDEBUGMSG
cvmfs_scrub_CXXFLAGS = $(CVMFS_CXXFLAGS) $(CFLAGS_SCRUB)
cvmfs_scrub_CFLAGS = $(CVMFS_CFLAGS) $(CFLAGS_SCRUB)
cvmfs_scrub_LDFLAGS = $(CVMFS_LIBS) $(OPENSSL_LIBS)


CFLAGS_ZPIPE =
if ! SYSTEM_ZLIB
CFLAGS_ZPIPE += -I../../zlib/src
endif
cvmfs_zpipe_CFLAGS = $(CVMFS_CFLAGS) $(CFLAGS_ZPIPE)
cvmfs_zpipe_LDFLAGS = $(CVMFS_LIBS) $(Z_LIBS)
