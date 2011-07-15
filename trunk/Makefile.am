if ! SYSTEM_SQLITE3
  SUBDIR_SQLITE3 = sqlite3
endif
if ! SYSTEM_LIBCURL
  SUBDIR_LIBCURL = libcurl
endif
if ! SYSTEM_LIBFUSE
  SUBDIR_LIBFUSE = libfuse
endif
if ! SYSTEM_ZLIB
  SUBDIR_ZLIB = zlib
endif
if BUILD_KMODULE
  SUBDIR_KMODULE = kernel
endif
SUBDIRS = $(SUBDIR_KMODULE) $(SUBDIR_SQLITE3) $(SUBDIR_ZLIB) $(SUBDIR_LIBCURL) $(SUBDIR_LIBFUSE) jemalloc cvmfs cvmfsd mount replica
EXTRA_DIST = sign-whitelist add-ons rpm doc libcurl libfuse kernel jemalloc zlib mount replica 
docdir = ${datadir}/doc/${PACKAGE}-${VERSION}
dist_doc_DATA = NEWS README ChangeLog AUTHORS INSTALL COPYING FAQ

install-exec-hook:
	test -d "$(DESTDIR)/cvmfs" || mkdir -p "$(DESTDIR)/cvmfs"	

dist-hook:
	rm -f $(distdir)/libcurl/curl-*.tar.gz
	rm -f $(distdir)/libfuse/fuse-*.tar.gz
	rm -f $(distdir)/jemalloc/jemalloc-*.tar.bz2
	rm -f $(distdir)/zlib/zlib-*.tar.gz
	rm -f $(distdir)/kernel/redirfs/redirfs-*.tar.gz
	rm -f $(distdir)/kernel/cvmfsflt/src/taint.sh
	rm -f $(distdir)/kernel/cvmfsflt/src/untaint.sh
