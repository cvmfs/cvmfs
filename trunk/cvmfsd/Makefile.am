SUBDIRS = src
EXTRA_DIST = cvmfsd.initd server.conf
if BUILD_SERVER
dist_bin_SCRIPTS = cvmfs-sync
install-exec-hook:
	test -z "$(DESTDIR)/etc" || mkdir -p "$(DESTDIR)/etc"
	test -z "$(DESTDIR)/etc/init.d" || mkdir -p "$(DESTDIR)/etc/init.d"
	test -z "$(DESTDIR)/etc/cvmfs" || mkdir -p "$(DESTDIR)/etc/cvmfs"
	install -c -m 0644 server.conf "$(DESTDIR)/etc/cvmfs/server.conf"
	install -c -m 0755 cvmfsd.initd "$(DESTDIR)/etc/init.d/cvmfsd" 
endif
