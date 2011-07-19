if BUILD_SERVER
dist_bin_SCRIPTS = cvmfs_snapshot 
install-exec-hook:
	test -z "$(DESTDIR)/etc" || mkdir -p "$(DESTDIR)/etc"
	test -z "$(DESTDIR)/etc/cvmfs" || mkdir -p "$(DESTDIR)/etc/cvmfs"
	test -z "$(DESTDIR)/etc/cvmfs/cgi-bin" || mkdir -p "$(DESTDIR)/etc/cvmfs/cgi-bin"
	install -c replica.cgi "$(DESTDIR)/etc/cvmfs/cgi-bin/replica.cgi"
	install -c -m 0644 etc.cvmfs.replica.conf "$(DESTDIR)/etc/cvmfs/replica.conf"
	install -c -m 0644 etc.httpd.conf.d.replica.conf "$(DESTDIR)/etc/cvmfs/etc.httpd.conf.d.replica.conf"
endif
