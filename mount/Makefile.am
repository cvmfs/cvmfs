if BUILD_MOUNTSCRIPTS
install-exec-hook:
	test -z "$(DESTDIR)/etc" || mkdir -p "$(DESTDIR)/etc"
	test -z "$(DESTDIR)/sbin" || mkdir -p "$(DESTDIR)/sbin"
	test -z "$(DESTDIR)/etc/init.d" || mkdir -p "$(DESTDIR)/etc/init.d"
	test -z "$(DESTDIR)/etc/cvmfs" || mkdir -p "$(DESTDIR)/etc/cvmfs"
	test -z "$(DESTDIR)/etc/cvmfs/config.d" || mkdir -p "$(DESTDIR)/etc/cvmfs/config.d"	
	test -z "$(DESTDIR)/etc/cvmfs/domain.d" || mkdir -p "$(DESTDIR)/etc/cvmfs/domain.d"
	test -z "$(DESTDIR)/etc/cvmfs/keys" || mkdir -p "$(DESTDIR)/etc/cvmfs/keys"
	install -c auto.cvmfs "$(DESTDIR)/etc/auto.cvmfs"
	install -c mount.cvmfs "$(DESTDIR)/sbin/mount.cvmfs"
	install -c umount.cvmfs "$(DESTDIR)/sbin/umount.cvmfs"
	install -c cvmfs.initd "$(DESTDIR)/etc/init.d/cvmfs"
	install -c -m 0644 config.sh "$(DESTDIR)/etc/cvmfs/config.sh"
	install -c -m 0644 default.conf "$(DESTDIR)/etc/cvmfs/default.conf"
	install -c -m 0644 domain.d/cern.ch.conf "$(DESTDIR)/etc/cvmfs/domain.d/cern.ch.conf"
	install -c -m 0444 keys/cern.ch.pub "$(DESTDIR)/etc/cvmfs/keys/cern.ch.pub"
endif
