#!/usr/bin/make -f

# This make file takes care of 'installing' the public key files
# and domain configurations while 'building' the cvmfs-keys deb package

all: # nothing to build

install:
	mkdir -p $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/cern-it1.cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/cern-it2.cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/cern-it3.cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	mkdir -p $(DESTDIR)/etc/cvmfs/domain.d
	cp mount/domain.d/cern.ch.conf $(DESTDIR)/etc/cvmfs/domain.d
	mkdir -p $(DESTDIR)/etc/cvmfs/default.d
	cp mount/default.d/50-cern.conf $(DESTDIR)/etc/cvmfs/default.d
	mkdir -p $(DESTDIR)/etc/cvmfs/config.d
	cp mount/config.d/atlas-nightlies.cern.ch.conf $(DESTDIR)/etc/cvmfs/config.d
	cp mount/config.d/cms.cern.ch.conf $(DESTDIR)/etc/cvmfs/config.d
	cp mount/config.d/grid.cern.ch.conf $(DESTDIR)/etc/cvmfs/config.d
