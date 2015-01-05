#!/usr/bin/make -f

# This make file takes care of 'installing' the public key files
# and domain configurations while 'building' the cvmfs-config-egi deb package

all: # nothing to build

install:
	mkdir -p $(DESTDIR)/etc/cvmfs/keys/egi.eu $(DESTDIR)/etc/cvmfs/keys/opensciencegrid.org
	cp mount/keys/egi.eu.pub $(DESTDIR)/etc/cvmfs/keys/egi.eu
	cp mount/keys/opensciencegrid.org.pub $(DESTDIR)/etc/cvmfs/keys/opensciencegrid.org
	mkdir -p $(DESTDIR)/etc/cvmfs/domain.d
	cp mount/domain.d/egi.eu.conf $(DESTDIR)/etc/cvmfs/domain.d
	cp mount/domain.d/opensciencegrid.org.conf $(DESTDIR)/etc/cvmfs/domain.d
	mkdir -p $(DESTDIR)/etc/cvmfs/default.d
	cp mount/default.d/60-egi.conf $(DESTDIR)/etc/cvmfs/default.d
