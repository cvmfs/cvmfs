#!/usr/bin/make -f

# This make file takes care of 'installing' the public key files
# and domain configurations while 'building' the cvmfs-config-default deb package

all: # nothing to build

install:
	mkdir -p $(DESTDIR)/etc/cvmfs/keys/cern.ch \
	  $(DESTDIR)/etc/cvmfs/keys/egi.eu \
	  $(DESTDIR)/etc/cvmfs/keys/opensciencegrid.org
	cp mount/keys/cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/cern-it1.cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/cern-it2.cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/cern-it3.cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/egi.eu.pub $(DESTDIR)/etc/cvmfs/keys/egi.eu
	cp mount/keys/opensciencegrid.org.pub $(DESTDIR)/etc/cvmfs/keys/opensciencegrid.org
	mkdir -p $(DESTDIR)/etc/cvmfs/domain.d
	cp mount/domain.d/cern.ch.conf $(DESTDIR)/etc/cvmfs/domain.d
	cp mount/domain.d/egi.eu.conf $(DESTDIR)/etc/cvmfs/domain.d
	cp mount/domain.d/opensciencegrid.org.conf $(DESTDIR)/etc/cvmfs/domain.d
	mkdir -p $(DESTDIR)/etc/cvmfs/default.d
	cp mount/default.d/50-cern.conf $(DESTDIR)/etc/cvmfs/default.d
	cp mount/default.d/60-egi.conf $(DESTDIR)/etc/cvmfs/default.d
	mkdir -p $(DESTDIR)/etc/cvmfs/config.d
	cp mount/config.d/atlas-nightlies.cern.ch.conf $(DESTDIR)/etc/cvmfs/config.d
	cp mount/config.d/cms.cern.ch.conf $(DESTDIR)/etc/cvmfs/config.d
	cp mount/config.d/grid.cern.ch.conf $(DESTDIR)/etc/cvmfs/config.d
