#!/usr/bin/make -f

# This make file takes care of 'installing' the public key files
# and domain configurations while 'building' the cvmfs-config-default deb package

all: # nothing to build

install:
	mkdir -p $(DESTDIR)/etc/cvmfs/default.d
	cp mount/default.d/42-shrinkwrap.conf $(DESTDIR)/etc/cvmfs/default.d
	mkdir -p $(DESTDIR)/etc/cvmfs/keys/cern.ch \
	  $(DESTDIR)/etc/cvmfs/keys/egi.eu \
	  $(DESTDIR)/etc/cvmfs/keys/opensciencegrid.org
	cp mount/keys/cern-it1.cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/cern-it4.cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/cern-it5.cern.ch.pub $(DESTDIR)/etc/cvmfs/keys/cern.ch
	cp mount/keys/egi.eu.pub $(DESTDIR)/etc/cvmfs/keys/egi.eu
	cp mount/keys/opensciencegrid.org.pub $(DESTDIR)/etc/cvmfs/keys/opensciencegrid.org
