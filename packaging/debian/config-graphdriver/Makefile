#!/usr/bin/make -f

# This make file takes care of 'installing' the public key files
# and domain configurations while 'building' the cvmfs-config-default deb package

all: # nothing to build

install:
	mkdir -p $(DESTDIR)/etc/cvmfs/default.d
	cp mount/default.d/42-graphdriver.conf $(DESTDIR)/etc/cvmfs/default.d
