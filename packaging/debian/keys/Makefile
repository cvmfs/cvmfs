#!/usr/bin/make -f

# This make file takes care of 'installing' the public key files
# and domain configurations while 'building' the cvmfs-keys deb package

all: # nothing to build

install:
	mkdir -p $(DESTDIR)/etc/cvmfs/keys
	cp cvmfs-keys/keys/*.pub $(DESTDIR)/etc/cvmfs/keys
	mkdir -p $(DESTDIR)/etc/cvmfs/domain.d
	cp cvmfs-keys/domain.d/*.conf $(DESTDIR)/etc/cvmfs/domain.d
	cp cvmfs-keys/serverorder.sh $(DESTDIR)/etc/cvmfs
