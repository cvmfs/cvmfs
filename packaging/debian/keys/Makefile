#!/usr/bin/make -f

# This make file takes care of 'installing' the public key files
# in /etc/cvmfs/keys while 'building' the cvmfs-keys deb package

all: # nothing to build

install:
	mkdir -p $(DESTDIR)/etc/cvmfs/keys
	cp mount/keys/*.pub $(DESTDIR)/etc/cvmfs/keys
