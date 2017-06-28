#!/usr/bin/make -f

# This make file takes care of 'installing' the CernVM apt repository
# and the associated gpg public key

all: # nothing to build

install:
	mkdir -p ${DESTDIR}/usr/share/cvmfs-release
	cp packaging/debian/cernvm.list.* ${DESTDIR}/usr/share/cvmfs-release/
	mkdir -p ${DESTDIR}/etc/apt/sources.list.d
	mkdir -p ${DESTDIR}/etc/apt/trusted.gpg.d
	cp packaging/debian/cernvm.gpg ${DESTDIR}/etc/apt/trusted.gpg.d/cernvm.gpg
