#!/usr/bin/make -f

# This make file takes care of 'installing' the CernVM apt repository
# and the associated gpg public key

all: # nothing to build

install:
	cp packaging/debian/cernvm.list /etc/apt/sources.list.d/cernvm.list
	cp packagin/debian/cernvm.gpg /etc/apt/trusted.gpg.d/cernvm.gpg
