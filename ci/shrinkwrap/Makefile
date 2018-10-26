# Builds the CernVM-FS shrinkwrap Docker container
# Requires a Debian based distribution with
# make, curl, gcc, xz-utils

SHRINKWRAP_PACKAGE=
CVMFS_SRC =

SHRINKWRAP_VERSION = $(shell basename $(SHRINKWRAP_PACKAGE) | cut -d_ -f2 | cut -d~ -f1)
SHRINKWRAP_RELEASE = $(shell basename $(SHRINKWRAP_PACKAGE) | cut -d_ -f2 | cut -d~ -f2)

IMAGE_NAME = cvmfs-shrinkwrap
IMAGE_ARCH = amd64
IMAGE_RELEASE = 1
IMAGE_VERSION = $(SHRINKWRAP_VERSION)-$(IMAGE_RELEASE)
IMAGE_TARBALL = $(IMAGE_NAME)-$(IMAGE_VERSION).tar.gz
BB_VERSION = 1.23.2
SHRINKWRAP_CONFIG_VERSION = 1.1
SHRINKWRAP_CONFIG_RELEASE = 1

BB_BASE_URL = http://ecsft.cern.ch/dist/cernvm
SHRINKWRAP_BASE_URL = $(shell dirname $(SHRINKWRAP_PACKAGE))
SHRINKWRAP_CONFIG_BASE_URL = https://ecsft.cern.ch/dist/cvmfs/cvmfs-config

BB_SRC = $(BB_BASE_URL)/busybox-$(BB_VERSION).tar.gz
SHRINKWRAP_SRC = $(SHRINKWRAP_BASE_URL)/cvmfs-shrinkwrap_$(SHRINKWRAP_VERSION)~$(SHRINKWRAP_RELEASE)_$(IMAGE_ARCH).deb
SHRINKWRAP_CONFIG_SRC = $(SHRINKWRAP_CONFIG_BASE_URL)/cvmfs-config-shrinkwrap_$(SHRINKWRAP_CONFIG_VERSION)-$(SHRINKWRAP_CONFIG_RELEASE)_all.deb

ROOTFS = $(IMAGE_NAME)-$(IMAGE_VERSION)

all: $(IMAGE_TARBALL)

clean:
	rm -f $(IMAGE_TARBALL)
	rm -rf $(IMAGE_NAME)-$(IMAGE_VERSION)

$(IMAGE_TARBALL): $(ROOTFS)/CONTENTS
	tar -C $(IMAGE_NAME)-$(IMAGE_VERSION) -zc . > $@
	rm -rf $(IMAGE_NAME)-$(IMAGE_VERSION)

$(ROOTFS)/CONTENTS: $(ROOTFS)/usr/bin/cvmfs_shrinkwrap
	cat /dev/null > $(ROOTFS)/.cvmfs-shrinkwrap.deb
	cat /dev/null > $(ROOTFS)/.cvmfs-config.deb
	cat /dev/null > $(ROOTFS)/.busybox-$(BB_VERSION).tar.gz
	rm -rf $(ROOTFS)/.busybox-$(BB_VERSION)/*
	echo "Image version:                $(IMAGE_VERSION)" >> $@
	echo "Busybox version:              $(BB_VERSION)" >> $@
	echo "CernVM-FS shrinkwrap:         $(SHRINKWRAP_SRC)" >> $@
	echo "CernVM-FS shrinkwrap version: $(SHRINKWRAP_VERSION)" >> $@


$(ROOTFS):
	mkdir -p $@

$(ROOTFS)/.busybox-$(BB_VERSION).tar.gz: | $(ROOTFS)
	curl -o $@ $(BB_SRC)

$(ROOTFS)/.busybox-$(BB_VERSION): $(ROOTFS)/.busybox-$(BB_VERSION).tar.gz
	cd $(ROOTFS) && tar xvf .busybox-$(BB_VERSION).tar.gz
	mv $(ROOTFS)/busybox-$(BB_VERSION) $(ROOTFS)/.busybox-$(BB_VERSION)

$(ROOTFS)/busybox: | $(ROOTFS)/.busybox-$(BB_VERSION)
	$(MAKE) -C $(ROOTFS)/.busybox-$(BB_VERSION) defconfig
	$(MAKE) -j $(shell nproc) -C $(ROOTFS)/.busybox-$(BB_VERSION)
	mv $(ROOTFS)/.busybox-$(BB_VERSION)/busybox $@

$(ROOTFS)/.done_minbase: $(ROOTFS)/busybox
	mkdir -p $(ROOTFS)/cache
	mkdir -p $(ROOTFS)/export
	mkdir -p $(ROOTFS)/var/lib/cvmfs
	ln -s /cache $(ROOTFS)/var/lib/cvmfs/shrinkwrap
	touch $(ROOTFS)/cvmfs.local
	ln -s /etc/cvmfs/default.d/42-shrinkwrap.conf $(ROOTFS)/cvmfs.conf
	mkdir -p $(ROOTFS)/bin
	mkdir -p $(ROOTFS)/dev
	mkdir -p $(ROOTFS)/lib
	mkdir -p $(ROOTFS)/mnt
	mkdir -p $(ROOTFS)/proc
	mkdir -p $(ROOTFS)/tmp
	mkdir -p $(ROOTFS)/usr/bin
	mkdir -p $(ROOTFS)/var/lib/cvmfs
	mkdir -p $(ROOTFS)/var/log
	mkdir -p $(ROOTFS)/var/run/cvmfs
	ln -fs lib $(ROOTFS)/lib64
	for CMD in $(shell $(ROOTFS)/busybox --list | sort -u); do \
	    ln -fvs /busybox $(ROOTFS)/bin/$$CMD; \
	  done
	touch $(ROOTFS)/.done_minbase

$(ROOTFS)/.cvmfs-config.deb: | $(ROOTFS)
	curl -fo $@ $(SHRINKWRAP_CONFIG_SRC)

$(ROOTFS)/.cvmfs-shrinkwrap.deb: | $(ROOTFS)
	curl -fo $@ $(SHRINKWRAP_SRC)

$(ROOTFS)/usr/bin/cvmfs_shrinkwrap: $(ROOTFS)/.done_minbase | $(ROOTFS)/.cvmfs-config.deb $(ROOTFS)/.cvmfs-shrinkwrap.deb
	cd $(ROOTFS) && ar p .cvmfs-config.deb data.tar.gz | tar zx
	cd $(ROOTFS) && ar p .cvmfs-shrinkwrap.deb data.tar.xz | tar Jx
	$(CVMFS_SRC)/ci/shrinkwrap/libs.sh $(ROOTFS)
	ldconfig -r $(ROOTFS)
