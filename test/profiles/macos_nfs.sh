# shellcheck shell=bash
#
# Profile: macos_nfs
#
# Runs the client integration test suite on macOS with the NFS frontend
# (CVMFS_MOUNT_BACKEND=nfs): cvmfs2 serves NFSv4 and the kernel NFS client
# mounts it, no macFUSE.  Expects the package from ci/nfs4/pkg.sh to be
# installed; see .github/workflows/ci_macos_nfs_tests.yml.
#
# Exclusions: the former macOS FUSE CI list, plus tests that depend on a FUSE
# device, the FUSE mount options, or reloading/attaching a FUSE session, and
# Linux-only tests (autofs, /proc).  011 expects a read to crash the client
# after its cache manager is killed; over NFS the kernel serves it from cache.
# The frontend forces CVMFS_NFS_SOURCE, which requires the workspace to be the
# cache directory: excludes 032 (CVMFS_WORKSPACE) and 068 (alien cache).

PROFILE_CLASS_NAME="MacOSNFSClientTests"
PROFILE_TESTSUITE="src/0* src/1*"

PROFILE_EXCLUSIONS="\
  src/004-davinci \
  src/005-asetup \
  src/006-buildkernel \
  src/007-testjobs \
  src/008-default_domain \
  src/011-rmemptyfilesrebuild \
  src/015-rebuild_on_crash \
  src/016-dnsunreachable \
  src/017-dnstimeout \
  src/024-reload-during-asetup \
  src/028-negativecache \
  src/032-workspace \
  src/039-reloadalarm \
  src/040-aliencache \
  src/041-rocache \
  src/045-oasis \
  src/050-configrepo \
  src/052-roundrobindns \
  src/053-uuid \
  src/055-ownership \
  src/056-lowspeedlimit \
  src/057-parallelmakecache \
  src/059-fallbackproxy \
  src/060-hidexattrs \
  src/061-systemdnokill \
  src/064-fsck \
  src/068-rocache \
  src/066-killall \
  src/069-systemremount \
  src/072-mountdevicepath \
  src/074-oom \
  src/077-doublemount \
  src/078-offlinemode \
  src/081-shrinkwrap \
  src/082-shrinkwrap-cms \
  src/083-suid \
  src/084-premounted \
  src/087-xattrs \
  src/088-watchdog \
  src/089-external_cache_plugin \
  src/090-talkchroot \
  src/091-defaultconfigrepo \
  src/091-talksocket \
  src/093-deviceid \
  src/092-stat \
  src/094-attachmount \
  src/095-fuser \
  src/096-cancelreq \
  src/097-statfs \
  src/099-http_tracing \
  src/100-reload-switch-debug \
  src/102-reusefd \
  src/103-reloadcachemgr \
  src/104-concurrent_mounts \
  src/105-streaming-cache \
  src/108-fuse-passthru \
  src/108-global-config \
  src/109-capabilities \
  src/111-ignore-mount-attempt \
  src/406-ducc-webhook-notifications"

export CVMFS_TEST_MOUNT_BACKEND=nfs
