# syntax=docker/dockerfile:1
#
# Builds the cvmfs service container in three stages:
#   builder   — compiles cvmfs from the bind-mounted source against a pinned
#               base image (no host toolchain involved)
#   libsource — resolves the dynamic library dependencies of the resulting
#               binaries against the same pinned base
#   final     — FROM scratch + COPY of the resolved root filesystem
#
ARG BASE_IMAGE=almalinux:9


# ---------- Stage: build cvmfs from source ----------
FROM ${BASE_IMAGE} AS builder

# Workaround for sudo perms regression in newer almalinux images
# (mirrors test/common/container/Dockerfile-dev).
RUN chmod 0400 /etc/shadow

ENV CMAKE_CXX_COMPILER_LAUNCHER=ccache

RUN dnf -y install dnf-utils \
 && dnf config-manager --set-enabled crb \
 && dnf -y install epel-release \
 && dnf -y install busybox ccache rpm-build \
                   git autoconf flex bison \
                   libtirpc-devel \
 && dnf clean all

# Install client build dependencies straight from the universal RPM spec so
# this stays in sync with the regular packaging build.
COPY packaging/rpm/cvmfs-universal.spec /tmp/packaging/rpm/
RUN --mount=type=cache,target=/var/cache/dnf,sharing=locked \
    dnf -y builddep /tmp/packaging/rpm/cvmfs-universal.spec \
 && dnf clean all

# Build automount-cvmfs (statically linked autofs fork) in its own layer so it
# is cached independently of the cvmfs source tree. The binary has no runtime
# library dependencies, so the libsource stage's ldd-walk leaves it alone.
ARG AUTOMOUNT_CVMFS_REPO=https://github.com/cvmfs/automount-cvmfs.git
ARG AUTOMOUNT_CVMFS_REF=main
# Glibc dropped the rpc/* headers in 2.32; on EL9 they're shipped separately
# in libtirpc-devel. Point the C compiler and the linker at libtirpc so the
# automount-cvmfs sources that include <rpc/rpc.h> build cleanly.
RUN set -eux; \
    git clone --depth 1 --branch "${AUTOMOUNT_CVMFS_REF}" \
        "${AUTOMOUNT_CVMFS_REPO}" /tmp/automount-cvmfs; \
    cd /tmp/automount-cvmfs; \
    export C_INCLUDE_PATH=/usr/include/tirpc; \
    ./configure LIBS=-ltirpc; \
    make STATIC=1; \
    install -D -m 755 daemon/automount-cvmfs /opt/automount/automount-cvmfs

# Compile cvmfs. Source is bind-mounted read-only; the build is out-of-source
# in /build and the install tree lands in /rootfs. Two cache mounts persist
# across rebuilds: ccache for compiler output, and the externals install
# prefix so prebuilt third-party libraries (openssl, curl, ...) are reused.
RUN --mount=type=bind,source=.,target=/src \
    --mount=type=cache,target=/root/.cache/ccache \
    --mount=type=cache,target=/root/.cache/cvmfs_externals_install \
    set -eux; \
    mkdir -p /build /rootfs; \
    cd /build && cmake \
      -DBUILD_SERVER=no -DBUILD_RECEIVER=no -DBUILD_GEOAPI=no \
      -DBUILD_LIBCVMFS=no -DBUILD_LIBCVMFS_CACHE=no \
      -DINSTALL_BASH_COMPLETION=no \
      -DEXTERNALS_PREFIX=/root/.cache/cvmfs_externals_install \
      /src; \
    make -j"$(nproc)"; \
    make DESTDIR=/rootfs install; \
    # Drop the fuse2 libraries — the container image only uses libfuse3.
    # mount.cvmfs and /usr/libexec/cvmfs/auto.cvmfs are kept so opt-in autofs
    # works. The /etc/auto.cvmfs -> /usr/libexec/cvmfs/auto.cvmfs symlink is
    # installed by cmake via install(CODE "ln -sf ..."), but that runs against
    # the live filesystem rather than DESTDIR; re-create it explicitly below.
    rm -f /rootfs/usr/lib*/libcvmfs_fuse.* \
          /rootfs/usr/lib*/libcvmfs_fuse_debug.* \
          /rootfs/usr/lib*/libcvmfs_fuse_stub.*; \
    # cvmfs_config requires bash, not available as a busybox applet
    rm -f /rootfs/usr/bin/cvmfs_config; \
    # Mountpoint for automount-cvmfs to attach autofs onto.
    mkdir -p /rootfs/cvmfs; \
    # Container-specific configuration + entrypoint scripts. /rootfs/etc must
    # exist before the auto.cvmfs symlink below — `make install` doesn't
    # populate it for client-only builds.
    mkdir -p /rootfs/etc/cvmfs/default.d /rootfs/var/log /rootfs/var/run/cvmfs; \
    # mount.cvmfs looks up the cvmfs user via getpwnam_r("cvmfs") and chowns
    # workspace/cache directories to that uid before exec'ing cvmfs2 with
    # uid=N,gid=N mount options. The from-scratch rootfs has no /etc/passwd,
    # so the lookup fails and the autofs trigger errors out with
    # "Failed to find user cvmfs in passwd database". Ship a minimal nss
    # database; the uid/gid match the system range used by the RPM postinstall.
    printf 'root:x:0:0:root:/root:/bin/sh\ncvmfs:x:999:999:CernVM-FS:/var/lib/cvmfs:/sbin/nologin\n' \
        > /rootfs/etc/passwd; \
    printf 'root:x:0:\ncvmfs:x:999:\nfuse:x:498:cvmfs\n' \
        > /rootfs/etc/group; \
    # cvmfs2 drops privileges to the cvmfs uid before opening
    # CVMFS_USYSLOG=/var/log/cvmfs.log (set in 42-container.conf), so /var/log
    # must be writable by uid 999. mount.cvmfs already chowns /var/run/cvmfs
    # at runtime; /var/log isn't on its list.
    chown 999:999 /rootfs/var/log; \
    ln -sf /usr/libexec/cvmfs/auto.cvmfs /rootfs/etc/auto.cvmfs; \
    cp /src/mount/default.d/42-container.conf       /rootfs/etc/cvmfs/default.d/; \
    cp /src/packaging/container/mount_cvmfs.sh      /rootfs/usr/bin/; \
    cp /src/packaging/container/check_cvmfs.sh      /rootfs/usr/bin/; \
    cp /src/packaging/container/terminate.sh        /rootfs/usr/bin/; \
    # automount-cvmfs (built in the previous layer). It is a cvmfs-specific
    # fork of autofs whose lookup_nss_read_master() synthesises a hardcoded
    # `/cvmfs file:/etc/auto.cvmfs` master entry, so no /etc/auto.master,
    # /etc/nsswitch.conf, or autofs `dir` lookup module is needed at runtime.
    # The entrypoint does not launch automount-cvmfs, so this is inert until
    # a user explicitly runs /usr/sbin/automount-cvmfs inside the container.
    install -D -m 755 /opt/automount/automount-cvmfs \
                      /rootfs/usr/sbin/automount-cvmfs; \
    # Busybox provides /bin/sh + coreutils for the entrypoint scripts
    mkdir -p /rootfs/bin; \
    cp /usr/sbin/busybox /rootfs/bin/busybox; \
    /rootfs/bin/busybox --install /rootfs/bin


# ---------- Stage: resolve dynamic libraries from a pinned base ----------
FROM ${BASE_IMAGE} AS libsource

# Candidate runtime libraries. The ldd-walk below only copies what's actually
# referenced by the binaries, so a slightly over-broad list is harmless.
# --allowerasing replaces libcurl-minimal (preinstalled in newer almalinux:9
# base images) with the full libcurl that cvmfs links against.
RUN dnf -y --allowerasing install \
      fuse3-libs \
      glibc \
      libcap \
      libcurl \
      libstdc++ \
      libuuid \
      openssl-libs \
      zlib \
 && dnf clean all

COPY --from=builder /rootfs /staging

# Resolve every binary against this stage's pinned libs and copy what's
# needed into the staging tree. The outer loop reruns until no new libs are
# added, so once a copied lib lands under /staging/usr/lib*/ (via the
# lib -> usr/lib symlinks) its transitive deps are also discovered.
RUN set -eu; \
    mkdir -p /staging/usr/lib /staging/usr/lib64; \
    ln -sfn usr/lib   /staging/lib; \
    ln -sfn usr/lib64 /staging/lib64; \
    libs_missing=1; \
    while [ "$libs_missing" -eq 1 ]; do \
      libs_missing=0; \
      for f in $(find /staging -type f); do \
        if ldd "$f" >/dev/null 2>&1; then \
          libs=$(ldd "$f" | grep -v "not found" | awk '{print $3}' \
                          | grep -v 0x | grep -v '^$' || true); \
          for l in $libs; do \
            if [ ! -f "/staging/$l" ]; then \
              libs_missing=1; \
              cp -v "$l" "/staging/$l"; \
            fi; \
          done; \
        fi; \
      done; \
    done; \
    case "$(uname -m)" in \
      x86_64)  ld=/lib64/ld-linux-x86-64.so.2 ;; \
      aarch64) ld=/lib/ld-linux-aarch64.so.1 ;; \
      *) echo "unsupported arch $(uname -m)"; exit 1 ;; \
    esac; \
    [ -f "/staging/$ld" ] || cp -v "$ld" "/staging/$ld"


# ---------- Stage: final scratch image ----------
FROM scratch

# Either a release version or a pre-release tag generated from version and git hash
ARG VERSION
# The base image from which runtime libraries are taken (fuse, ssl, etc)
ARG PLATFORM

LABEL maintainer="jblomer@cern.ch"
LABEL version="$VERSION"
LABEL platform="$PLATFORM"

# Can be set to auto-discover the squid proxy
ENV CVMFS_CLIENT_PROFILE=
# Needs to be set to the site squid
ENV CVMFS_HTTP_PROXY=
# CVMFS_REPOSITORIES selects the entrypoint mode:
#   unset  -> launch automount-cvmfs (lazy on-demand mounting of any repo)
#   set    -> pre-mount the listed repos via cvmfs2 (legacy behaviour);
#             cvmfs-config.cern.ch is always prepended.
ENV CVMFS_REPOSITORIES=
# Default: 4G cache
ENV CVMFS_QUOTA_LIMIT 4000
# Use the VERSION argument in the mount_cvmfs script
ENV VERSION $VERSION

COPY --from=libsource /staging/ /

ENTRYPOINT [ "/usr/bin/mount_cvmfs.sh" ]

HEALTHCHECK --interval=5m --start-period=1m --timeout=1m \
  CMD [ "/usr/bin/check_cvmfs.sh", "liveness" ]
