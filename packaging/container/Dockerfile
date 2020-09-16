FROM gitlab-registry.cern.ch/cernvm/build-images/centos_x86_64:8 as build-base

FROM build-base as builder
ARG CVMFS_COMMIT=cvmfs-2.7.4

RUN git clone -b ${CVMFS_COMMIT} --depth=1 https://github.com/cvmfs/cvmfs
RUN mkdir cvmfs/build
WORKDIR cvmfs/build
RUN cmake -DBUILD_SERVER=no \
        -DBUILD_RECEIVER=no \
        -DBUILD_GEOAPI=no \
        -DBUILD_LIBCVMFS=no \
        -DBUILD_LIBCVMFS_CACHE=no \
        -DINSTALL_BASH_COMPLETION=no \
        ..
RUN make -j4

RUN mkdir ~/root && \
        make DESTDIR=~/root install && \
        rm -rf ~/root/usr/lib*/libcvmfs_fuse.* \
        rm -rf ~/root/usr/lib*/libcvmfs_fuse_debug.* \
        rm -rf ~/root/usr/lib*/libcvmfs_fuse_stub.* \
        rm -rf ~/root/usr/lib*/cvmfs/auto.cvmfs

# the executables that we want are now in ~/root/usr
# however we need to isolate the runtime dependecies of those executables
# we start by finding all the files with `find`
# then we run ldd against each of those files to get their runtime dependencies
# the output of ldd is manipulated with awk, grep, sort and uniq to get only a list of dependencies
# finally we use rsync to copy the dependencies we need in a specific directory
RUN mkdir ~/deps && \
        find ~/root/usr/ -type f | \
        xargs -n 1 -I {} ldd {} 2>/dev/null | \
        awk '{print $3}' | \
        grep /lib | sort | uniq | \
        rsync -aL --files-from=- / ~/deps/

# the executables and the dependencies are then pushed into a busybox
# we also push the loader and we create the necessary directories
FROM busybox
COPY --from=builder /home/sftnight/deps /
COPY --from=builder /home/sftnight/root /
COPY --from=builder /lib64/ld-linux-x86-64.so.2 /lib64

RUN mkdir -p /var/log /var/run/cvmfs

ADD mount_cvmfs.sh /usr/bin
ADD check_cvmfs.sh /usr/bin
ADD terminate.sh /usr/bin

# Needs to be set to the site squid
ENV CVMFS_HTTP_PROXY=
# The cvmfs-config.cern.ch repository gets always mounted
ENV CVMFS_REPOSITORIES unpacked.cern.ch,singularity.opensciencegrid.org
# Default: 4G cache
ENV CVMFS_QUOTA_LIMIT 4000
# Use the VERSION argument in the mount_cvmfs script
ENV VERSION ${CVMFS_COMMIT}

ENTRYPOINT [ "/usr/bin/mount_cvmfs.sh" ]

HEALTHCHECK --interval=5m --start-period=1m --timeout=1m \
  CMD [ "/usr/bin/check_cvmfs.sh", "liveness" ]
