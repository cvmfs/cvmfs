FROM scratch

# Either a release version or a pre-release tag generated from version and git hash
ARG VERSION
# The build platform from which base libraries are taken (fuse, ssl, etc)
ARG PLATFORM

LABEL maintainer="jblomer@cern.ch"
LABEL version="$VERSION"
LABEL platform="$PLATFORM"

# Can be set to auto-discover the squid proxy
ENV CVMFS_CLIENT_PROFILE=
# Needs to be set to the site squid
ENV CVMFS_HTTP_PROXY=
# The cvmfs-config.cern.ch repository gets always mounted
ENV CVMFS_REPOSITORIES unpacked.cern.ch,singularity.opensciencegrid.org
# Default: 4G cache
ENV CVMFS_QUOTA_LIMIT 4000
# Use the VERSION argument in the mount_cvmfs script
ENV VERSION $VERSION

ADD rootfs.tar /

ENTRYPOINT [ "/usr/bin/mount_cvmfs.sh" ]

HEALTHCHECK --interval=5m --start-period=1m --timeout=1m \
  CMD [ "/usr/bin/check_cvmfs.sh", "liveness" ]
