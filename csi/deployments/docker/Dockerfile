FROM registry.access.redhat.com/ubi9/ubi:9.2-489

ARG TARGETARCH
ARG REL_VERSION=2.11.0
ARG PKG_VERSION=2.11.0-1

RUN if [[ "${TARGETARCH}" == "amd64" ]]; then \
    export FRIENDLY_ARCH_NAME="x86_64" ; \
    elif [[ "${TARGETARCH}" == "arm64" ]]; then \
    export FRIENDLY_ARCH_NAME="aarch64" ; \
    else export FRIENDLY_ARCH_NAME="${TARGETARCH}" ; \
    fi ; \
    dnf install -y \
    https://ecsft.cern.ch/dist/cvmfs/cvmfs-${REL_VERSION}/cvmfs-${PKG_VERSION}.el9.${FRIENDLY_ARCH_NAME}.rpm \
    https://ecsft.cern.ch/dist/cvmfs/cvmfs-${REL_VERSION}/cvmfs-libs-${PKG_VERSION}.el9.${FRIENDLY_ARCH_NAME}.rpm \
    https://ecsft.cern.ch/dist/cvmfs/cvmfs-${REL_VERSION}/cvmfs-fuse3-${PKG_VERSION}.el9.${FRIENDLY_ARCH_NAME}.rpm \
    https://ecsft.cern.ch/dist/cvmfs/cvmfs-config/cvmfs-config-default-latest.noarch.rpm \
    && dnf --enablerepo=* clean all \
    && rm -rf /var/cache/yum /var/cache/dnf

ARG RELEASE
ARG GITREF
ARG CREATED

LABEL org.opencontainers.image.title="cvmfs-csi" \
      org.opencontainers.image.description="The CernVM-FS CSI image contains tools for exposing CVMFS repositories through Container Storage Interface." \
      org.opencontainers.image.authors="CernVM-FS CSI authors" \
      org.opencontainers.image.vendor="CernVM-FS CSI authors" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.documentation="https://github.com/cvmfs/csi" \
      org.opencontainers.image.source="https://github.com/cvmfs/csi" \
      org.opencontainers.image.url="https://github.com/cvmfs/csi" \
      org.opencontainers.image.revision=${GITREF} \
      org.opencontainers.image.version=${RELEASE} \
      org.opencontainers.image.created=${CREATED} \
      org.opencontainers.image.ref.name="" \
      org.opencontainers.image.base.digest="" \
      org.opencontainers.image.base.name=""

COPY bin/linux-${TARGETARCH}/csi-cvmfsplugin /csi-cvmfsplugin
COPY bin/linux-${TARGETARCH}/automount-runner /automount-runner
COPY bin/linux-${TARGETARCH}/automount-reconciler /automount-reconciler
COPY bin/linux-${TARGETARCH}/singlemount-runner /singlemount-runner
