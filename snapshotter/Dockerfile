#   Copyright The containerd Authors.

#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

ARG CONTAINERD_VERSION=v1.7.3

FROM golang:1.20.4-bullseye AS golang-base

# Build containerd
FROM golang-base AS containerd-dev
ARG CONTAINERD_VERSION
RUN apt-get update -y && apt-get install -y libbtrfs-dev libseccomp-dev && \
    git clone -b ${CONTAINERD_VERSION} --depth 1 \
              https://github.com/containerd/containerd $GOPATH/src/github.com/containerd/containerd && \
    cd $GOPATH/src/github.com/containerd/containerd && \
    GO111MODULE=off make && DESTDIR=/out/ make install

# Build the cvmfs snapshotter
FROM golang-base AS snapshotter-dev
ARG SNAPSHOTTER_BUILD_FLAGS
ARG CTR_REMOTE_BUILD_FLAGS
COPY . $GOPATH/src/github.com/cvmfs/snapshotter
RUN cd $GOPATH/src/github.com/cvmfs/snapshotter && \
    PREFIX=/out/ GO_BUILD_FLAGS=${SNAPSHOTTER_BUILD_FLAGS} make

# Image which can be used as a node image for KinD
FROM kindest/node:v1.27.1
COPY --from=containerd-dev /out/usr/local/bin/containerd /out/usr/local/bin/containerd-shim-runc-v2 /usr/local/bin/
COPY --from=snapshotter-dev /out/* /usr/local/bin/
COPY ./script/config/ /
RUN apt-get update -y && apt-get install --no-install-recommends -y fuse3 wget && \
    systemctl enable cvmfs-snapshotter && chmod +x /usr/local/bin/kind-entrypoint.sh && \
    wget -nv -qO- https://github.com/containerd/nerdctl/releases/download/v1.4.0/nerdctl-1.4.0-linux-amd64.tar.gz | tar -C /usr/local/bin -zx
ENTRYPOINT [ "/usr/local/bin/kind-entrypoint.sh", "/usr/local/bin/entrypoint", "/sbin/init" ]
