# Copyright CERN.
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Based on the helm Makefile:
# https://github.com/helm/helm/blob/master/Makefile

BINDIR           := $(CURDIR)/bin
DIST_DIRS        := find * -type d -exec
TARGETS          ?= linux/amd64 linux/arm64
IMAGE_BUILD_TOOL ?= podman
GOX_PARALLEL     ?= 3

GOPATH        = $(shell go env GOPATH)
GOX           = $(GOPATH)/bin/gox
GOIMPORTS     = $(GOPATH)/bin/goimports

# go option
PKG        := ./...
TAGS       :=
TESTS      := .
TESTFLAGS  :=
LDFLAGS    := -w -s
GOFLAGS    :=
SRC        := $(shell find . -type f -name '*.go' -print)

# Required for globs to work correctly
SHELL      = /bin/bash

GIT_BRANCH = $(shell git rev-parse --abbrev-ref HEAD)
GIT_COMMIT = $(shell git rev-parse HEAD)
GIT_SHA    = $(shell git rev-parse --short HEAD)
GIT_TAG    = $(shell git describe --tags --abbrev=0 --exact-match 2>/dev/null)
GIT_DIRTY  = $(shell test -n "`git status --porcelain`" && echo "dirty" || echo "clean")

ifdef VERSION
	BINARY_VERSION = $(VERSION)
endif
BINARY_VERSION ?= ${GIT_TAG}

BASE_PKG = github.com/cvmfs/cvmfs-csi
# Only set Version if building a tag or VERSION is set
ifneq ($(BINARY_VERSION),)
	LDFLAGS += -X ${BASE_PKG}/internal/version.version=${BINARY_VERSION}
endif

# Clear the "unreleased" string in BuildMetadata
ifneq ($(GIT_TAG),)
	LDFLAGS += -X ${BASE_PKG}/internal/version.metadata=
endif
LDFLAGS += -X ${BASE_PKG}/internal/version.commit=${GIT_COMMIT}
LDFLAGS += -X ${BASE_PKG}/internal/version.treestate=${GIT_DIRTY}

IMAGE_TAG := ${GIT_BRANCH}
ifneq ($(GIT_TAG),)
    IMAGE_TAG = ${GIT_TAG}
endif

ARCH := $(shell uname -m)
ifeq ($(ARCH),x86_64)
    LOCAL_TARGET=linux/amd64
else ifeq ($(ARCH),arm64)
    LOCAL_TARGET=linux/arm64
endif

.PHONY: all
all: build

# ------------------------------------------------------------------------------
#  build

build: TARGETS = $(LOCAL_TARGET)
build: build-cross

$(BINDIR)/csi-cvmfsplugin: $(SRC)
	go build $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' -o $@ ./cmd/csi-cvmfsplugin

$(BINDIR)/automount-runner: $(SRC)
	go build $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' -o $@ ./cmd/automount-runner

$(BINDIR)/automount-reconciler: $(SRC)
	go build $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' -o $@ ./cmd/automount-reconciler

$(BINDIR)/singlemount-runner: $(SRC)
	go build $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' -o $@ ./cmd/singlemount-runner

.PHONY: build-cross
build-cross: LDFLAGS += -extldflags "-static"
build-cross: $(GOX) $(SRC)
	CGO_ENABLED=0 $(GOX) -parallel=$(GOX_PARALLEL) -output="$(BINDIR)/{{.OS}}-{{.Arch}}/csi-cvmfsplugin" -osarch='$(TARGETS)' $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' ./cmd/csi-cvmfsplugin
	CGO_ENABLED=0 $(GOX) -parallel=$(GOX_PARALLEL) -output="$(BINDIR)/{{.OS}}-{{.Arch}}/automount-runner" -osarch='$(TARGETS)' $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' ./cmd/automount-runner
	CGO_ENABLED=0 $(GOX) -parallel=$(GOX_PARALLEL) -output="$(BINDIR)/{{.OS}}-{{.Arch}}/automount-reconciler" -osarch='$(TARGETS)' $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' ./cmd/automount-reconciler
	CGO_ENABLED=0 $(GOX) -parallel=$(GOX_PARALLEL) -output="$(BINDIR)/{{.OS}}-{{.Arch}}/singlemount-runner" -osarch='$(TARGETS)' $(GOFLAGS) -tags '$(TAGS)' -ldflags '$(LDFLAGS)' ./cmd/singlemount-runner

# ------------------------------------------------------------------------------
#  image

image: build
	$(IMAGE_BUILD_TOOL) build                                      \
		--build-arg RELEASE=$(IMAGE_TAG)                           \
		--build-arg GITREF=$(GIT_COMMIT)                           \
		--build-arg CREATED=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ") \
		-t registry.cern.ch/kubernetes/cvmfs-csi:$(IMAGE_TAG)      \
		-f ./deployments/docker/Dockerfile .

# ------------------------------------------------------------------------------
#  dependencies

$(GOX):
	go install github.com/mitchellh/gox@v1.0.1

# ------------------------------------------------------------------------------
.PHONY: clean
clean:
	@rm -rf $(BINDIR)

.PHONY: info
info:
	 @echo "Version:           ${VERSION}"
	 @echo "Git Tag:           ${GIT_TAG}"
	 @echo "Git Commit:        ${GIT_COMMIT}"
	 @echo "Git Tree State:    ${GIT_DIRTY}"
