#!/bin/sh

#
# This script builds the CernVM-FS service container in OCI and docker archive.
# The cvmfs-service-$version.x86_64.docker.tar.gz archive can be added to docker
# with `cat $archive | docker load`
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_BUSYBOX="$3"
CVMFS_TAG="$4"

[ ! -z $CVMFS_SOURCE_LOCATION ] || exit 1
[ ! -z $CVMFS_RESULT_LOCATION ] || exit 1
[ ! -z $CVMFS_BUSYBOX ] || exit 1
[ ! -z $CVMFS_TAG ] || exit 1

# sanity checks
# TODO: add externals?
for d in build rootfs; do
  if [ -d ${CVMFS_RESULT_LOCATION}/${d} ]; then
    echo "build directory seems to be used before (${CVMFS_RESULT_LOCATION}/${d} exists)"
    exit 1
  fi
done
for d in build rootfs; do
  mkdir -p ${CVMFS_RESULT_LOCATION}/${d}
done

mkdir ${CVMFS_RESULT_LOCATION}/rootfs/bin
cp $CVMFS_BUSYBOX ${CVMFS_RESULT_LOCATION}/rootfs/bin/
${CVMFS_RESULT_LOCATION}/rootfs/bin/busybox --install ${CVMFS_RESULT_LOCATION}/rootfs/bin

cd ${CVMFS_RESULT_LOCATION}/build
cmake -DBUILD_SERVER=no -DBUILD_RECEIVER=no -DBUILD_GEOAPI=no \
  -DBUILD_LIBCVMFS=no -DBUILD_LIBCVMFS_CACHE=no \
  -DINSTALL_BASH_COMPLETION=no \
  -DEXTERNALS_PREFIX=${CVMFS_RESULT_LOCATION}/externals \
  ${CVMFS_SOURCE_LOCATION}
make -j4
make DESTDIR=${CVMFS_RESULT_LOCATION}/rootfs install

# Remove fuse2 and autofs related parts
rm -f ${CVMFS_RESULT_LOCATION}/rootfs/usr/bin/mount.cvmfs
rm -f ${CVMFS_RESULT_LOCATION}/rootfs/usr/lib*/libcvmfs_fuse.*
rm -f ${CVMFS_RESULT_LOCATION}/rootfs/usr/lib*/libcvmfs_fuse_debug.*
rm -f ${CVMFS_RESULT_LOCATION}/rootfs/usr/lib*/libcvmfs_fuse_stub.*
rm -f ${CVMFS_RESULT_LOCATION}/rootfs/usr/lib*/cvmfs/auto.cvmfs

# cvmfs_config uses bash, which we don't have as a busybox applet
rm -f ${CVMFS_RESULT_LOCATION}/rootfs/usr/bin/cvmfs_config

# Add dependent libraries (openssl, libfuse, etc)
mkdir -p ${CVMFS_RESULT_LOCATION}/rootfs/usr/lib \
  ${CVMFS_RESULT_LOCATION}/rootfs/usr/lib64
ln -s usr/lib ${CVMFS_RESULT_LOCATION}/rootfs/lib
ln -s usr/lib64 ${CVMFS_RESULT_LOCATION}/rootfs/lib64
libs_missing=1
while [ $libs_missing -eq 1 ]; do
  libs_missing=0
  for f in $(find ${CVMFS_RESULT_LOCATION}/rootfs/usr -type f); do
    libs=
    if ldd $f >/dev/null 2>&1; then
      # ldd may not find dependencies between cvmfs' own libraries in the
      # rootfs directory
      echo "[DEP] $f"
      libs="$(ldd $f | grep -v "not found" | awk '{print $3}' | grep -v 0x | \
              grep -v '^$' || true)"
      echo "  --> $(echo $libs | tr \n ' ')"
    fi
    if [ -z "$libs" ]; then
      echo "[CHECK] $f"
      echo "  --> empty list of dependencies, skipping"
      continue
    fi
    for l in $libs; do
      if [ ! -f ${CVMFS_RESULT_LOCATION}/rootfs/$l ]; then
        libs_missing=1
        cp -v $l ${CVMFS_RESULT_LOCATION}/rootfs/$l
      else
        echo "${CVMFS_RESULT_LOCATION}/rootfs/$l present, skipping"
      fi
    done
  done
done
# TODO(jblomer): use readelf to not hardcode the interpreter
if [ ! -f ${CVMFS_RESULT_LOCATION}/rootfs/lib64/ld-linux-x86-64.so.2 ]; then
  cp -v /lib64/ld-linux-x86-64.so.2 ${CVMFS_RESULT_LOCATION}/rootfs/lib64/
fi

# Add the container-specific configuration
mkdir -p ${CVMFS_RESULT_LOCATION}/rootfs/var/log
mkdir -p ${CVMFS_RESULT_LOCATION}/rootfs/var/run/cvmfs
cp -v ${CVMFS_SOURCE_LOCATION}/mount/default.d/42-container.conf \
  ${CVMFS_RESULT_LOCATION}/rootfs/etc/cvmfs/default.d/

# Add the entry point and health check scripts
cp -v ${CVMFS_SOURCE_LOCATION}/packaging/container/mount_cvmfs.sh \
  ${CVMFS_RESULT_LOCATION}/rootfs/usr/bin/
cp -v ${CVMFS_SOURCE_LOCATION}/packaging/container/check_cvmfs.sh \
  ${CVMFS_RESULT_LOCATION}/rootfs/usr/bin/
cp -v ${CVMFS_SOURCE_LOCATION}/packaging/container/terminate.sh \
  ${CVMFS_RESULT_LOCATION}/rootfs/usr/bin/

# Tar up the root file system and build the container
cd ${CVMFS_RESULT_LOCATION}/rootfs
tar --owner=root --group=root -cvf rootfs.tar .

cp ${CVMFS_SOURCE_LOCATION}/packaging/container/Dockerfile .
docker build \
  --build-arg VERSION=$CVMFS_TAG \
  --build-arg PLATFORM="$(. /etc/os-release; echo $PRETTY_NAME)" \
  --tag cvmfs/service:$CVMFS_TAG \
  .

# TODO(jblomer): use buildah once build nodes are recent enough
#buildah bud \
#  --build-arg VERSION=$CVMFS_TAG \
#  --build-arg PLATFORM="$(. /etc/os-release; echo $PRETTY_NAME)" \
#  --tag cvmfs/service:$CVMFS_TAG

ARCHIVE_NAME="cvmfs-service-${CVMFS_TAG}.$(uname -m)"
IMAGE_NAME="cvmfs/service:$CVMFS_TAG"

docker inspect $IMAGE_NAME
docker save \
  --output ${CVMFS_RESULT_LOCATION}/${ARCHIVE_NAME}.docker.tar \
  $IMAGE_NAME
docker rmi $IMAGE_NAME

# buildah inspect $IMAGE_NAME
#buildah push $IMAGE_NAME \
#  oci-archive:${CVMFS_RESULT_LOCATION}/${ARCHIVE_NAME}.oci.tar:${IMAGE_NAME}
#buildah push cvmfs/service:$CVMFS_TAG \
#  docker-archive:${CVMFS_RESULT_LOCATION}/${ARCHIVE_NAME}.docker.tar:${IMAGE_NAME}
#buildah rmi $IMAGE_NAME

gzip --force ${CVMFS_RESULT_LOCATION}/${ARCHIVE_NAME}.docker.tar
