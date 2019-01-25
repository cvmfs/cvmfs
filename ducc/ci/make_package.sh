
set -e

go version
go env

SOURCE_LOCATION="$1"
GOPATH="$2"
NIGHTLY_NUMBER="$3"

pushd $SOURCE_LOCATION

source ci/release_data.sh

if [ x"$NIGHTLY_NUMBER" != x"" ]; then
  RELEASE="$RELEASE.$NIGHTLY_NUMBER"
fi

RELEASE=$RELEASE$(rpm --eval "%{?dist}")

GOPATH=$GOPATH make

echo "Creating togo project"
mkdir togo
pushd togo

togo project create ducc
pushd ducc

echo "Coping binarites into the project"
mkdir -p root/usr/bin
cp ../../ducc root/usr/bin

echo "Changing the spec file"
sed -i -e "s/<<DUCC_VERSION>>/$VERSION/g" ../../ci/header
sed -i -e "s/<<DUCC_RELEASE>>/$RELEASE/g" ../../ci/header

cp --remove-destination ../../ci/header spec/header

echo "Excluding directoies from togo project"
togo file exclude root/usr/bin

echo "Building togo project"
togo build package

popd
popd

echo "Moving rpms into main rpm directory"
mkdir rpms
cp togo/ducc/rpms/*.rpm rpms/

mkdir rpms/src
cp togo/ducc/rpms/src/*.rpm rpms/

echo "Creating package map"
mkdir -p pkgmap
PKGMAP_FILE=pkgmap/pkgmap.cc7_x86_64
PACKAGE_NAME=ducc-${VERSION}-${RELEASE}.x86_64.rpm
echo "[cc7_x86_64]" >> ${PKGMAP_FILE}
echo "ducc=${PACKAGE_NAME}" >> ${PKGMAP_FILE}

#rm -rf togo
