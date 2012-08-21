#!/bin/sh

usage() {
  echo "$0 <build dir> <tag> <init scripts tag>"
}

build_dir=$1
if [ -z $build_dir ]; then
  usage
  exit 1
fi

tag=$2
if [ -z $tag ]; then
  usage
  exit 1
fi

init_scripts_tag=$3
if [ -z $init_scripts_tag ]; then
  usage
  exit 1
fi

if [ -d $build_dir ]; then
  echo "Build directory already exists"
  exit 2
fi

mkdir -p $build_dir || exit 2
cd $build_dir || exit 2

svn co $tag cvmfs || exit 3
svn co $init_scripts_tag init-scripts || exit 4

cd cvmfs || exit 4
./bootstrap.sh || exit 4
./configure --enable-libcurl-builtin --enable-sqlite3-builtin --enable-mount-scripts --enable-zlib-builtin --prefix=/usr || exit 4
make -j4 || exit 4
rm -rf /etc/cvmfs || exit 4
make install || exit 4

cd ../init-scripts || exit 5
cp -v etc/cvmfs/config.d/* /etc/cvmfs/config.d/ || exit 5

cvmfs_config setup || exit 6
cvmfs_config chksetup || exit 7

