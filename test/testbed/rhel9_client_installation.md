# Installation of Testbed: Client

## Packages to Install
```
dnf install htop
dnf install autofs zlib-devel libcap-devel httpd attr usbutils buildah
dnf install git cmake gcc gcc-c++ ninja-build golang
dnf install openssl-devel bzip2 libuuid-devel
dnf install vim patch fuse fuse-devel
dnf install python3 python-unversioned-command valgrind python3-devel unzip meson virtualenv
```

## Patch Kernel
Following patches you need. You need the fuse kernel module. The libfuse we will
replace with a newer version freshly build.
```
fuse3-3.10.2-7.el9.x86_64.rpm
fuse3-devel-3.10.2-7.el9.x86_64.rpm
fuse-common-3.10.2-7.el9.x86_64.rpm
fuse3-debuginfo-3.10.2-7.el9.x86_64.rpm
fuse3-libs-3.10.2-7.el9.x86_64.rpm
fuse3-debugsource-3.10.2-7.el9.x86_64.rpm
fuse3-libs-debuginfo-3.10.2-7.el9.x86_64.rpm
```

To install all RPMs in the current directory use 
```
rpm -U ./*

## Libfuse: Build and Install
```
# Create Python virtual env
cd ~
virtualenv -ppython3 pyVirtualenv
source pyVirtualenv/bin/activate
pip install --upgrade pip
pip install pytest
pip install looseversion
```

```
# Libfuse
cd ~
git clone https://github.com/libfuse/libfuse.git
cd libfuse
git checkout tags/fuse-3.16.2

# add support to allow fuse kernel backports (listen to flag FUSE_HAS_EXPIRE_ONLY )
# this cherry pick needs a bit of merge cleanup. just drop anything added for FUSE_DIRECT_IO_ALLOW_MMAP
git cherry-pick 54007eeddeae22523b720e4a426081fbdecb6bdf
git cherry-pick 67d28fb4b762989d895732f8b1aec9a4f69c0d56
# + you have to add in include/fuse_kernel.h the following line:
#define FUSE_HAS_EXPIRE_ONLY    (1ULL << 35)


mkdir build; cd build
meson setup ..
ninja
sudo chown root:root util/fusermount3
sudo chmod 4755 util/fusermount3
# in case not done: activate python env: source ~/pyVirtualenv/bin/activate
python3 -m pytest test/
sudo ninja install
```

## CVMFS: Build and Install

```
cd ~
git clone https://github.com/cvmfs/cvmfs.git
cd cvmfs/
mkdir build; cd build
ls /usr/local/lib64/libfuse3.so
export FUSE3_LIBRARY_PATH=/usr/local/lib64/libfuse3.so
ls /usr/local/include/
export FUSE3_INCLUDE_DIR_PATH=/usr/local/include/
reset && cmake -G Ninja -D BUILD_UNITTESTS=ON -D BUILD_GATEWAY=ON -D BUILD_UNITTESTS_DEBUG=ON -D BUILD_SERVER_DEBUG=ON -DCMAKE_INSTALL_RPATH_USE_LINK_PATH:BOOL=ON -D FUSE3_INCLUDE_DIR=$FUSE3_INCLUDE_DIR_PATH -D FUSE3_LIBRARY=$FUSE3_LIBRARY_PATH  ../; ninja; ninja install
```

# Further changes
For using share memory as cache base for cvmfs create the following:
```
mkdir /dev/shm/cvmfs-cache
```

--> for this run `./setupClient.sh` once after booting the server

Set readahead to 1024 KiB 
```
echo 1024 > /sys/class/bdi/<fsid>/read_ahead_kb
```

--> for this run `./runEachMount.sh /cvmfs/<repo_name>` after every new mounting