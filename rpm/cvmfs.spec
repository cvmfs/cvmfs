Summary: CernVM File System
Name: cvmfs
Version: 0.2.78
Release: 1
Source0: https://cernvm.cern.ch/project/trac/downloads/cernvm/%{name}-%{version}.tar.gz
Group: System/Filesystems
License: Copyright (c) 2009, CERN.  Distributed unter the BSD License.
Requires: bash coreutils grep gawk sed which perl glibc-common sudo initscripts shadow-utils psmisc autofs fuse curl cvmfs-keys >= 1.1
Requires(preun): chkconfig initscripts
%description
HTTP File System for Distributing Software to CernVM.
See http://cernvm.cern.ch
%prep
%setup -q

%build
./configure --enable-sqlite3-builtin --enable-libcurl-builtin --enable-zlib-builtin --enable-mount-scripts --disable-server --prefix=/usr
make

%pre
/usr/bin/getent passwd cvmfs >/dev/null
if [ $? -ne 0 ]; then
   /usr/sbin/useradd -r -d /var/cache/cvmfs2 -s /sbin/nologin -c "CernVM-FS service account" cvmfs
fi

/usr/bin/getent group fuse | grep -q cvmfs
if [ $? -ne 0 ]; then
   /usr/sbin/usermod -aG fuse cvmfs
fi

%install
make install
mkdir -p /var/cache/cvmfs2
mkdir -p /cvmfs

%post
/sbin/chkconfig --add cvmfs

%preun
if [ $1 = 0 ] ; then
   /sbin/service cvmfs stop >/dev/null 2>&1
   /sbin/chkconfig --del cvmfs
fi

%postun
if [ $1 -eq 0 ]; then
  #sed -i "/^\/mnt\/cvmfs \/etc\/auto.cvmfs/d" /etc/auto.master
  [ -f /var/lock/subsys/autofs ] && /sbin/service autofs reload >/dev/null
  sed -i "/added by CernVM-FS/d" /etc/fuse.conf
fi

%files
%defattr(-,root,root)
/usr/bin/cvmfs2
/usr/bin/cvmfs2_debug
/usr/bin/cvmfs_proxy_rtt
/usr/bin/cvmfs-talk
/usr/bin/cvmfs_fsck
/usr/bin/cvmfs_config
/etc/rc.d/init.d/cvmfs
/etc/auto.cvmfs
/etc/cvmfs/config.sh
/sbin/mount.cvmfs
/sbin/umount.cvmfs
%dir /etc/cvmfs/config.d
%dir /etc/cvmfs/domain.d
%dir /cvmfs
%attr(0700,cvmfs,cvmfs) %dir /var/cache/cvmfs2
%config /etc/cvmfs/default.conf 
%config /etc/cvmfs/domain.d/cern.ch.conf
%doc COPYING AUTHORS README NEWS ChangeLog FAQ
