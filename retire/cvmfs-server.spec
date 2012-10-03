Summary: CernVM File System Server Utilities
Name: cvmfs-server
Version: 2.0.4
Release: 1
Source0: https://cernvm.cern.ch/project/trac/downloads/cernvm/cvmfs-%{version}.tar.gz
Group: System/Filesystems
License: Copyright (c) 2.0.4, CERN.  Distributed unter the BSD License.
Requires: httpd cvmfs-keys >= 1.1
Requires(post): chkconfig 
Requires(preun): chkconfig initscripts
%description
HTTP File System for Distributing Software to CernVM.
See http://cernvm.cern.ch
%prep
%setup -q -n cvmfs-%{version}

%build
./configure --enable-sqlite3-builtin --enable-libcurl-builtin --enable-zlib-builtin --disable-cvmfs --prefix=/usr
make

%install
make install
install -D -m 755 add-ons/cvmfs-lvmrotate /usr/bin/cvmfs-lvmrotate

%post
/sbin/chkconfig --add cvmfsd

%preun
if [ $1 -eq 0 ]; then
  /sbin/service cvmfsd stop &>/dev/null
  /sbin/chkconfig --del cvmfsd
fi

%files
%defattr(-,root,root)
/usr/bin/cvmfs_sync.bin
/usr/bin/cvmfs_zpipe
/usr/bin/cvmfs_sign
/usr/bin/cvmfs_clgcmp
/usr/bin/cvmfs-sync
/usr/bin/cvmfsd_ctrl
/usr/bin/cvmfs-lvmrotate
/usr/bin/cvmfs_decrypt
/usr/bin/cvmfs_lscat
/usr/bin/cvmfs_mkkey
/usr/bin/cvmfs_unsign
/etc/rc.d/init.d/cvmfsd
/usr/bin/cvmfs_pull
/usr/bin/cvmfs_scrub
/usr/bin/cvmfs_snapshot
/usr/bin/cvmfs_server
/etc/cvmfs/cgi-bin/replica.cgi
/etc/cvmfs/etc.httpd.conf.d.replica.conf
%config /etc/cvmfs/server.conf 
%config /etc/cvmfs/replica.conf
%doc /usr/share/doc/cvmfs-%{version}/COPYING
%doc /usr/share/doc/cvmfs-%{version}/AUTHORS
%doc /usr/share/doc/cvmfs-%{version}/README
%doc /usr/share/doc/cvmfs-%{version}/NEWS
%doc /usr/share/doc/cvmfs-%{version}/ChangeLog
%doc /usr/share/doc/cvmfs-%{version}/FAQ
