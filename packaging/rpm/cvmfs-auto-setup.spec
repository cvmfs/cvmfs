Summary: CernVM File System Auto System Setup
Name: cvmfs-auto-setup
Version: 1.5
Release: 1
#Source0: %{name}-%{version}.tar.gz
BuildArch: noarch
Requires: cvmfs >= 2.1
Group: System/Filesystems
License: Copyright (c) 2009, CERN.  Distributed unter the BSD License.
%description
HTTP File System for Distributing Software to CernVM.
See http://cernvm.cern.ch
%post
/usr/bin/cvmfs_config setup
service autofs status > /dev/null 2>&1
if [ $? -ne 0 ]; then
  service autofs start > /dev/null
fi

%files
