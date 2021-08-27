Summary: CernVM File System Auto System Setup
Name: cvmfs-auto-setup
Version: 1.5
Release: 3
BuildArch: noarch
Requires: cvmfs >= 2.1
Group: System/Filesystems
License: Copyright (c) 2009, CERN.  Distributed unter the BSD License.
%description
HTTP File System for Distributing Software to CernVM.
See http://cernvm.cern.ch

%prep

%build

%install

%post
/usr/bin/cvmfs_config setup
service autofs status > /dev/null 2>&1
if [ $? -ne 0 ]; then
  service autofs start > /dev/null
fi

%files

%changelog
* Sat Apr 10 2021 Jakob Blomer <jblomer@cern.ch> - 1.5-3
- Fix RPM linter errors
