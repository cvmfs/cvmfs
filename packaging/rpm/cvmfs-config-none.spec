Summary: CernVM File System Empty Configuration
Name: cvmfs-config-none
Version: 1.0
Release: 3
BuildArch: noarch
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Provides: cvmfs-config = %{version}-%{release}
Conflicts: cvmfs < 2.1.20
Conflicts: cvmfs-server < 2.1.20

%description
Empty configuration for CernVM-FS, used to fulfill the cvmfs dependency on cvmfs-config

%prep

%build

%install

%files

%changelog
* Sat Apr 10 2021 Jakob Blomer <jblomer@cern.ch> - 1.0-3
- Fix RPM linter errors

* Mon Feb 23 2015 Jakob Blomer <jblomer@cern.ch> - 1.0-2
- use versioned provides

* Wed Dec 17 2014 Jakob Blomer <jblomer@cern.ch> - 1.0-1
- initial packaging
