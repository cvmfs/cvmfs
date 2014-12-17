Summary: CernVM File System CERN Configuration and Public Keys
Name: cvmfs-config-cern
Version: 1.0
Release: 1
Source0: cern.ch.pub
Source1: cern-it1.cern.ch.pub
Source2: cern-it2.cern.ch.pub
Source3: cern-it3.cern.ch.pub
Source4: cern.ch.conf
Source5: 50-cern.conf
BuildArch: noarch
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Provides: cvmfs-config
Provides: cvmfs-keys
Obsoletes: cvmfs-keys
Obsoletes: cvmfs-init-scripts

Requires: cvmfs >= 2.1.20

%description
Configuration parameters and public keys for CernVM-FS repositories under the
cern.ch domain

%prep

%install
rm -rf $RPM_BUILD_ROOT
for key in %{SOURCE0} %{SOURCE1} %{SOURCE2} %{SOURCE3}; do
    install -D -m 444 "${key}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/cern.ch/${key}
done
install -D -m 444 %{SOURCE4} $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/domain.d/cern.ch.conf
install -D -m 444 %{SOURCE5} $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/default.d/50-cern.conf

%files
%dir %{_sysconfdir}/cvmfs/keys/cern.ch
%{_sysconfdir}/cvmfs/keys/cern.ch/*
%config %{_sysconfdir}/cvmfs/domain.d/cern.ch.conf
%config %{_sysconfdir}/cvmfs/default.d/50-cern.conf

%changelog
* Tue Dec 16 2014 Jakob Blomer <jblomer@cern.ch> - 1.0-1
- initial packaging
