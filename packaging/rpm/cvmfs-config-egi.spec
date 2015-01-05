Summary: CernVM File System Configuration and Public Keys for EGI
Name: cvmfs-config-egi
Version: 1.0
Release: 1
Source0: egi.eu.pub
Source1: egi.eu.conf
Source2: 60-egi.conf
BuildArch: noarch
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Requires: cvmfs-config-cern

Provides: cvmfs-config
Provides: cvmfs-keys
Obsoletes: cvmfs-keys = 1.5

Conflicts: cvmfs < 2.1.20
Conflicts: cvmfs-server < 2.1.20

%description
Configuration parameters and public keys for CernVM-FS repositories for use on
EGI worker nodes.

%prep

%install
rm -rf $RPM_BUILD_ROOT
for cvmfsdir in keys/egi.eu domain.d default.d config.d; do
    mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/$cvmfsdir
done
for key in %{SOURCE0} do
    install -D -m 444 "${key}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/egi.eu
done
install -D -m 444 %{SOURCE1} $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/domain.d
install -D -m 444 %{SOURCE2} $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/default.d

%files
%dir %{_sysconfdir}/cvmfs/keys/egi.eu
%{_sysconfdir}/cvmfs/keys/egi.eu/*
%config %{_sysconfdir}/cvmfs/domain.d/egi.eu.conf
%config %{_sysconfdir}/cvmfs/default.d/60-egi.conf

%changelog
* Mon Jan 01 2015 Jakob Blomer <jblomer@cern.ch> - 1.0-1
- initial packaging
