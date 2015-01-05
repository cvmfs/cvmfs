Summary: CernVM File System Configuration and Public Keys for EGI
Name: cvmfs-config-egi
Version: 1.0
Release: 1
Source0: egi.eu.pub
Source1: opensciencegrid.org.pub
Source2: egi.eu.conf
Source3: opensciencegrid.org.conf
Source4: 60-egi.conf
BuildArch: noarch
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Requires: cvmfs-config-cern

Provides: cvmfs-config
Obsoletes: cvmfs-keys
Obsoletes: cvmfs-init-scripts

Conflicts: cvmfs < 2.1.20
Conflicts: cvmfs-server < 2.1.20

%description
Configuration parameters and public keys for CernVM-FS repositories for use on
EGI worker nodes.

%prep

%install
rm -rf $RPM_BUILD_ROOT
for cvmfsdir in keys/egi.eu keys/opensciencegrid.org domain.d default.d config.d; do
    mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/$cvmfsdir
done
for key in %{SOURCE0}; do
    install -D -m 444 "${key}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/egi.eu
done
for key in %{SOURCE1}; do
    install -D -m 444 "${key}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/opensciencegrid.org
done
for domainconf in %{SOURCE2} %{SOURCE3}; do
    install -D -m 444 "${domainconf}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/domain.d
done
install -D -m 444 %{SOURCE4} $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/default.d

%files
%dir %{_sysconfdir}/cvmfs/keys/egi.eu
%dir %{_sysconfdir}/cvmfs/keys/opensciencegrid.org
%{_sysconfdir}/cvmfs/keys/egi.eu/*
%{_sysconfdir}/cvmfs/keys/opensciencegrid.org/*
%config %{_sysconfdir}/cvmfs/domain.d/egi.eu.conf
%config %{_sysconfdir}/cvmfs/domain.d/opensciencegrid.org.conf
%config %{_sysconfdir}/cvmfs/default.d/60-egi.conf

%changelog
* Mon Jan 01 2015 Jakob Blomer <jblomer@cern.ch> - 1.0-1
- initial packaging
