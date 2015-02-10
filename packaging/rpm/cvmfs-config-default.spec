Summary: CernVM File System Default Configuration and Public Keys
Name: cvmfs-config-default
Version: 1.1
Release: 1
Source0: cern.ch.pub
Source1: cern-it1.cern.ch.pub
Source2: cern-it2.cern.ch.pub
Source3: cern-it3.cern.ch.pub
Source4: egi.eu.pub
Source5: opensciencegrid.org.pub
Source6: cern.ch.conf
Source7: egi.eu.conf
Source8: opensciencegrid.org.conf
Source9: 50-cern.conf
Source10: 60-egi.conf
Source11: atlas-nightlies.cern.ch.conf
Source12: cms.cern.ch.conf
Source13: grid.cern.ch.conf
BuildArch: noarch
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Provides: cvmfs-config
Obsoletes: cvmfs-keys
Conflicts: cvmfs-keys
Obsoletes: cvmfs-init-scripts
Conflicts: cvmfs-init-scripts

Conflicts: cvmfs < 2.1.20
Conflicts: cvmfs-server < 2.1.20

%description
Default configuration parameters and public keys for CernVM-FS, providing access
to repositories under the cern.ch, egi.eu, and opensciencegrid.org domains

%prep

%install
rm -rf $RPM_BUILD_ROOT
for cvmfsdir in keys/cern.ch keys/egi.eu keys/opensciencegrid.org domain.d default.d config.d; do
    mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/$cvmfsdir
done
for key in %{SOURCE0} %{SOURCE1} %{SOURCE2} %{SOURCE3}; do
    install -D -m 444 "${key}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/cern.ch
done
for key in %{SOURCE4}; do
    install -D -m 444 "${key}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/egi.eu
done
for key in %{SOURCE5}; do
    install -D -m 444 "${key}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/opensciencegrid.org
done
for domainconf in %{SOURCE6} %{SOURCE7} %{SOURCE8}; do
    install -D -m 444 "${domainconf}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/domain.d
done
for defaultconf in %{SOURCE9} %{SOURCE10}; do
    install -D -m 444 "${defaultconf}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/default.d
done
for conf in %{SOURCE11} %{SOURCE12} %{SOURCE13}; do
    install -D -m 444 "${conf}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/config.d
done

%files
%dir %{_sysconfdir}/cvmfs/keys/cern.ch
%dir %{_sysconfdir}/cvmfs/keys/egi.eu
%dir %{_sysconfdir}/cvmfs/keys/opensciencegrid.org
%{_sysconfdir}/cvmfs/keys/cern.ch/*
%{_sysconfdir}/cvmfs/keys/egi.eu/*
%{_sysconfdir}/cvmfs/keys/opensciencegrid.org/*
%config %{_sysconfdir}/cvmfs/domain.d/egi.eu.conf
%config %{_sysconfdir}/cvmfs/domain.d/opensciencegrid.org.conf
%config %{_sysconfdir}/cvmfs/domain.d/cern.ch.conf
%config %{_sysconfdir}/cvmfs/default.d/50-cern.conf
%config %{_sysconfdir}/cvmfs/default.d/60-egi.conf
%config %{_sysconfdir}/cvmfs/config.d/*

%changelog
* Tue Feb 02 2015 Dave Dykstra <dwd@fnal.gov> - 1.1-1
- add CVMFS_USE_GEOAPI=yes to egi.eu and opensciencegrid.org

* Thu Jan 22 2015 Jakob Blomer <jblomer@cern.ch> - 1.0-1
- initial packaging
