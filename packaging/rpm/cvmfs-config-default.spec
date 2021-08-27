Summary: CernVM File System Default Configuration and Public Keys
Name: cvmfs-config-default
Version: 2.0
Release: 2
Source0: cern-it1.cern.ch.pub
Source1: cern-it4.cern.ch.pub
Source2: cern-it5.cern.ch.pub
Source3: egi.eu.pub
Source4: opensciencegrid.org.pub
Source5: cern.ch.conf
Source6: egi.eu.conf
Source7: opensciencegrid.org.conf
Source8: 50-cern.conf
Source9: README-config.d
Source10: cvmfs-config.cern.ch.conf
BuildArch: noarch
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Provides: cvmfs-config = %{version}-%{release}
Obsoletes: cvmfs-keys < 1.6
Provides: cvmfs-keys = 1.6
Obsoletes: cvmfs-init-scripts < 1.0.21
Provides: cvmfs-init-scripts = 1.0.21

Conflicts: cvmfs < 2.1.20
Conflicts: cvmfs-server < 2.1.20

%description
Default configuration parameters and public keys for CernVM-FS, providing access
to repositories under the cern.ch, egi.eu, and opensciencegrid.org domains

%prep

%build

%install
rm -rf $RPM_BUILD_ROOT
for cvmfsdir in keys/cern.ch keys/egi.eu keys/opensciencegrid.org domain.d default.d config.d; do
    mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/$cvmfsdir
done
for key in %{SOURCE0} %{SOURCE1} %{SOURCE2}; do
    install -D -m 444 "${key}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/cern.ch
done
for key in %{SOURCE3}; do
    install -D -m 444 "${key}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/egi.eu
done
for key in %{SOURCE4}; do
    install -D -m 444 "${key}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/opensciencegrid.org
done
for domainconf in %{SOURCE5} %{SOURCE6} %{SOURCE7}; do
    install -D -m 444 "${domainconf}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/domain.d
done
for defaultconf in %{SOURCE8}; do
    install -D -m 444 "${defaultconf}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/default.d
done
for conf in %{SOURCE10}; do
    install -D -m 444 "${conf}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/config.d
done
install -D -m 444 "%{SOURCE9}" $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/config.d/README

%files
%dir %{_sysconfdir}/cvmfs/keys/cern.ch
%dir %{_sysconfdir}/cvmfs/keys/egi.eu
%dir %{_sysconfdir}/cvmfs/keys/opensciencegrid.org
%{_sysconfdir}/cvmfs/keys/cern.ch/*
%{_sysconfdir}/cvmfs/keys/egi.eu/*
%{_sysconfdir}/cvmfs/keys/opensciencegrid.org/*
%config %{_sysconfdir}/cvmfs/config.d/cvmfs-config.cern.ch.conf
%config %{_sysconfdir}/cvmfs/domain.d/egi.eu.conf
%config %{_sysconfdir}/cvmfs/domain.d/opensciencegrid.org.conf
%config %{_sysconfdir}/cvmfs/domain.d/cern.ch.conf
%config %{_sysconfdir}/cvmfs/default.d/50-cern.conf
%config %{_sysconfdir}/cvmfs/config.d/*

%changelog
* Sat Apr 10 2021 Jakob Blomer <jblomer@cern.ch> - 2.0-2
- Fix RPM linter errors

* Mon Jul 13 2020 Jakob Blomer <jblomer@cern.ch> - 2.0-1
- Various minor fixes
- Use config repository

* Fri May 25 2018 Dave Dykstra <dwd@fnal.gov> - 1.7-1
- Skipped versions 1.5 and 1.6 because debian packages with those
  versions already exist
- Replace BNL and FNAL stratum 1s with their OSG aliases for the
  cern.ch domain

* Thu May 11 2017 Jakob Blomer <jblomer@cern.ch> - 1.4-1
- Remove dedicated atlas-nightlies.cern.ch Stratum 1

* Wed Jan 18 2017 Jakob Blomer <jblomer@cern.ch> - 1.3-1
- Update cern.ch master keys
- Disable ASGC Stratum 1

* Fri May 22 2015 Dave Dykstra <dwd@fnal.gov> - 1.2-2
- Change Obsoletes/Conflicts on cvmfs-keys and cvmfs-init-scripts to
  Obsoletes/Provides with specific version numbers, according to Fedora
  packaging guidelines

* Wed Apr 01 2015 Jakob Blomer <jblomer@cern.ch> - 1.2-1
- Disable Geo-API for ATLAS nightlies

* Mon Feb 23 2015 Jakob Blomer <jblomer@cern.ch> - 1.1-2
- use versioned provides

* Mon Feb 02 2015 Dave Dykstra <dwd@fnal.gov> - 1.1-1
- add CVMFS_USE_GEOAPI=yes to egi.eu and opensciencegrid.org
- fix BNL URL for opensciencegrid.org

* Thu Jan 22 2015 Jakob Blomer <jblomer@cern.ch> - 1.0-1
- initial packaging
