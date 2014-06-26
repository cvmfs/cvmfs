Summary: CernVM File System Public Keys and Configs
Name: cvmfs-keys
Version: 1.5
Release: 1%{?dist}
Source0: %{name}-%{version}.tar.gz
BuildArch: noarch
Requires: curl
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Group: System/Filesystems
License: Copyright (c) 2014, CERN.  Distributed unter the BSD License.
%description
HTTP File System for Distributing Software for CernVM
See http://cernvm.cern.ch
%prep
%setup -q
%install
rm -rf $RPM_BUILD_ROOT
install -D -m 444 serverorder.sh $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/serverorder.sh
for dom in cern.ch egi.eu opensciencegrid.org; do
    install -D -m 444 keys/$dom.pub $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/keys/$dom.pub
    install -D -m 444 domain.d/$dom.conf $RPM_BUILD_ROOT%{_sysconfdir}/cvmfs/domain.d/$dom.conf
done
%files
%dir %{_sysconfdir}/cvmfs
%dir %{_sysconfdir}/cvmfs/keys
%dir %{_sysconfdir}/cvmfs/domain.d
%{_sysconfdir}/cvmfs/serverorder.sh
%{_sysconfdir}/cvmfs/keys/*.pub
%config %{_sysconfdir}/cvmfs/domain.d/*.conf
%postun
if [ $1 = 0 ]; then rm -f %{_sysconfdir}/cvmfs/domain.d/*.serverorder; fi

%changelog
* Thu Jun 18 2014 Dave Dykstra <dwd@fnal.gov> - 1.5-1
- Add egi and osg keys and config, and move in cern config from cvmfs
  package.  Add automated server ordering to egi and osg config.
  Cannot change cern config because rpm only allows the same file to
  be in two packages at once if the contents are the same.
