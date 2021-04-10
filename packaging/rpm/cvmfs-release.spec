Name:           cvmfs-release
Version:        2
Release:        7
Summary:        Packages for the CernVM File System

Group:          Applications/System
License:        BSD

# This is a Red Hat maintained package which is specific to
# our distribution.  Thus the source is only available from
# within this srpm.
URL:            http://cvmrepo.web.cern.ch/cvmrepo/yum
Source0:        http://cvmrepo.web.cern.ch/cvmrepo/yum/RPM-GPG-KEY-CernVM
Source1:        BSD
Source2:        cernvm.repo

BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildArch:     noarch
Requires:      redhat-release >= 5

%description
This package contains the yum configuration for the CernVM File System packages.

%prep
%setup -q  -c -T
install -pm 644 %{SOURCE0} .
install -pm 644 %{SOURCE1} .

%build


%install
rm -rf $RPM_BUILD_ROOT

#GPG Key
install -Dpm 644 %{SOURCE0} \
    $RPM_BUILD_ROOT%{_sysconfdir}/pki/rpm-gpg/RPM-GPG-KEY-CernVM

# yum
install -dm 755 $RPM_BUILD_ROOT%{_sysconfdir}/yum.repos.d
install -pm 644 %{SOURCE2}  \
    $RPM_BUILD_ROOT%{_sysconfdir}/yum.repos.d

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root,-)
%doc BSD
%config(noreplace) /etc/yum.repos.d/*
/etc/pki/rpm-gpg/*


%changelog
* Sat Apr 10 2021 Jakob Blomer <jblomer@cern.ch> - 2-7
- Fix RPM linter errors
* Tue Apr 05 2016 Jakob Blomer <jblomer@cern.ch> - 2-6
- Update GPG key
* Tue Mar 31 2015 Jakob Blomer <jblomer@cern.ch> - 2-5
- Added cernvm-config repository
- Remove dist tag from release
* Wed Jan 30 2013 Jakob Blomer <jblomer@cern.ch> - 2-3
- Added cvmfs-testing repositories
* Tue Apr 24 2012 Jakob Blomer <jblomer@cern.ch> - 1-1
- Initial package
