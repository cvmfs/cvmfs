Name:           cvmfs-release
Version:        5
Release:        1
Summary:        Packages for the CernVM File System

Group:          Applications/System
License:        BSD

# This is a Red Hat maintained package which is specific to
# our distribution.  Thus the source is only available from
# within this srpm.
URL:            http://cvmrepo.s3.cern.ch/cvmrepo/yum
Source0:        http://cvmrepo.s3.cern.ch/cvmrepo/yum/RPM-GPG-KEY-CernVM
Source1:        BSD
Source2:        cernvm.repo

BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildArch:     noarch

Recommends:    ( redhat-release >= 5 or openSUSE-release or sles-release or system-release or fedora-release-common )  


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

%post
if [[ ! -f /etc/os-release ]]; then
  >&2 echo "Warning: could not find /etc/os-release. Assuming this is a RHEL-compatible distribution."
else
. /etc/os-release
if  [[ "$ID_LIKE" == *"suse"* ]]; then
mv /etc/yum.repos.d/cernvm.repo /etc/zypp/repos.d
sed -i 's/EL/suse/g' /etc/zypp/repos.d/cernvm.repo
sed -i 's/releasever/releasever_major/g' /etc/zypp/repos.d/cernvm.repo
fi
if  [[ "$ID" == "amzn" ]]; then
if  [[ "$VERSION" == "2" ]]; then
sed -i 's/$releasever/7/g' /etc/yum.repos.d/cernvm.repo
elif  [[ "$VERSION" == "2023" ]]; then
sed -i 's/$releasever/9/g' /etc/yum.repos.d/cernvm.repo
fi
fi
if  [[ "$ID" == "fedora" ]]; then
sed -i 's/EL/fedora/g' /etc/yum.repos.d/cernvm.repo
fi
fi


%changelog
* Tue Feb 25 2025 Valentin Volkl <vavolkl@cern.ch> - 5-1
- Drop explicit dependence on *-release packages
* Sat Jan 04 2025 Valentin Volkl <vavolkl@cern.ch> - 4-1
- Add second url as mirror
- Add support for fedora, suse, amzn (equiv to centos7/9)
* Fri Jul 08 2022 Jakob Blomer <jblomer@cern.ch> - 3-2
- Update S3 repository mirror URL
* Tue Jul 05 2022 Jakob Blomer <jblomer@cern.ch> - 3-1
- Set repository URL to S3 mirror
- Remove cernvm-kernel repository
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
