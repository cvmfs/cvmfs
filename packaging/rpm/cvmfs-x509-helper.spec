%{?suse_version:%define dist .suse%suse_version}
%define cvmfs_version 2.3.0

Summary: CernVM File System X509 Authz Helper
Name: cvmfs-x509-helper
Version: 0.9
Release: 1%{?dist}
Source0: https://ecsft.cern.ch/dist/cvmfs/cvmfs-%{cvmfs_version}.tar.gz
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%if 0%{?el5}
BuildRequires: buildsys-macros
%endif
BuildRequires: cmake
BuildRequires: gcc-c++
BuildRequires: globus-common-devel
BuildRequires: globus-gsi-callback-devel
BuildRequires: globus-gsi-cert-utils-devel
BuildRequires: globus-gsi-credential-devel
BuildRequires: globus-gsi-sysconfig-devel
BuildRequires: libuuid-devel
BuildRequires: openssl-devel
BuildRequires: pkgconfig
BuildRequires: voms-devel

Requires: cvmfs

%description
Authorization helper to verify X.509 proxy certificates and VOMS membership for
the CernVM-FS client.
See http://cernvm.cern.ch
Copyright (c) CERN

%prep
%setup -q -n cvmfs-%{cvmfs_version}

%build
%ifarch i386 i686
export CXXFLAGS="`echo %{optflags}|sed 's/march=i386/march=i686/'`"
export CFLAGS="`echo %{optflags}|sed 's/march=i386/march=i686/'`"
%endif

%if 0%{?suse_version}
cmake -DCMAKE_INSTALL_LIBDIR:PATH=%{_lib} \
  -DBUILD_CVMFS=no \
  -DBUILD_LIBCVMFS=no
  -DBUILD_SERVER=no \
  -DINSTALL_MOUNT_SCRIPTS=no \
  -DINSTALL_MOUNT_SCRIPTS=no \
  -DINSTALL_BASH_COMPLETION=no \
  -DBUILD_X509_HELPER=yes \
  -DCMAKE_INSTALL_PREFIX:PATH=/usr .
%else
%cmake -DCMAKE_INSTALL_LIBDIR:PATH=%{_lib} \
  -DBUILD_CVMFS=no \
  -DBUILD_LIBCVMFS=no \
  -DBUILD_SERVER=no \
  -DINSTALL_MOUNT_SCRIPTS=no \
  -DINSTALL_MOUNT_SCRIPTS=no \
  -DINSTALL_BASH_COMPLETION=no \
  -DBUILD_X509_HELPER=yes .
%endif

make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install

mv $RPM_BUILD_ROOT/usr/share/doc/cvmfs-%{cvmfs_version} \
   $RPM_BUILD_ROOT/usr/share/doc/%{name}-%{version}

# Fix docdir on SuSE
%if 0%{?suse_version}
mkdir -p %RPM_BUILD_ROOT/usr/share/doc/package/%{name}
mv $RPM_BUILD_ROOT/usr/share/doc/%{name}-%{version} %RPM_BUILD_ROOT/usr/share/doc/package/%{name}
%endif

# Fix docdir on Fedora
%if 0%{?fedora}
rm -rf $RPM_BUILD_ROOT/usr/share/doc/%{name}-%{version}
%endif


%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{_bindir}/cvmfs_x509_helper
%doc COPYING AUTHORS README ChangeLog

%changelog
* Thu Apr 21 2016 Jakob Blomer <jblomer@cern.ch> - 0.9-1
- Initial packaging
