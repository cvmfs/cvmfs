
%{?suse_version:%define dist .suse%suse_version}
%if 0%{?el6} || 0%{?fc17} || 0%{?fc18} || 0%{?fc19} || 0%{?fc20}
%define selinux_cvmfs 1
%define selinux_variants mls strict targeted
%endif
%if 0%{?dist:1}
%else
  %define redhat_major %(cat /etc/issue | head -n1 | tr -cd [0-9] | head -c1)
  %if 0%{?redhat_major} == 4
    %define el4 1
    %define dist .el4
  %endif
%endif

%define __strip /bin/true
%define debug_package %{nil}
%if 0%{?el6} || 0%{?el5} || 0%{?el4}
%define __os_install_post %{nil}
%endif

Summary: CernVM File System
Name: cvmfs
Version: 2.1.18
Release: 1%{?dist}
Source0: https://ecsft.cern.ch/dist/cvmfs/%{name}-%{version}.tar.gz
%if 0%{?selinux_cvmfs}
Source1: cvmfs.te
%endif
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

%if 0%{?el4}
BuildRequires: gcc4
BuildRequires: gcc4-c++
%else
BuildRequires: gcc
BuildRequires: gcc-c++
%endif
BuildRequires: cmake
BuildRequires: fuse-devel
BuildRequires: pkgconfig
BuildRequires: openssl-devel
BuildRequires: libattr-devel
%{?el5:BuildRequires: buildsys-macros}

Requires: bash
Requires: coreutils
Requires: grep
Requires: gawk
Requires: sed
Requires: perl
Requires: sudo
Requires: psmisc
Requires: autofs
Requires: fuse
Requires: curl
Requires: attr
Requires: zlib
Requires: gdb
# Account for different package names
%if 0%{?suse_version}
Requires: libfuse2
Requires: glibc
Requires: util-linux
Requires: pwdutils
  %if 0%{?suse_version} < 1200
Requires: sysvinit
  %else
Requires: sysvinit-tools
  %endif
%else
Requires: fuse-libs
Requires: glibc-common
Requires: which
Requires: shadow-utils
  %if 0%{?el5} || 0%{?el4}
Requires: SysVinit
  %else
Requires: sysvinit-tools
  %endif
%endif
Requires: cvmfs-keys >= 1.2

# SELinux integration
# These are needed to build the selinux policy module.
%if 0%{?selinux_cvmfs}
%global selinux_policyver %(%{__sed} -e 's,.*selinux-policy-\\([^/]*\\)/.*,\\1,' /usr/share/selinux/devel/policyhelp || echo 0.0.0)
BuildRequires:  checkpolicy selinux-policy-devel hardlink selinux-policy-targeted
Requires:       selinux-policy >= %{selinux_policyver}
Requires(post):         /usr/sbin/semodule /usr/sbin/semanage /sbin/fixfiles
Requires(preun):        /sbin/service /usr/sbin/semodule /usr/sbin/semanage /sbin/fixfiles
Requires(postun):       /usr/sbin/semodule
%endif

%description
HTTP File System for Distributing Software to CernVM.
See http://cernvm.cern.ch
Copyright (c) CERN

%package devel
Summary: CernVM-FS static client library
Group: Applications/System
Requires: openssl
%description devel
CernVM-FS static client library for pure user-space use

%package server
Summary: CernVM-FS server tools
Group: Application/System
%if 0%{?suse_version}
Requires: insserv
%else
Requires: initscripts
%endif
Requires: bash
Requires: coreutils
Requires: grep
Requires: sed
Requires: sudo
Requires: psmisc
Requires: curl
Requires: attr
Requires: openssl
Requires: httpd
Requires: cvmfs-keys >= 1.2

Conflicts: cvmfs-server < 2.1

%description server
CernVM-FS tools to maintain Stratum 0/1 repositories

%package unittests
Summary: CernVM-FS unit tests binary
Group: Application/System
Requires: cvmfs-server = %{version}
%description unittests
CernVM-FS unit tests binary.  This RPM is not required except for testing.

%prep
%setup -q

%if 0%{?selinux_cvmfs}
mkdir SELinux
cp %{SOURCE1} SELinux
%endif

%build

%ifarch i386 i686
export CXXFLAGS="`echo %{optflags}|sed 's/march=i386/march=i686/'`"
export CFLAGS="`echo %{optflags}|sed 's/march=i386/march=i686/'`"
%endif

%if 0%{?el4}
export CC=gcc4
export CXX=g++4
%endif

%if 0%{?suse_version}
cmake -DCMAKE_INSTALL_LIBDIR:PATH=%{_lib} -DBUILD_SERVER=yes -DBUILD_SERVER_DEBUG=yes -DBUILD_LIBCVMFS=yes -DBUILD_UNITTESTS=yes -DINSTALL_UNITTESTS=yes -DCMAKE_INSTALL_PREFIX:PATH=/usr .
%else
%cmake -DCMAKE_INSTALL_LIBDIR:PATH=%{_lib} -DBUILD_SERVER=yes -DBUILD_SERVER_DEBUG=yes -DBUILD_LIBCVMFS=yes -DBUILD_UNITTESTS=yes -DINSTALL_UNITTESTS=yes .
%endif

make %{?_smp_mflags}

%if 0%{?selinux_cvmfs}
pushd SELinux
for variant in %{selinux_variants}
do
    make NAME=${variant} -f %{_datadir}/selinux/devel/Makefile
    mv cvmfs.pp cvmfs.pp.${variant}
    make NAME=${variant} -f %{_datadir}/selinux/devel/Makefile clean
done
popd
%endif

%pre
%if 0%{?suse_version}
  /usr/bin/getent group cvmfs >/dev/null
  if [ $? -ne 0 ]; then
    /usr/sbin/groupadd -r cvmfs
  fi
  /usr/bin/getent passwd cvmfs >/dev/null
  if [ $? -ne 0 ]; then
    /usr/sbin/useradd -r -g cvmfs -d /var/lib/cvmfs -s /sbin/nologin -c "CernVM-FS service account" cvmfs
  fi
%else
  /usr/bin/getent passwd cvmfs >/dev/null
  if [ $? -ne 0 ]; then
     /usr/sbin/useradd -r -d /var/lib/cvmfs -s /sbin/nologin -c "CernVM-FS service account" cvmfs
  fi

  # The useradd command will add a cvmfs group too - but we're in trouble if
  # the sysadmin has the cvmfs user, but not the group, pre-created.
  /usr/bin/getent group cvmfs >/dev/null
  if [ $? -ne 0 ]; then
    /usr/sbin/groupadd -r cvmfs
  fi
%endif

/usr/bin/getent group fuse | grep -q cvmfs
if [ $? -ne 0 ]; then
  /usr/sbin/usermod -aG fuse cvmfs > /dev/null 2>&1 || :
fi

%install
export DONT_STRIP=1
rm -rf $RPM_BUILD_ROOT

make DESTDIR=$RPM_BUILD_ROOT install
mkdir -p $RPM_BUILD_ROOT/var/lib/cvmfs
mkdir -p $RPM_BUILD_ROOT/cvmfs
mkdir -p $RPM_BUILD_ROOT/etc/cvmfs/config.d
mkdir -p $RPM_BUILD_ROOT/etc/cvmfs/repositories.d
mkdir -p $RPM_BUILD_ROOT/etc/bash_completion.d

# Keys are in cvmfs-keys
rm -f $RPM_BUILD_ROOT/etc/cvmfs/keys/*

# Fix docdir on SuSE
%if 0%{?suse_version}
mkdir -p %RPM_BUILD_ROOT/usr/share/doc/package/%{name}
mv $RPM_BUILD_ROOT/usr/share/doc/%{name}-%{version} %RPM_BUILD_ROOT/usr/share/doc/package/%{name}
%endif

# Fix docdir on FC20
%if 0%{?fc20}
rm -rf $RPM_BUILD_ROOT/usr/share/doc/%{name}-%{version}
%endif

%if 0%{?selinux_cvmfs}
pushd SELinux
for variant in %{selinux_variants}
do
    install -d $RPM_BUILD_ROOT%{_datadir}/selinux/${variant}
    install -p -m 644 cvmfs.pp.${variant} \
           $RPM_BUILD_ROOT%{_datadir}/selinux/${variant}/cvmfs.pp
done
popd
# Hardlink identical policy module packages together
/usr/sbin/hardlink -cv $RPM_BUILD_ROOT%{_datadir}/selinux
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%post
%if 0%{?selinux_cvmfs}
# Install SELinux policy modules
for selinuxvariant in %{selinux_variants}
do
  /usr/sbin/semodule -s ${selinuxvariant} -i \
    %{_datadir}/selinux/${selinuxvariant}/cvmfs.pp &> /dev/null || :
done
%endif
/sbin/ldconfig
if [ -d /var/run/cvmfs ]; then
  /usr/bin/cvmfs_config reload
fi
:

%preun
%if 0%{?selinux_cvmfs}
if [ $1 = 0 ] ; then
    for variant in %{selinux_variants} ; do
        /usr/sbin/semodule -s ${variant} -r cvmfs &> /dev/null || :
    done
fi
%endif

%postun
if [ $1 -eq 0 ]; then
   #sed -i "/^\/mnt\/cvmfs \/etc\/auto.cvmfs/d" /etc/auto.master
   [ -f /var/lock/subsys/autofs ] && /sbin/service autofs reload >/dev/null
   if [ -e /etc/fuse.conf ]; then
     sed -i "/added by CernVM-FS/d" /etc/fuse.conf
   fi
fi

%if 0%{?selinux_cvmfs}
if [ $1 -eq 0 ]; then
    for variant in %{selinux_variants} ; do
        /usr/sbin/semodule -u %{_datadir}/selinux/${variant}/cvmfs.pp || :
    done
fi
%endif

%files
%defattr(-,root,root)
%{_bindir}/cvmfs2
%{_libdir}/libcvmfs_fuse.so
%{_libdir}/libcvmfs_fuse.so.%{version}
%{_libdir}/libcvmfs_fuse_debug.so
%{_libdir}/libcvmfs_fuse_debug.so.%{version}
%{_bindir}/cvmfs_talk
%{_bindir}/cvmfs_fsck
%{_bindir}/cvmfs_config
%{_sysconfdir}/auto.cvmfs
%{_sysconfdir}/cvmfs/config.sh
%if 0%{?selinux_cvmfs}
%{_datadir}/selinux/mls/cvmfs.pp
%{_datadir}/selinux/strict/cvmfs.pp
%{_datadir}/selinux/targeted/cvmfs.pp
%endif
/sbin/mount.cvmfs
%dir %{_sysconfdir}/cvmfs/config.d
%dir %{_sysconfdir}/cvmfs/domain.d
%dir /cvmfs
%attr(700,cvmfs,cvmfs) %dir /var/lib/cvmfs
%{_sysconfdir}/cvmfs/default.d/README
%config %{_sysconfdir}/cvmfs/default.conf
%config %{_sysconfdir}/cvmfs/domain.d/cern.ch.conf
%dir %{_sysconfdir}/bash_completion.d
%config(noreplace) %{_sysconfdir}/bash_completion.d/cvmfs
%doc COPYING AUTHORS README ChangeLog

%files devel 
%defattr(-,root,root)
%{_libdir}/libcvmfs.a
%{_includedir}/libcvmfs.h
%doc COPYING AUTHORS README ChangeLog

%files server
%defattr(-,root,root)
%{_bindir}/cvmfs_swissknife
%{_bindir}/cvmfs_swissknife_debug
%{_bindir}/cvmfs_suid_helper
%{_bindir}/cvmfs_server 
%{_sysconfdir}/cvmfs/cvmfs_server_hooks.sh.demo
%{_libdir}/libtbb_cvmfs.so
%{_libdir}/libtbb_cvmfs.so.2
%{_libdir}/libtbbmalloc_cvmfs.so
%{_libdir}/libtbbmalloc_cvmfs.so.2
%{_libdir}/libtbb_cvmfs_debug.so
%{_libdir}/libtbb_cvmfs_debug.so.2
%{_libdir}/libtbbmalloc_cvmfs_debug.so
%{_libdir}/libtbbmalloc_cvmfs_debug.so.2
%dir %{_sysconfdir}/cvmfs/repositories.d
%doc COPYING AUTHORS README ChangeLog

%files unittests
%defattr(-,root,root)
%{_bindir}/cvmfs_unittests

%changelog
* Thu Apr 10 2014 Jakob Blomer <jblomer@cern.ch> - 2.1.18
- Add /etc/cvmfs/default.d
* Thu Apr 3 2014 Jakob Blomer <jblomer@cern.ch> - 2.1.18
- Fix for EL6.5 32bit
* Tue Feb 11 2014 Jakob Blomer <jblomer@cern.ch> - 2.1.18
- Fedora 20 compatibility fixes
* Tue Jan 21 2014 Jakob Blomer <jblomer@cern.ch> - 2.1.17
- SL4 compatibility fixes
* Fri Dec 20 2013 Jakob Blomer <jblomer@cern.ch> - 2.1.16
- Add cvmfs_suid_binary
* Thu Nov 14 2013 Jakob Blomer <jblomer@cern.ch> - 2.1.16
- Fixes for ARM builds
* Tue Jun 04 2013 Jakob Blomer <jblomer@cern.ch> - 2.1.12
- Add cvmfs_swissknife_debug binary
- Add cvmfs-unittests package
* Mon Feb 18 2013 Jakob Blomer <jblomer@cern.ch> - 2.1.7
- Added libattr-devel as a build requirement
* Tue Feb 12 2013 Jakob Blomer <jblomer@cern.ch> - 2.1.7
- Avoid reloading when the reload sockets are missing (upgrade from 2.0)
* Tue Jan 29 2013 Jakob Blomer <jblomer@cern.ch> - 2.1.7
- Renamed cvmfs-lib package to cvmfs-devel package
* Tue Jan 15 2013 Jakob Blomer <jblomer@cern.ch>
- Package conflicts with the cvmfs 2.0 branch
* Tue Oct 02 2012 Jakob Blomer <jblomer@cern.ch>
- Added sub packages for server and library
* Wed Sep 12 2012 Jakob Blomer <jblomer@cern.ch>
- Enabled selinux for FC17
- Add sysvinit-tools for /sbin/pidof
* Tue Sep 11 2012 Jakob Blomer <jblomer@cern.ch>
- Compatibility fixes for OpenSuSE
* Mon Feb 20 2012 Jakob Blomer <jblomer@cern.ch>
- Brought selinux back into main package
* Sat Feb 18 2012 Jakob Blomer <jblomer@cern.ch>
- Included Brian's latest changes: group creation bug fixes, selinux as sub package
* Thu Feb 16 2012 Jakob Blomer <jblomer@cern.ch>
- SuSE compatibility, disabled SELinux for SuSE
* Wed Feb 15 2012 Jakob Blomer <jblomer@cern.ch>
- Small adjustments to run with continueous integration
* Thu Jan 12 2012 Brian Bockelman <bbockelm@cse.unl.edu> - 2.0.13
- Addition of SELinux support.

