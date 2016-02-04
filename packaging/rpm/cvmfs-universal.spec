
%{?suse_version:%define dist .suse%suse_version}
%if 0%{?el6} || 0%{?el7} || 0%{?fedora}
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
Version: 2.3.0
Release: 1%{?dist}
Source0: https://ecsft.cern.ch/dist/cvmfs/%{name}-%{version}.tar.gz
%if 0%{?selinux_cvmfs}
Source1: cvmfs.te
Source2: cvmfs.fc
%endif
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

# Build with voms-devel on Fedora / RHEL derivatives.
# Note that we *load* VOMS at runtime, not link against it; this means that
# the produced RPM will not depend on VOMS.
%if 0%{?suse_version}
# TODO(bbockelm): figure out solution for VOMS on SUSE.
%else
BuildRequires: voms-devel
%endif

%if 0%{?el5}
BuildRequires: buildsys-macros
%endif
%if 0%{?el5} || 0%{?el4}
BuildRequires: e2fsprogs-devel
%else
BuildRequires: libuuid-devel
%endif
%if 0%{?el4}
BuildRequires: gcc4
BuildRequires: gcc4-c++
%else
BuildRequires: gcc
BuildRequires: gcc-c++
BuildRequires: valgrind-devel
%endif
BuildRequires: cmake
BuildRequires: fuse-devel
BuildRequires: libattr-devel
BuildRequires: openssl-devel
BuildRequires: patch
BuildRequires: pkgconfig
BuildRequires: python-devel
BuildRequires: unzip

Requires: bash
Requires: coreutils
Requires: grep
Requires: gawk
Requires: sed
Requires: perl
Requires: psmisc
Requires: autofs
Requires: fuse
Requires: curl
Requires: attr
Requires: zlib
Requires: gdb
# Account for different package names
%if 0%{?suse_version}
Requires: aaa_base
Requires: libfuse2
Requires: glibc
Requires: pwdutils
  %if 0%{?suse_version} < 1200
Requires: sysvinit
  %else
Requires: sysvinit-tools
  %endif
%else
Requires: chkconfig
Requires: fuse-libs
Requires: glibc-common
Requires: which
Requires: shadow-utils
  %if 0%{?el5} || 0%{?el4}
Requires: SysVinit
Requires: e2fsprogs
  %else
    %if 0%{?fedora}
Requires: procps-ng
    %else
Requires: sysvinit-tools
    %endif
    %if 0%{?el6}
Requires: util-linux-ng
    %else
Requires: util-linux
    %endif
  %endif
%endif
Requires: cvmfs-config

# SELinux integration
# These are needed to build the selinux policy module.
%if 0%{?selinux_cvmfs}
%{!?_selinux_policy_version: %global _selinux_policy_version %(sed -e 's,.*selinux-policy-\\([^/]*\\)/.*,\\1,' /usr/share/selinux/devel/policyhelp 2>/dev/null)}
%if "%{_selinux_policy_version}" != ""
Requires:      selinux-policy >= %{_selinux_policy_version}
%endif
BuildRequires:  checkpolicy selinux-policy-devel hardlink selinux-policy-targeted
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
BuildRequires: python-devel
BuildRequires: libcap-devel
BuildRequires: unzip
%if 0%{?suse_version}
Requires: insserv
%else
Requires: initscripts
%endif
Requires: bash
Requires: coreutils
Requires: grep
Requires: sed
Requires: psmisc
Requires: curl
Requires: gzip
Requires: attr
Requires: openssl
Requires: httpd
Requires: libcap
Requires: lsof
Requires: rsync
%if 0%{?el6} || 0%{?el7} || 0%{?fedora} || 0%{?suse_version} >= 1300
# this is 'nice-to-have' at the moment
# TODO(rmeusel): consider using 'Recommends:' in the far future
Requires: jq
%endif

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
cp %{SOURCE1} %{SOURCE2} SELinux
%endif

%build

%ifarch i386 i686
export CXXFLAGS="`echo %{optflags}|sed 's/march=i386/march=i686/'`"
export CFLAGS="`echo %{optflags}|sed 's/march=i386/march=i686/'`"
%endif

%if 0%{?el4}
export CC=gcc4
export CXX=g++4
export CFLAGS="$CFLAGS -O0"
export CXXFLAGS="$CXXFLAGS -O0"
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

# Keys and configs are in cvmfs-config
rm -rf $RPM_BUILD_ROOT/etc/cvmfs/keys/*
rm -f $RPM_BUILD_ROOT/etc/cvmfs/config.d/*.conf
rm -f $RPM_BUILD_ROOT/etc/cvmfs/domain.d/*.conf
rm -f $RPM_BUILD_ROOT/etc/cvmfs/default.d/*.conf
rm -f $RPM_BUILD_ROOT/etc/cvmfs/serverorder.sh

# Fix docdir on SuSE
%if 0%{?suse_version}
mkdir -p %RPM_BUILD_ROOT/usr/share/doc/package/%{name}
mv $RPM_BUILD_ROOT/usr/share/doc/%{name}-%{version} %RPM_BUILD_ROOT/usr/share/doc/package/%{name}
%endif

# Fix docdir on Fedora
%if 0%{?fedora}
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
restorecon -R /var/lib/cvmfs
%endif
/sbin/ldconfig
if [ -d /var/run/cvmfs ]; then
  /usr/bin/cvmfs_config reload
fi
:

%preun
if [ $1 = 0 ] ; then
%if 0%{?selinux_cvmfs}
  for variant in %{selinux_variants} ; do
    /usr/sbin/semodule -s ${variant} -r cvmfs &> /dev/null || :
  done
%endif

  /usr/bin/cvmfs_config umount
fi

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
/usr/libexec/cvmfs/auto.cvmfs
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
%{_bindir}/cvmfs_rsync
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
/var/www/wsgi-scripts/cvmfs-api.wsgi
/usr/share/cvmfs-server/
/var/lib/cvmfs-server/
/var/spool/cvmfs/README
%doc COPYING AUTHORS README ChangeLog

%files unittests
%defattr(-,root,root)
%{_bindir}/cvmfs_unittests
%doc COPYING AUTHORS README ChangeLog

%changelog
* Sat Jan 23 2016 Brian Bockelman <bbockelm@cse.unl.edu> - 2.2.0
- Build with VOMS support
* Thu Jan 21 2016 Jakob Blomer <jblomer@cern.ch> - 2.2.0
- Remove sudo dependency
* Fri Jan 15 2016 Jakob Blomer <jblomer@cern.ch> - 2.2.0
- Add valgrind-devel except for EL4
* Tue Jan 12 2016 Rene Meusel <rene.meusel@cern.ch> - 2.2.0
- Fix dependency for Fedora 23
* Tue Dec 15 2015 Jakob Blomer <jblomer@cern.ch> - 2.2.0
- Unmount repositories when cvmfs is erased
* Fri Dec 11 2015 Rene Meusel <rene.meusel@cern.ch> - 2.2.0
- Add jq (weak) dependency
* Fri Oct 23 2015 Rene Meusel <rene.meusel@cern.ch> - 2.2.0
- Fix dependency for Fedora 22
- Add lsof dependency for cvmfs-server
* Tue Oct 13 2015 Rene Meusel <rene.meusel@cern.ch> - 2.2.0
- Add libcap dependency for cvmfs-server
* Wed Sep 30 2015 Rene Meusel <rene.meusel@cern.ch> - 2.2.0
- Drop explicit support for Fedora < 21
- Use generic 'fedora' macro name where possible
* Mon Aug 17 2015 Jakob Blomer <jblomer@cern.ch> - 2.2.0
- Avoid rm -f /var/lib/cvmfs-server/geo/* in preuninstall
* Wed Jan 07 2015 Jakob Blomer <jblomer@cern.ch> - 2.1.20
- Add chkconfig dependency
* Wed Dec 10 2014 Jakob Blomer <jblomer@cern.ch> - 2.1.20
- Adjust for new cvmfs-config-... packages
* Wed Dec 10 2014 Jakob Blomer <jblomer@cern.ch> - 2.1.20
- Add libuuid-devel dependency
- Fixes for Fedora 21
* Tue Oct 21 2014 Jakob Blomer <jblomer@cern.ch> - 2.1.20
- /etc/auto.cvmfs is now a link to /usr/libexec/cvmfs/auto.cvmfs
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

