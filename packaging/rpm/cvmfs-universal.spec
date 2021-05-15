%{?suse_version:%define dist .suse%suse_version}
%if 0%{?suse_version} == 1315
%define sle12 1
%define dist .sle12
%endif
%if 0%{?suse_version} == 1500
%define sle15 1
%define dist .sle15
%endif
%if 0%{?dist:1}
%else
  %define redhat_major %(cat /etc/issue | head -n1 | tr -cd [0-9] | head -c1)
%endif

%if 0%{?rhel} >= 6 || 0%{?fedora}
%define selinux_cvmfs 1
%define selinux_variants mls strict targeted
%endif
%if 0%{?rhel} >= 7 || 0%{?fedora}
%define selinux_cvmfs_server 1
%endif

%if 0%{?rhel} >= 7 || 0%{?fedora} >= 29
  %if "%{?_arch}" != "aarch64"
    %define build_gateway 1
    %define build_ducc 1
  %endif
%endif
%if 0%{?sle15}
  %define build_gateway 1
  %define build_ducc 1
%endif


# List of platforms that require systemd/autofs fix as described in CVM-1200
%if 0%{?rhel} >= 7 || 0%{?fedora} || 0%{?sle12} || 0%{?sle15}
%define systemd_autofs_patch 1
%endif

# fuse3 is in epel starting with epel6;
# the fuse3 libraries are available in SLES 15, too, but the devel package is missing
%if 0%{?fedora} >= 29 || 0%{?rhel} >= 6
%define build_fuse3 1
%endif

%define cvmfs_python python
%define cvmfs_python_devel python-devel
%define cvmfs_python_setuptools python-setuptools
%if 0%{?el8} || 0%{?fedora} >= 31
%define cvmfs_python python2
%define cvmfs_python_devel python2-devel
%define cvmfs_python_setuptools python2-setuptools
%endif
# On SLES15, we need the python2 interpreter but python3 devel and setuptools
# TODO(jblomer): upgrade all python components to Python3
%if 0%{?sle15}
%define cvmfs_python_devel python3-devel
%define cvmfs_python_setuptools python3-setuptools
%endif

%define cvmfs_go golang
%if 0%{?sle15}
%define cvmfs_go go
%endif

%define hardlink /usr/sbin/hardlink
%if 0%{?fedora} >= 31
%define hardlink /usr/bin/hardlink
%endif

%define __strip /bin/true
%define debug_package %{nil}
%if 0%{?el6} || 0%{?el5} || 0%{?el4}
%define __os_install_post %{nil}
%endif

Summary: CernVM File System
Name: cvmfs
Version: 2.9.0
Release: 1%{?dist}
URL: https://cernvm.cern.ch/fs/
Source0: https://ecsft.cern.ch/dist/cvmfs/%{name}-%{version}/%{name}-%{version}.tar.gz
%if 0%{?selinux_cvmfs}
Source1: cvmfs.te
Source2: cvmfs.fc
%endif
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires: bzip2
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
%if 0%{?build_fuse3}
BuildRequires: fuse3-devel
%endif
BuildRequires: libattr-devel
BuildRequires: openssl-devel
BuildRequires: patch
BuildRequires: pkgconfig
BuildRequires: %{cvmfs_python_devel}
BuildRequires: unzip
BuildRequires: zlib-devel
%if 0%{?rhel} >= 7 || 0%{?fedora} || 0%{?sle12} || 0%{?sle15}
BuildRequires: systemd
%endif

Requires: bash
Requires: coreutils
Requires: grep
Requires: gawk
Requires: sed
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
  %if 0%{?suse_version} < 1500
Requires: pwdutils
  %endif
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
    %if 0%{?fedora} || 0%{?rhel} >= 8
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

%if 0%{?build_fuse3}
%package fuse3
Summary: additional libraries to enable libfuse3 support
Group: Applications/System
Requires: cvmfs = %{version}
Requires: fuse3
Requires: fuse3-libs
%description fuse3
Shared libraries implementing the CernVM-FS fuse module based on libfuse3
%endif

%package devel
Summary: CernVM-FS static client library
Group: Applications/System
Requires: openssl
%description devel
CernVM-FS static client library for pure user-space use

%package server
Summary: CernVM-FS server tools
Group: Application/System
BuildRequires: %{cvmfs_python_devel}
BuildRequires: libcap-devel
BuildRequires: unzip
BuildRequires: %{cvmfs_python_setuptools}
%if 0%{?suse_version}
  %if 0%{?suse_version} < 1500
Requires: insserv
  %else
Requires: libcap-progs
  %endif
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
%if 0%{?sle15}
Requires: libcap2
%else
Requires: libcap
%endif
Requires: lsof
Requires: rsync
Requires: usbutils
Requires: sqlite
%if 0%{?rhel} >= 6 || 0%{?fedora} || 0%{?suse_version} >= 1300
Requires: jq
%endif
%if 0%{?selinux_cvmfs_server}
Requires(post): /usr/sbin/semanage
Requires(postun): /usr/sbin/semanage
%endif

Conflicts: cvmfs-server < 2.1

%description server
CernVM-FS tools to maintain Stratum 0/1 repositories

%package shrinkwrap
Summary: CernVM-FS shrinkwrap utility to export /cvmfs file system trees
Group: Application/System
%description shrinkwrap
CernVM-FS shrinkwrap utility to export /cvmfs file system trees into container
images.

%package unittests
Summary: CernVM-FS unit tests binary
Group: Application/System
%description unittests
CernVM-FS unit tests binary.  This RPM is not required except for testing.

%if 0%{?build_gateway}
%package gateway
Summary: CernVM-FS Repository Gateway
Group: Application/System
BuildRequires: %{cvmfs_go} >= 1.11.4
Requires: cvmfs-server = %{version}
%description gateway
The CernVM-FS repository gateway service enables multiple remote publishers
to write to the same repository.
%endif

%if 0%{?build_ducc}
%package ducc
Summary: ducc: Daemon Unpacking Containers in CVMFS
Group: Application/System
BuildRequires: %{cvmfs_go} >= 1.11.4
%description ducc
Daemon to automatically unpack and expose containers images into CernVM-FS
%endif

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
%if 0%{?el5}
export CFLAGS="$CFLAGS -O0"
export CXXFLAGS="$CXXFLAGS -O0"
%endif
%endif

%if 0%{?el4}
export CC=gcc4
export CXX=g++4
export CFLAGS="$CFLAGS -O0"
export CXXFLAGS="$CXXFLAGS -O0"
%endif

BUILD_GATEWAY=no
%if 0%{?build_gateway}
BUILD_GATEWAY=yes
%endif
BUILD_DUCC=no
%if 0%{?build_ducc}
BUILD_DUCC=yes
%endif

%if 0%{?suse_version}
cmake -DCMAKE_INSTALL_LIBDIR:PATH=%{_lib} \
  -DBUILD_SERVER=yes \
  -DBUILD_SERVER_DEBUG=yes \
  -DBUILD_RECEIVER=yes \
  -DBUILD_RECEIVER_DEBUG=yes \
  -DBUILD_LIBCVMFS=yes \
  -DBUILD_LIBCVMFS_CACHE=yes \
  -DBUILD_SHRINKWRAP=yes \
  -DBUILD_UNITTESTS=yes \
  -DBUILD_GATEWAY=$BUILD_GATEWAY \
  -DBUILD_DUCC=$BUILD_DUCC \
  -DINSTALL_UNITTESTS=yes \
  -DCMAKE_INSTALL_PREFIX:PATH=/usr .
%else
EXTRA_CMAKE_OPTS=""
%if 0%{?el5} || 0%{?el4}
EXTRA_CMAKE_OPTS="-DBUILD_GEOAPI=no"
%endif
%cmake -DCMAKE_INSTALL_LIBDIR:PATH=%{_lib} \
  -DBUILD_SERVER=yes \
  -DBUILD_SERVER_DEBUG=yes \
  -DBUILD_RECEIVER=yes \
  -DBUILD_RECEIVER_DEBUG=yes \
  -DBUILD_LIBCVMFS=yes \
  -DBUILD_LIBCVMFS_CACHE=yes \
  -DBUILD_SHRINKWRAP=yes \
  -DBUILD_UNITTESTS=yes \
  -DBUILD_GATEWAY=$BUILD_GATEWAY \
  -DBUILD_DUCC=$BUILD_DUCC \
  -DINSTALL_UNITTESTS=yes $EXTRA_CMAKE_OPTS .
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

%if 0%{?el4}
%else
%pretrans server
[ -d "/var/spool/cvmfs"  ]          || exit 0
[ -d "/etc/cvmfs/repositories.d/" ] || exit 0

for repo in /var/spool/cvmfs/*; do
  [ -d $repo ] && [ ! -f /etc/cvmfs/repositories.d/$(basename $repo)/replica.conf ] || continue

  if [ -f ${repo}/in_transaction.lock ] || \
     [ -d ${repo}/in_transaction      ] || \
     [ -f ${repo}/in_transaction      ]; then
    echo "     Found open CernVM-FS repository transactions."           >&2
    echo "     Please abort or publish them before updating CernVM-FS." >&2
    exit 1
  fi
done

exit 0
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

%if 0%{?build_gateway}
%pre gateway
if $(systemctl is-active --quiet cvmfs-gateway); then
  systemctl stop cvmfs-gateway
fi
if $(systemctl is-active --quiet cvmfs-gateway@*); then
  systemctl stop cvmfs-gateway@*
fi
%endif

%install
export DONT_STRIP=1
rm -rf $RPM_BUILD_ROOT

make DESTDIR=$RPM_BUILD_ROOT install
mkdir -p $RPM_BUILD_ROOT/var/lib/cvmfs
mkdir -p $RPM_BUILD_ROOT/cvmfs
mkdir -p $RPM_BUILD_ROOT/etc/cvmfs/config.d
mkdir -p $RPM_BUILD_ROOT/etc/cvmfs/repositories.d
mkdir -p $RPM_BUILD_ROOT/etc/bash_completion.d
mkdir -p $RPM_BUILD_ROOT/usr/share/cvmfs-server

# Keys and configs are in cvmfs-config
rm -rf $RPM_BUILD_ROOT/etc/cvmfs/keys/*
rm -f $RPM_BUILD_ROOT/etc/cvmfs/config.d/README
rm -f $RPM_BUILD_ROOT/etc/cvmfs/config.d/*.conf
rm -f $RPM_BUILD_ROOT/etc/cvmfs/domain.d/*.conf
rm -f $RPM_BUILD_ROOT/etc/cvmfs/default.d/*.conf
rm -f $RPM_BUILD_ROOT/etc/cvmfs/serverorder.sh

# Don't install coincidentially built libfuse3 libraries
%if ! 0%{?build_fuse3}
rm -f $RPM_BUILD_ROOT%{_libdir}/libcvmfs_fuse3*
%endif

# Fix docdir on SuSE
%if 0%{?suse_version}
mkdir -p %RPM_BUILD_ROOT/usr/share/doc/package/%{name}
mv $RPM_BUILD_ROOT/usr/share/doc/%{name}-%{version} %RPM_BUILD_ROOT/usr/share/doc/package/%{name}
%endif

# Fix docdir on Fedora
%if 0%{?fedora} || 0%{?rhel} >= 8
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
%{hardlink} -cv $RPM_BUILD_ROOT%{_datadir}/selinux
%endif

%if 0%{?systemd_autofs_patch}
mkdir -p $RPM_BUILD_ROOT/usr/lib/systemd/system/autofs.service.d
cat << EOF > $RPM_BUILD_ROOT/usr/lib/systemd/system/autofs.service.d/50-cvmfs.conf
# Addresses distribution bug in autofs configuration
# See CVM-1200 under https://sft.its.cern.ch/jira/browse/CVM-1200
[Service]
KillMode=process
EOF
%endif

%if 0%{?build_gateway}
mkdir -p $RPM_BUILD_ROOT/var/lib/cvmfs-gateway
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%post
if [ $1 -eq 1 ]; then
   mkdir /cvmfs
   chmod 755 /cvmfs
fi
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
%if 0%{?systemd_autofs_patch}
/usr/bin/systemctl daemon-reload
%endif
if [ -d /var/run/cvmfs ]; then
  /usr/bin/cvmfs_config reload
fi
:

%if 0%{?build_fuse3}
%post fuse3
/sbin/ldconfig
%endif

%post server
/usr/bin/cvmfs_server fix-permissions || :
%if 0%{?selinux_cvmfs_server}
# Port 8000 is also assigned to soundd (CVM-1308)
/usr/sbin/semanage port -m -t http_port_t -p tcp 8000 2>/dev/null || :
%endif
# remove old-style geoip data
rm -f /var/lib/cvmfs-server/geo/*.dat
/sbin/ldconfig

%if 0%{?build_gateway}
%post gateway
systemctl daemon-reload
%endif

%preun
if [ $1 = 0 ] ; then
%if 0%{?selinux_cvmfs}
  for variant in %{selinux_variants} ; do
    /usr/sbin/semodule -s ${variant} -r cvmfs &> /dev/null || :
  done
%endif

  /usr/bin/cvmfs_config umount
fi

%if 0%{?build_gateway}
%preun gateway
if [ $1 = 0 ]; then
  if $(systemctl is-active --quiet cvmfs-gateway); then
    systemctl stop cvmfs-gateway
  fi
  if $(systemctl is-active --quiet cvmfs-gateway@*); then
    systemctl stop cvmfs-gateway@*
  fi
fi
%endif

%postun
if [ $1 -eq 0 ]; then
   [ -f /var/lock/subsys/autofs ] && /sbin/service autofs reload >/dev/null
   if [ -e /etc/fuse.conf ]; then
     sed -i "/added by CernVM-FS/d" /etc/fuse.conf
   fi
   rm -f /etc/systemd/system/autofs.service.d/cvmfs-autosetup.conf
   if grep -q "automatically generated by CernVM-FS" /etc/auto.master.d/cvmfs.autofs 2>/dev/null
   then
     rm -f /etc/auto.master.d/cvmfs.autofs
   fi
   [ -f /var/lock/subsys/autofs ] && /sbin/service autofs reload >/dev/null
   rmdir /cvmfs
fi

%if 0%{?selinux_cvmfs}
if [ $1 -eq 0 ]; then
    for variant in %{selinux_variants} ; do
        /usr/sbin/semodule -u %{_datadir}/selinux/${variant}/cvmfs.pp || :
    done
fi
%endif
/sbin/ldconfig

%postun server
%if 0%{?selinux_cvmfs_server}
if [ $1 -eq 0 ]; then
  /usr/sbin/semanage port -d -t http_port_t -p tcp 8000 2>/dev/null || :
fi
%endif
/sbin/ldconfig

%if 0%{?build_gateway}
%postun gateway
systemctl daemon-reload
%endif


%files
%defattr(-,root,root)
%{_bindir}/cvmfs2
%{_libdir}/libcvmfs_fuse_stub.so
%{_libdir}/libcvmfs_fuse_stub.so.%{version}
%{_libdir}/libcvmfs_fuse.so
%{_libdir}/libcvmfs_fuse.so.%{version}
%{_libdir}/libcvmfs_fuse_debug.so
%{_libdir}/libcvmfs_fuse_debug.so.%{version}
%{_bindir}/cvmfs_talk
%{_bindir}/cvmfs_fsck
%{_bindir}/cvmfs_config
/usr/libexec/cvmfs/auto.cvmfs
/usr/libexec/cvmfs/authz/cvmfs_allow_helper
/usr/libexec/cvmfs/authz/cvmfs_deny_helper
/usr/libexec/cvmfs/cache/cvmfs_cache_ram
/usr/libexec/cvmfs/cache/cvmfs_cache_posix
%{_sysconfdir}/auto.cvmfs
%{_sysconfdir}/cvmfs/config.sh
%if 0%{?selinux_cvmfs}
%{_datadir}/selinux/mls/cvmfs.pp
%{_datadir}/selinux/strict/cvmfs.pp
%{_datadir}/selinux/targeted/cvmfs.pp
%endif
%if 0%{?systemd_autofs_patch}
/usr/lib/systemd/system/autofs.service.d/50-cvmfs.conf
%endif
/sbin/mount.cvmfs
%dir %{_sysconfdir}/cvmfs/config.d
%dir %{_sysconfdir}/cvmfs/domain.d
%attr(700,cvmfs,cvmfs) %dir /var/lib/cvmfs
%{_sysconfdir}/cvmfs/default.d/README
%config %{_sysconfdir}/cvmfs/default.conf
%dir %{_sysconfdir}/bash_completion.d
%config(noreplace) %{_sysconfdir}/bash_completion.d/cvmfs
%doc COPYING AUTHORS README.md ChangeLog

%if 0%{?build_fuse3}
%files fuse3
%defattr(-,root,root)
%{_libdir}/libcvmfs_fuse3_stub.so
%{_libdir}/libcvmfs_fuse3_stub.so.%{version}
%{_libdir}/libcvmfs_fuse3.so
%{_libdir}/libcvmfs_fuse3.so.%{version}
%{_libdir}/libcvmfs_fuse3_debug.so
%{_libdir}/libcvmfs_fuse3_debug.so.%{version}
%doc COPYING AUTHORS README.md ChangeLog
%endif

%files devel
%defattr(-,root,root)
%{_libdir}/libcvmfs.a
%{_libdir}/libcvmfs_cache.a
%{_includedir}/libcvmfs.h
%{_includedir}/libcvmfs_cache.h
%doc COPYING AUTHORS README.md ChangeLog

%files server
%defattr(-,root,root)
%{_bindir}/cvmfs_publish
%{_bindir}/cvmfs_publish_debug
%{_bindir}/cvmfs_receiver
%{_bindir}/cvmfs_receiver_debug
%{_bindir}/cvmfs_swissknife
%{_bindir}/cvmfs_swissknife_debug
%{_bindir}/cvmfs_suid_helper
%{_bindir}/cvmfs_server
%{_bindir}/cvmfs_rsync
%{_libdir}/libcvmfs_server.so
%{_libdir}/libcvmfs_server.so.%{version}
%{_libdir}/libcvmfs_server_debug.so
%{_libdir}/libcvmfs_server_debug.so.%{version}
%{_sysconfdir}/cvmfs/cvmfs_server_hooks.sh.demo
%dir %{_sysconfdir}/cvmfs/repositories.d
/var/www/wsgi-scripts/cvmfs-server/cvmfs-api.wsgi
/usr/share/cvmfs-server/
/var/lib/cvmfs-server/
/var/spool/cvmfs/README
%doc COPYING AUTHORS README.md ChangeLog

%files shrinkwrap
%defattr(-,root,root)
%{_bindir}/cvmfs_shrinkwrap
/usr/libexec/cvmfs/shrinkwrap/spec_builder.py*
%doc COPYING AUTHORS README.md ChangeLog

%files unittests
%defattr(-,root,root)
%{_bindir}/cvmfs_unittests
%{_bindir}/cvmfs_test_cache
%{_bindir}/cvmfs_test_shrinkwrap
%{_bindir}/cvmfs_test_publish
%doc COPYING AUTHORS README.md ChangeLog

%if 0%{?build_gateway}
%files gateway
%{_bindir}/cvmfs_gateway
/usr/libexec/cvmfs-gateway/scripts/run_cvmfs_gateway.sh
%{_unitdir}/cvmfs-gateway.service
%{_unitdir}/cvmfs-gateway@.service
%dir /var/lib/cvmfs-gateway
%config(noreplace) %{_sysconfdir}/cvmfs/gateway/repo.json
%config(noreplace) %{_sysconfdir}/cvmfs/gateway/user.json
%endif

%if 0%{?build_ducc}
%files ducc
%{_bindir}/cvmfs_ducc
%{_unitdir}/cvmfs_ducc.service
%endif

%changelog
* Fri May 7 2021 Jakob Blomer <jblomer@cern.ch> - 2.9.0
- Add gateway sub package
* Tue Apr 27 2021 Michael Brown <mbrown@fensystems.co.uk> - 2.9.0
- Add cvmfs_receiver_debug binary
* Mon Apr 19 2021 Jakob Blomer <jblomer@cern.ch> - 2.9.0
- Add SLES15 support
* Tue Apr 14 2020 Jan Priessnitz <jan.priessnitz@cern.ch> - 2.7.2
- Fix python2-devel dependency for Fedora >=31
- Change to /usr/bin/hardlink for Fedora >=31
* Thu Oct 03 2019 Jakob Blomer <jblomer@cern.ch> - 2.7.0
- Add EL8 support
* Wed Jun 12 2019 Jakob Blomer <jblomer@cern.ch> - 2.7.0
- Remove cvmfs_stratum_agent
* Wed Apr 03 2019 Jakob Blomer <jblomer@cern.ch> - 2.7.0
- Add fuse3 sub package
* Tue Feb 19 2019 Simone Mosciatti <simone.mosciatti@cern.ch> - 2.6.0
- Add ducc sub package
* Wed Sep 26 2018 Jakob Blomer <jblomer@cern.ch> - 2.6.0
- Add shrinkwrap sub package
* Tue Aug 07 2018 Dave Dykstra <dwd@fnal.gov> - 2.5.1
- Add python-setuptools build requirement, and disable building geoapi
  on el4 & el5
* Thu Mar 22 2018 Jakob Blomer <jblomer@cern.ch> - 2.5.0
- Add missing bzip2 build requirement
* Mon Sep 18 2017 Jakob Blomer <jblomer@cern.ch> - 2.5.0
- Add cvmfs_stratum_agent to the cvmfs-server package
* Wed Aug 02 2017 Jakob Blomer <jblomer@cern.ch> - 2.4.0
- Fix dependencies for Fedora >= 25
* Wed Jul 05 2017 Jakob Blomer <jblomer@cern.ch> - 2.4.0
- Assign port 8000 to httpd in selinux configuration - 2.4.0
* Thu Jun 29 2017 Jakob Blomer <jblomer@cern.ch> - 2.4.0
- Add cvmfs_test_cache to unittests sub package
* Tue May 09 2017 Dave Dykstra <dwd@fnal.gov> - 2.4.0
- Add cvmfs_receiver
* Wed Mar 22 2017 Jakob Blomer <jblomer@cern.ch> - 2.4.0
- Update upstream package
* Wed Mar 22 2017 Jakob Blomer <jblomer@cern.ch> - 2.3.5
- Drop systemd patch configuration for autofs where necessary
* Mon Mar 06 2017 Jakob Blomer <jblomer@cern.ch> - 2.3.4
- Remove systemd bugfix configuration file if necessary
* Mon Aug 22 2016 Jakob Blomer <jblomer@cern.ch> - 2.3.1
- Reset cvmfs_swissknife capability if overlayfs is used
* Thu Jul 28 2016 Jakob Blomer <jblomer@cern.ch> - 2.3.1
- Update upstream package
* Thu Jun 30 2016 Jakob Blomer <jblomer@cern.ch> - 2.3.1
- Fix SLES12 dist tag
* Tue May 03 2016 Jakob Blomer <jblomer@cern.ch> - 2.3.0
- No optimiziation on EL5/i686 to prevent faulty atomics
* Fri Apr 29 2016 Jakob Blomer <jblomer@cern.ch> - 2.3.0
- voms-devel not necessary anymore
* Mon Apr 11 2016 Rene Meusel <rene.meusel@cern.ch> - 2.3.0
- Disable open repo transaction check in EL4
* Thu Apr 07 2016 Rene Meusel <rene.meusel@cern.ch> - 2.3.0
- Check for open repo transactions before updating server package
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

