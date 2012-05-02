
%{?suse_version:%define dist .suse%suse_version}
%if 0%{?el6} || 0%{?fc16}
%define selinux_cvmfs 1
%define selinux_variants mls strict targeted
%endif

Summary: CernVM File System
Name: cvmfs
Version: 2.1.0
Release: 1%{?dist}
Source0: https://ecsft.cern.ch/dist/cvmfs/%{name}-%{version}.tar.gz
%if 0%{?selinux_cvmfs}
Source1: cvmfs.te
%endif
Group: Applications/System
License: BSD
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires: cmake
BuildRequires: fuse-devel
BuildRequires: pkgconfig 
BuildRequires: openssl-devel
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
Requires: fuse-libs
Requires: curl
Requires: attr 
# Account for different package names
%if 0%{?suse_version}
Requires: aaa_base
Requires: glibc
Requires: insserv
Requires: util-linux
Requires: pwdutils
Requires(preun): aaa_base insserv
%else
Requires: chkconfig
Requires: glibc-common
Requires: initscripts
Requires: which
Requires: shadow-utils
Requires(preun): chkconfig initscripts
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

%prep
%setup -q

%if 0%{?selinux_cvmfs}
mkdir SELinux
cp %{SOURCE1} SELinux
%endif

%build
%ifarch x86_64
%cmake -DBUILD_SERVER=no .
%else
export CFLAGS="-march=i686" 
export CXXFLAGS="-march=i686"
%cmake -DBUILD_SERVER=no .
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
rm -rf $RPM_BUILD_ROOT

make DESTDIR=$RPM_BUILD_ROOT install
mkdir -p $RPM_BUILD_ROOT/var/lib/cvmfs
mkdir -p $RPM_BUILD_ROOT/cvmfs
mkdir -p $RPM_BUILD_ROOT/etc/cvmfs/config.d

# Keys are in cvmfs-keys
rm -f $RPM_BUILD_ROOT/etc/cvmfs/keys/*

# Fix docdir on SuSE
%if 0%{?suse_version}
mkdir -p %RPM_BUILD_ROOT/usr/share/doc/package/%{name}
mv $RPM_BUILD_ROOT/usr/share/doc/%{name}-%{version} %RPM_BUILD_ROOT/usr/share/doc/package/%{name}
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
/sbin/chkconfig --add cvmfs >/dev/null 2>&1 || :

%if 0%{?selinux_cvmfs}
# Install SELinux policy modules
for selinuxvariant in %{selinux_variants}
do
  /usr/sbin/semodule -s ${selinuxvariant} -i \
    %{_datadir}/selinux/${selinuxvariant}/cvmfs.pp &> /dev/null || :
done
%endif

%preun
if [ $1 = 0 ] ; then
   /sbin/service cvmfs stop >/dev/null 2>&1
   /sbin/chkconfig --del cvmfs || :
fi

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
%{_bindir}/cvmfs2_debug
%{_bindir}/cvmfs_talk
%{_bindir}/cvmfs_fsck
%{_bindir}/cvmfs_config
%{_sysconfdir}/init.d/cvmfs
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
%config %{_sysconfdir}/cvmfs/default.conf 
%config %{_sysconfdir}/cvmfs/domain.d/cern.ch.conf
%doc COPYING AUTHORS README ChangeLog

%changelog
* Mon Feb 20 2012 Jakob Blomer <jblomer@cern.ch>
- Brought selinux back into main package
* Fri Feb 18 2012 Jakob Blomer <jblomer@cern.ch>
- Included Brian's latest changes: group creation bug fixes, selinux as sub package
* Wed Feb 16 2012 Jakob Blomer <jblomer@cern.ch>
- SuSE compatibility, disabled SELinux for SuSE
* Wed Feb 15 2012 Jakob Blomer <jblomer@cern.ch>
- Small adjustments to run with continueous integration
* Thu Jan 12 2012 Brian Bockelman <bbockelm@cse.unl.edu> - 2.0.13
- Addition of SELinux support.

