#
# To build against different kernel use: --define "kernel_version VERSION-RELEASE"
# Required: kernel-devel, rpmdevtools, kernel-xen-devel
# Copy slc-kmodtool, redirfs-depmod.conf to /usr/src/redhat/SOURCES

%define         svn 20101209svn671
Name:           redirfs
License:        GPLv3
Summary:        Redirecting filesystem kernel module
Version:        0.10
Release:        0.1%{?svn:.%{svn}}%{?dist}
# The source for this package was pulled from upstream's vcs.  Use the
# following commands to generate the tarball:
# svn export -r 671 http://www.redirfs.org/svn/redirfs/trunk/src/redirfs/ redirfs-0.10-20101209svn671
# tar -czvf redirfs-0.10-20101209svn671.tar.gz redirfs-0.10-20101209svn671
Source0:        redirfs-%{version}%{?svn:-%{svn}}.tar.gz  

Source1:        %{name}-depmod.conf
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Group:          System Environment/Kernel
Requires:       %{name}-kmod
Provides:       %{name}-kmod-common = %{?epoch:%{epoch}:}%{version}
BuildArch:      i686 x86_64


Source1111:     slc-kmodtool
BuildRequires:  %kernel_module_package_buildreqs

%define upvar default
%ifarch i686
%define paevar PAE
%endif
%ifarch i686 ia64 x86_64
%define xenvar xen
%endif

%kernel_module_package -s %{SOURCE1111} %{?upvar} %{?xenvar} %{?paevar}

%description
The RedirFS or redirecting file system is a new layer between virtual 
file system switch (VFS) and file system drivers. It is implemented 
as an out-of-kernel module for Linux 2.6 and it provides framework 
allowing modification of file system calls in the VFS layer. 

%prep
%setup -n redirfs-%{version}%{?svn:-%{svn}}
set -- *
mkdir source
mv "$@" source/
mkdir obj

%build

for flavor in %{flavors_to_build} ; do
   rm -rf obj/$flavor
   cp -r source obj/$flavor
   make -C %{kernel_source $flavor} M=$PWD/obj/$flavor
done

%install
export INSTALL_MOD_PATH=$RPM_BUILD_ROOT
export INSTALL_MOD_DIR=extra/%{name}
for flavor in %flavors_to_build ; do
   make -C %{kernel_source $flavor} modules_install \
           M=$PWD/obj/$flavor
done

install -d -m0755 $RPM_BUILD_ROOT/%{_sysconfdir}/depmod.d/
install -m0644 %{SOURCE1} $RPM_BUILD_ROOT/%{_sysconfdir}/depmod.d/%{name}.conf

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%config(noreplace) %{_sysconfdir}/depmod.d/%{name}.conf
%doc source/README

%changelog
* Thu Dec 9 2010 Steve Traylen <steve.traylen@cern.ch> - 0.10-0.1.20101209svn671
- Copy Jaraoslaw Polok's aqpi kernel module package
  and adapt for redirfs



