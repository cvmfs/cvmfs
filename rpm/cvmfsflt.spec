#
# To build against different kernel use: --define "kernel_version VERSION-RELEASE"
# Required: kernel-devel, rpmdevtools, kernel-xen-devel
# Copy slc-kmodtool, cvmfsflt-depmod.conf to /usr/src/redhat/SOURCES

Name:           cvmfsflt
License:        BSD/GPL dual
Summary:        CernVM-FS change log filter for redirfs
Version:        2.0.4
Release:        1
# The source for this package was extracted from 
# https://cernvm.cern.ch/project/trac/downloads/cernvm/%{name}-%{version}.tar.gz
Source0:        cvmfsflt-%{version}.tar.gz  

Source1:        %{name}-depmod.conf
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Group:          System Environment/Kernel
Requires:       %{name}-kmod redirfs-kmod
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
The CernVM-FS filter for redirfs creates a change log that
is used as input vor cvmfs_sync.bin

%prep
%setup -n cvmfsflt-%{version}
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



