%define         debug_package %{nil}

%define         minio_tag RELEASE.2017-09-29T19-16-56Z
%define         minio_subver %(echo %{tag} | sed -e 's/[^0-9]//g')
# define         minio_commitid DEFINEME
%define         minio_import_path github.com/minio/minio

%define         shuttle_version 1.1
# define        shuttle_commitid DEFINEME
%define         shuttle_repo_path github.com/cvmfs/cvmfs
%define         shuttle_import_path %{shuttle_repo_path}/cvmfs/portal

Summary:        CernVM-FS Server Portals Add-Ons
Name:           cvmfs-server-portals
Version:        0.9
Release:        1%{?dist}
Source0:        https://ecsft.cern.ch/cvmfs/cvmfs-shuttle-%{shuttle_version}.tar.gz
Source1:        https://github.com/cvmfs/minio/archive/%{minio_tag}.tar.gz
Group:          Applications/System
License:        BSD
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires: gcc
BuildRequires: golang >= 1.8
Requires: cvmfs-server

%description
The portals extension for the CernVM-FS release manager machine allows for
creating named S3 enpoints into a repository.  It uses the Minio server
for S3 traffic.

%prep
%setup -qc
mkdir -p src/$(dirname %{minio_import_path})
ln -s ../../../cvmfs-minio-%{minio_tag} src/%{minio_import_path}

%setup -TDqa 1
mkdir -p src/$(dirname %{shuttle_repo_path})
ln -s ../../../cvmfs-shuttle-%{shuttle_version} src/%{shuttle_repo_path}

%build
gcc -Wall -g -o cvmfs_runas src/%{shuttle_import_path}/runas.c

export GOPATH=$(pwd)

go build -v \
  -ldflags="-X main.version=%{shuttle_version} -X main.git_hash=%{shuttle_commitid}" \
  -o cvmfs_shuttle %{shuttle_import_path}

minio_tag=%{minio_tag}
minio_version=${minio_tag#RELEASE.}
minio_commitid=%{minio_commitid}
minio_scommitid=$(echo $commitid | head -c12)
minio_prefix=%{minio_import_path}/cmd

MINIO_LDFLAGS="
-X $minio_prefix.Version=$minio_version
-X $minio_prefix.ReleaseTag=$minio_tag
-X $minio_prefix.CommitID=$minio_commitid
-X $minio_prefix.ShortCommitID=$minio_scommitid
"

go build -ldflags "${MINIO_LDFLAGS}" -o cvmfs_minio %{minio_import_path}

# check that version set properly
./cvmfs_minio version | tee v
v=$(awk '/Version:/{print $2}' v)
test "$v" = $minio_version
v=$(awk '/Release-Tag:/{print $2}' v)
test "$v" = $minio_tag
v=$(awk '/Commit-ID:/{print $2}' v)
test "$v" = $minio_commitid

%install
rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT%{_bindir}
install -d ${RPM_BUILD_ROOT}/usr/lib/systemd/system
install -p cvmfs_minio $RPM_BUILD_ROOT%{_bindir}
install -p cvmfs_shuttle $RPM_BUILD_ROOT%{_bindir}
install -p cvmfs_runas $RPM_BUILD_ROOT%{_bindir}
install -p "src/%{shuttle_import_path}/cvmfs-portal@.service" ${RPM_BUILD_ROOT}/usr/lib/systemd/system
install -p "src/%{shuttle_import_path}/cvmfs-shuttle@.service" ${RPM_BUILD_ROOT}/usr/lib/systemd/system

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{_bindir}/cvmfs_minio
%{_bindir}/cvmfs_shuttle
%{_bindir}/cvmfs_runas
/usr/lib/systemd/system/cvmfs-portal@.service
/usr/lib/systemd/system/cvmfs-shuttle@.service

%changelog
* Tue Nov 14 2017 Jakob Blomer <jblomer@cern.ch> - 0.9
- Initial packaging
