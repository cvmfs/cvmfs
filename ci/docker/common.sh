#!/bin/bash

die() {
  local msg="$1"
  echo -e "$msg"
  exit 1
}

check_yum_environment() {
  which rpm > /dev/null 2>&1                    || die "RPM utility is not available"
  which yum > /dev/null 2>&1                    || die "Yum ist not available"
  rpm -q gpg-pubkey | grep -q 1d1e034b-42bfd0c5 || die "CERN's GPG pubkey is not installed \n\
Please run:                                                                                \n\
cat > /etc/pki/rpm-gpg/RPM-GPG-KEY-cern << EOF                                             \n\
-----BEGIN PGP PUBLIC KEY BLOCK-----                                                       \n\
Version: GnuPG v1.2.6 (GNU/Linux)                                                          \n\
                                                                                           \n\
mQGiBEK/0MURBACv5Rm/jRnrbyocW5t43hrjFxlw/DPLTWiA16apk3P2HQQ8F6cs                           \n\
EY/gmNmUf4U8KB6ncxdye/ostSBFJmVYh0YEYUxBSYM6ZFui3teVRxxXqN921jU2                           \n\
GbbWGqqlxbDqvBxDEG95pA9oSiFYalVfjxVv0hrcrAHQDW5DL2b8l48kGwCgnxs1                           \n\
iO7Z/5KRalKSJqKx70TVIUkD/2YkkHjcwp4Nt1pPlKxLaFp41cnCEGMEZVsNIQuJ                           \n\
1SgHyMHKBzMWkD7QHqAeW3Sa9CDAJKoVPHZK99puF8etyUpC/HfmOIF6jwGpfG5A                           \n\
S7YbqHX6vitRlQt1b1aq5K83J8Y0+8WmjZmCQY6+y2KHOPP+zHWKe5TJDeqDnN0j                           \n\
sZsKA/441IF4JJTPEhvRFsPJO5WKg1zGFbxRPKvgi7+YY6pJ0VFbOMcJVMkvSZ2w                           \n\
4QRD+2ets/pRxNhITHfPToMV3lhC8m1Je5fzoSvSixgH/5o9mekWWSW7Uq7U0IWA                           \n\
7OD7RraJRrGxy0Tz3G+exA7svv/zn9TW/BaHFlMHoyyDHOYZmIhhBB8RAgAhBQJC                           \n\
v+/uFwyAEeb+6rc8Txi4s8pfgZAf4xOTel99AgcAAAoJEF4D/eUdHgNLGCgAmwdu                           \n\
KegSOBXpDe061zF8NoN6+OFiAJ9nKo+uC6xBZ9Ey550SmhFCPPA2/rRTQ0VSTiBM                           \n\
aW51eCBTdXBwb3J0IChSUE0gc2lnbmluZyBrZXkgZm9yIENFUk4gTGludXggU3Vw                           \n\
cG9ydCkgPGxpbnV4LnN1cHBvcnRAY2Vybi5jaD6IWgQTEQIAGgUCQr/QxQULBwMC                           \n\
AQMVAgMDFgIBAh4BAheAAAoJEF4D/eUdHgNL/HsAn1ntKwRoSA9L0r8UyF7Zqn3U                           \n\
79m1AJ9Y4NsSE/dlFYLfmf0+baoq7b5asIicBBMBAgAGBQJCv9DjAAoJEPy9YCiW                           \n\
u335GTwEALjUQ7+cHxi0sifstCLoyRYQSu7Eas0M1UD2ZxSQNBnYsx4rDZJk9TmK                           \n\
7QCzR1yRw9aixzZsRlNbed5VPxSzn89PE5m7Sx1bpl89sPgZ4BY95AL2wExyDWRp                           \n\
1ON2+ztYeYtT7ZCkmeM+PBzt6RHR/jo3361faBS+qOkmpiiRWf3XiEYEExECAAYF                           \n\
AkK/0WAACgkQkB/jE5N6X33DFQCgkvy1ruogu5Ibs5CzGY/ALiSJhyAAn3ygn3p/                           \n\
xrNQ8Dy5j4KfgJINoxT4iEYEEBECAAYFAkK/9CcACgkQDIloXtlLxZSiRACdG0kT                           \n\
KlB4X4VBocUyxMReO9e5MvsAoIKWgcJYE8AGmRXjfIisCAzPtVX+iEYEExECAAYF                           \n\
AkK/8oUACgkQtQgG0wyY/52z1ACgkkxNdhHKbEol3Kwka1tICWHMIwIAn3PWJQR0                           \n\
C1MV1+CnT8UupHzxy6J7                                                                       \n\
=IUD3                                                                                      \n\
-----END PGP PUBLIC KEY BLOCK-----                                                         \n\
EOF                                                                                        \n\
rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-cern                                               \
"
}

fix_yum_config_to_architecture() {
  local yum_repos_d="$1"
  local basearch="$2"

  for f in $(find $yum_repos_d -type f); do
    echo -n "patching ${f}... "
    sed -i -e "s/\$basearch/$basearch/g" $f
    echo "done"
  done
}

setup_base_configuration() {
  local chroot_d="$1"
  cp /etc/resolv.conf ${chroot_d}/etc/resolv.conf                   || return 1
  echo "NETWORKING=yes" > ${chroot_d}/etc/sysconfig/network         || return 2
  chroot $chroot_d ln -f /usr/share/zoneinfo/Etc/UTC /etc/localtime || return 3
  mount --bind /dev ${chroot_d}/dev                                 || return 4
}

recreate_rpm_database() {
  local chroot_d="$1"
  local global_rpm_db_d="$2"

  rm -fR $global_rpm_db_d && mkdir -p $global_rpm_db_d
  chroot $chroot_d /bin/rpm --initdb
  chroot $chroot_d /bin/rpm -ivh --justdb '/var/cache/yum/*/packages/*.rpm'
  rm -r ${chroot_d}/var/cache/yum/
}
