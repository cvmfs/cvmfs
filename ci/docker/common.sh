#!/bin/bash

die() {
  local msg="$1"
  echo -e "$msg"
  exit 1
}

check_yum_environment() {
  which rpm > /dev/null 2>&1                    || die "RPM utility is not available"
  which yum > /dev/null 2>&1                    || die "Yum ist not available"
  rpm -q gpg-pubkey | grep -q 1d1e034b-42bfd0c5 || die "CERN's GPG pubkey missing \n\
Please run:                                                                       \n\
cat > /etc/pki/rpm-gpg/RPM-GPG-KEY-cern << EOF                                    \n\
-----BEGIN PGP PUBLIC KEY BLOCK-----                                              \n\
Version: GnuPG v1.2.6 (GNU/Linux)                                                 \n\
                                                                                  \n\
mQGiBEK/0MURBACv5Rm/jRnrbyocW5t43hrjFxlw/DPLTWiA16apk3P2HQQ8F6cs                  \n\
EY/gmNmUf4U8KB6ncxdye/ostSBFJmVYh0YEYUxBSYM6ZFui3teVRxxXqN921jU2                  \n\
GbbWGqqlxbDqvBxDEG95pA9oSiFYalVfjxVv0hrcrAHQDW5DL2b8l48kGwCgnxs1                  \n\
iO7Z/5KRalKSJqKx70TVIUkD/2YkkHjcwp4Nt1pPlKxLaFp41cnCEGMEZVsNIQuJ                  \n\
1SgHyMHKBzMWkD7QHqAeW3Sa9CDAJKoVPHZK99puF8etyUpC/HfmOIF6jwGpfG5A                  \n\
S7YbqHX6vitRlQt1b1aq5K83J8Y0+8WmjZmCQY6+y2KHOPP+zHWKe5TJDeqDnN0j                  \n\
sZsKA/441IF4JJTPEhvRFsPJO5WKg1zGFbxRPKvgi7+YY6pJ0VFbOMcJVMkvSZ2w                  \n\
4QRD+2ets/pRxNhITHfPToMV3lhC8m1Je5fzoSvSixgH/5o9mekWWSW7Uq7U0IWA                  \n\
7OD7RraJRrGxy0Tz3G+exA7svv/zn9TW/BaHFlMHoyyDHOYZmIhhBB8RAgAhBQJC                  \n\
v+/uFwyAEeb+6rc8Txi4s8pfgZAf4xOTel99AgcAAAoJEF4D/eUdHgNLGCgAmwdu                  \n\
KegSOBXpDe061zF8NoN6+OFiAJ9nKo+uC6xBZ9Ey550SmhFCPPA2/rRTQ0VSTiBM                  \n\
aW51eCBTdXBwb3J0IChSUE0gc2lnbmluZyBrZXkgZm9yIENFUk4gTGludXggU3Vw                  \n\
cG9ydCkgPGxpbnV4LnN1cHBvcnRAY2Vybi5jaD6IWgQTEQIAGgUCQr/QxQULBwMC                  \n\
AQMVAgMDFgIBAh4BAheAAAoJEF4D/eUdHgNL/HsAn1ntKwRoSA9L0r8UyF7Zqn3U                  \n\
79m1AJ9Y4NsSE/dlFYLfmf0+baoq7b5asIicBBMBAgAGBQJCv9DjAAoJEPy9YCiW                  \n\
u335GTwEALjUQ7+cHxi0sifstCLoyRYQSu7Eas0M1UD2ZxSQNBnYsx4rDZJk9TmK                  \n\
7QCzR1yRw9aixzZsRlNbed5VPxSzn89PE5m7Sx1bpl89sPgZ4BY95AL2wExyDWRp                  \n\
1ON2+ztYeYtT7ZCkmeM+PBzt6RHR/jo3361faBS+qOkmpiiRWf3XiEYEExECAAYF                  \n\
AkK/0WAACgkQkB/jE5N6X33DFQCgkvy1ruogu5Ibs5CzGY/ALiSJhyAAn3ygn3p/                  \n\
xrNQ8Dy5j4KfgJINoxT4iEYEEBECAAYFAkK/9CcACgkQDIloXtlLxZSiRACdG0kT                  \n\
KlB4X4VBocUyxMReO9e5MvsAoIKWgcJYE8AGmRXjfIisCAzPtVX+iEYEExECAAYF                  \n\
AkK/8oUACgkQtQgG0wyY/52z1ACgkkxNdhHKbEol3Kwka1tICWHMIwIAn3PWJQR0                  \n\
C1MV1+CnT8UupHzxy6J7                                                              \n\
=IUD3                                                                             \n\
-----END PGP PUBLIC KEY BLOCK-----                                                \n\
EOF                                                                               \n\
rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-cern                                      \
"

  rpm -q gpg-pubkey | grep -q f4a80eb5-53a7ff4b || die "CentOS's GPG pubkey missing \n\
Please run:                                                                         \n\
cat > /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7 << EOF                                  \n\
-----BEGIN PGP PUBLIC KEY BLOCK-----                                                \n\
Version: GnuPG v1.4.5 (GNU/Linux)                                                   \n\
                                                                                    \n\
mQINBFOn/0sBEADLDyZ+DQHkcTHDQSE0a0B2iYAEXwpPvs67cJ4tmhe/iMOyVMh9                    \n\
Yw/vBIF8scm6T/vPN5fopsKiW9UsAhGKg0epC6y5ed+NAUHTEa6pSOdo7CyFDwtn                    \n\
4HF61Esyb4gzPT6QiSr0zvdTtgYBRZjAEPFVu3Dio0oZ5UQZ7fzdZfeixMQ8VMTQ                    \n\
4y4x5vik9B+cqmGiq9AW71ixlDYVWasgR093fXiD9NLT4DTtK+KLGYNjJ8eMRqfZ                    \n\
Ws7g7C+9aEGHfsGZ/SxLOumx/GfiTloal0dnq8TC7XQ/JuNdB9qjoXzRF+faDUsj                    \n\
WuvNSQEqUXW1dzJjBvroEvgTdfCJfRpIgOrc256qvDMp1SxchMFltPlo5mbSMKu1                    \n\
x1p4UkAzx543meMlRXOgx2/hnBm6H6L0FsSyDS6P224yF+30eeODD4Ju4BCyQ0jO                    \n\
IpUxmUnApo/m0eRelI6TRl7jK6aGqSYUNhFBuFxSPKgKYBpFhVzRM63Jsvib82rY                    \n\
438q3sIOUdxZY6pvMOWRkdUVoz7WBExTdx5NtGX4kdW5QtcQHM+2kht6sBnJsvcB                    \n\
JYcYIwAUeA5vdRfwLKuZn6SgAUKdgeOtuf+cPR3/E68LZr784SlokiHLtQkfk98j                    \n\
NXm6fJjXwJvwiM2IiFyg8aUwEEDX5U+QOCA0wYrgUQ/h8iathvBJKSc9jQARAQAB                    \n\
tEJDZW50T1MtNyBLZXkgKENlbnRPUyA3IE9mZmljaWFsIFNpZ25pbmcgS2V5KSA8                    \n\
c2VjdXJpdHlAY2VudG9zLm9yZz6JAjUEEwECAB8FAlOn/0sCGwMGCwkIBwMCBBUC                    \n\
CAMDFgIBAh4BAheAAAoJECTGqKf0qA61TN0P/2730Th8cM+d1pEON7n0F1YiyxqG                    \n\
QzwpC2Fhr2UIsXpi/lWTXIG6AlRvrajjFhw9HktYjlF4oMG032SnI0XPdmrN29lL                    \n\
F+ee1ANdyvtkw4mMu2yQweVxU7Ku4oATPBvWRv+6pCQPTOMe5xPG0ZPjPGNiJ0xw                    \n\
4Ns+f5Q6Gqm927oHXpylUQEmuHKsCp3dK/kZaxJOXsmq6syY1gbrLj2Anq0iWWP4                    \n\
Tq8WMktUrTcc+zQ2pFR7ovEihK0Rvhmk6/N4+4JwAGijfhejxwNX8T6PCuYs5Jiv                    \n\
hQvsI9FdIIlTP4XhFZ4N9ndnEwA4AH7tNBsmB3HEbLqUSmu2Rr8hGiT2Plc4Y9AO                    \n\
aliW1kOMsZFYrX39krfRk2n2NXvieQJ/lw318gSGR67uckkz2ZekbCEpj/0mnHWD                    \n\
3R6V7m95R6UYqjcw++Q5CtZ2tzmxomZTf42IGIKBbSVmIS75WY+cBULUx3PcZYHD                    \n\
ZqAbB0Dl4MbdEH61kOI8EbN/TLl1i077r+9LXR1mOnlC3GLD03+XfY8eEBQf7137                    \n\
YSMiW5r/5xwQk7xEcKlbZdmUJp3ZDTQBXT06vavvp3jlkqqH9QOE8ViZZ6aKQLqv                    \n\
pL+4bs52jzuGwTMT7gOR5MzD+vT0fVS7Xm8MjOxvZgbHsAgzyFGlI1ggUQmU7lu3                    \n\
uPNL0eRx4S1G4Jn5                                                                    \n\
=OGYX                                                                               \n\
-----END PGP PUBLIC KEY BLOCK-----                                                  \n\
EOF                                                                                 \n\
rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7                                    \
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
