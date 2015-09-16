#!/bin/bash

die() {
  local msg="$1"
  echo -e "$msg"
  exit 1
}

check_yum_environment() {
  which rpm > /dev/null 2>&1 || die "RPM utility is not available"
  which yum > /dev/null 2>&1 || die "Yum ist not available"
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
