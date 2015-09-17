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
