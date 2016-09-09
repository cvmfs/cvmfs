#!/bin/bash

# link the cvmfs source tree conveniently into the home directory
[ -L cvmfs ] || ln -s /vagrant cvmfs

# date and time
pacman -Sy ntp
systemctl enable ntpd
systemctl start ntpd

# install dependencies
pacman -Sy --noconfirm \
  base-devel apache libutil-linux fuse attr openssl python curl autofs gdb jq
  libcap lsof rsync valgrind cmake unzip bind-tools

# convenience packages
pacman -Sy --noconfirm screen iftop htop git tig strace vim tree

# enable httpd on boot
systemctl enable httpd
systemctl start httpd
