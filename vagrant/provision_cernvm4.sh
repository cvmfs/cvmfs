#!/bin/bash

# link the cvmfs source tree conveniently into the home directory
[ -L cvmfs ] || ln -s /vagrant cvmfs

# enable httpd on boot
systemctl enable httpd
systemctl start httpd
