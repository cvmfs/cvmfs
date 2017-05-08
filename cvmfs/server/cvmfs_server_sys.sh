# This file should implement all platform specific functions. The other components of the
# cvmfs_server script should interact with the underlying system only through the functions
# defined here. This allows simple(r) unit testing of the "cvmfs_server" script

# Returns the major.minor.patch-build kernel version string
cvmfs_sys_uname() {
    uname -r | grep -oe '^[0-9]\+\.[0-9]\+.[0-9]\+-*[0-9]*'
}


cvmfs_sys_file_is_regular() {
    [ -f $1 ]
}


cvmfs_sys_file_is_executable() {
    [ -x $1 ]
}


cvmfs_sys_file_is_empty() {
    [ ! -s $1 ]
}


cvmfs_sys_is_redhat() {
  cvmfs_sys_file_is_regular /etc/redhat-release
}


cvmfs_sys_get_fstype() {
  echo $(df -T /var/spool/cvmfs | tail -1 | awk {'print $2'})
}
