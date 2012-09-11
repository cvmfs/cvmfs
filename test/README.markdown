# Cvmfs-test #

Cvmfs-test is a test framework for [cvmfs](http://cernvm.cern.ch/portal/filesystem).
It consist of a shell and a daemon. The daemon, receiving command from the shell, is able to start several test case against cvmfs features. It covers both network and non-network related test. For network related test, the daemon is also able to start its own web server, proxy server and dns.

### Installation ###

You can install cvmfs in two ways. The first one, and the suggested one, is to install it together with the latest version of cvmfs itself:

`git clone git://github.com/cvmfs/cvmfs cvmfs`  
`cd cvmfs`  
`cmake -DCMAKE_INSTALL_PREFIX:PATH=/usr -DINSTALL_TEST_SYSTEM=/opt`  
`sudo make install`

Otherwise, if you already have an older version of cvmfs and you want only install cvmfs-test, you can do it as follows:

`git clone git://github.com/ruvolof/cvmfs-test cvmfs-test`  
`cd cvmfs-test`  
`sudo ./Install.pl`  

### Documentation ###

Cvmfs-test comes with a man page that explain its basic usage and how to expand it with new test case. To see the man page, just run

`man cvmfs-test`

after installation. If your system does not have groff installed or lack $MANPATH environment variable (as it happen on CernVM), you could anyway launch cvmfs-test and type `help` at shell prompt.
