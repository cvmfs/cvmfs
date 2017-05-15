[![Build Status](https://travis-ci.org/cvmfs/cvmfs.svg?branch=devel)](https://travis-ci.org/cvmfs/cvmfs) [![Documentation Status](https://readthedocs.org/projects/cvmfs/badge/?version=latest)](http://cvmfs.readthedocs.org/en/latest/?badge=master)

The CernVM-File System (CernVM-FS) provides a scalable, reliable and low-
maintenance software distribution service. It was developed to assist High
Energy Physics (HEP) collaborations to deploy software on the worldwide-
distributed computing infrastructure used to run data processing applications.
CernVM-FS is implemented as a POSIX read-only file system in user space (a FUSE
module). Files and directories are hosted on standard web servers and mounted
in the universal namespace /cvmfs. Internally, CernVM-FS uses content-
addressable storage and Merkle trees in order to maintain file data and
meta-data. CernVM-FS uses outgoing HTTP connections only, thereby it avoids
most of the firewall issues of other network file systems. It transfers data
and meta-data on demand and verifies data integrity by cryptographic hashes.

By means of aggressive caching and reduction of latency, CernVM-FS focuses
specifically on the software use case. Software usually comprises many small
files that are frequently opened and read as a whole. Furthermore, the software
use case includes frequent look-ups for files in multiple directories when
search paths are examined.

Content is published into /cvmfs by means of dedicated "release manager
machines". The release manager machines provide a writeable CernVM-FS instance
by means of a union file system (aufs or overlayfs) on top of the read-only
client. When publishing, the CernVM-FS server tools process new and modified
data from the union file system's writable branch and transform the data into
the CernVM-FS storage format.

CernVM-FS is actively used by small and large scientific collaborations. In many
cases, it replaces package managers and shared software areas on cluster file 
systems as means to distribute the software used to process experiment data.
