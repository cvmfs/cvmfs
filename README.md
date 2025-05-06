[![Build Status](https://github.com/cvmfs/cvmfs/actions/workflows/ci.yml/badge.svg?branch=devel)](https://github.com/cvmfs/cvmfs/actions/workflows/ci.yml?query=branch%3Adevel) [![Documentation Status](https://readthedocs.org/projects/cvmfs/badge/?version=latest)](http://cvmfs.readthedocs.org/en/latest/?badge=master) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.1010441.svg)](https://doi.org/10.5281/zenodo.1010441)

# CernVM-File System (CernVM-FS)

The CernVM-File System provides a scalable, reliable and low-maintenance
software distribution service. It was developed to assist High Energy Physics
(HEP) collaborations to deploy software on the worldwide-distributed computing
infrastructure used to run data processing applications.
CernVM-FS is implemented as a POSIX read-only file system in user space (a FUSE
module). Files and directories are hosted on standard web servers and mounted
in the universal namespace `/cvmfs`. Internally, CernVM-FS uses
content-addressable storage and Merkle trees in order to maintain file data and
meta-data. CernVM-FS uses outgoing HTTP connections only, thereby it avoids
most of the firewall issues of other network file systems. It transfers data
and meta-data on demand and verifies data integrity by cryptographic hashes.

By means of aggressive caching and reduction of latency, CernVM-FS focuses
specifically on the software use case. Software usually comprises many small
files that are frequently opened and read as a whole. Furthermore, the software
use case includes frequent look-ups for files in multiple directories when
search paths are examined.

Content is published into `/cvmfs` by means of dedicated "release manager
machines". The release manager machines provide a writable CernVM-FS instance
by means of a union file system (e.g., `overlayfs`) on top of the read-only
client. When publishing, the CernVM-FS server tools process new and modified
data from the union file system's writable branch and transform the data into
the CernVM-FS storage format.

CernVM-FS is actively used by small and large scientific collaborations. In many
cases, it replaces package managers and shared software areas on cluster file
systems as means to distribute the software used to process experiment data.

## Non-exhaustive List of Resources Related to CernVM-FS
- [Official Documentation](https://cvmfs.readthedocs.io/en/stable/)
  - Aimed at maintainers of CernVM-FS instances, users and developers. Contains
    many in-depth explanations and a complete list of all (client) configuration
    parameters in the appendix.
- [Quickstart Guide for Developers](doc/developer/10-overview.md)
  - Aimed at developers. Includes how to contribute, code style, many short
    coding examples how to set up a working CernVM-File System,
    and how to test and debug.
- [cvmfs-contrib](https://cvmfs-contrib.github.io/)
  - Community-contributed packages related to CernVM-FS but not maintained by
    the CernVM-FS developer team.
## How to Get in Touch With Us
- [CernVM Forum](https://cernvm-forum.cern.ch/) For support questions, or
  problems you encounter.
- [GitHub Issues](https://github.com/cvmfs/cvmfs/issues) For bug reporting or
  feature requests. This issue tracker is used for all CernVM-FS-related
  repositories.
