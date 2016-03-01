[![Build Status](https://travis-ci.org/cvmfs/cvmfs.svg?branch=devel)](https://travis-ci.org/cvmfs/cvmfs) [![Documentation Status](https://readthedocs.org/projects/cvmfs/badge/?version=latest)](http://cvmfs.readthedocs.org/en/latest/?badge=master)

CVMFS is a FUSE module which implements an HTTP read-only filesystem. The idea
is based on the GROW-FS filesystem, which makes use of Parrot, a component of
the CCTools package developed at the University of Notre Dame.

CVMFS presents a remote HTTP directory as a local file system, in which the
client has read access to all available files. On its first access request, a
file is downloaded and cached locally. All downloaded pieces are verified with
SHA-1.

To do so, a directory structure is transformed into a CVMFS "repository", a
form of content-addressable storage.

This preparation of directories is transparent to web servers and web proxies,
which serve only static content, i.e., arbitrary files. Any HTTP server can do
the job.

CVMFS was created chiefly for the delivery of experiment software stacks for
the LHC experiments at CERN; development continues to address the software
distribution needs of experiments worldwide.
