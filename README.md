CVMFS Repository Services
=========================

Build status: [![build status](https://travis-ci.org/cvmfs/cvmfs_services.svg?branch=master)](https://travis-ci.org/cvmfs/cvmfs_services)

CVMFS Repository Services (part of the CernVM File System)

The CVMFS Repository Services is an OTP release consisting of different components:

+ Authorization (cvmfs_auth) - access control, user credentials
+ Leases (cvmfs_lease) - managing leases (locks) on different sub-paths of repositories
+ Gateway (cvmfs_gw) - interface to the Stratum 0 repository
+ Back-end (cvmfs_be) - backend, "business logic", compression and hashing, interaction with gateway
+ Front-end (cvmfs_fe) - REST API, routing
+ Operations and monitoring (cvmfs_om) - logging, metrics, alarms etc.

Build
-----

    $ rebar3 compile

License and copyright
---------------------

See LICENSE in the project root.

