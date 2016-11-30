CVMFS Repository Services
=========================

Build status: [![build status](https://travis-ci.org/cvmfs/cvmfs_services.svg?branch=master)](https://travis-ci.org/cvmfs/cvmfs_services)

CVMFS Repository Services (part of the CernVM File System)

The CVMFS Repository Services is an OTP release consisting of different components:

+ Authorization (auth) - access control, user credentials
+ Leases (lease) - managing leases (locks) on different sub-paths of repositories
+ Gateway (gw) - interface to the Stratum 0 repository
+ Back-end (be) - backend, "business logic", compression and hashing, interaction with gateway
+ Front-end (fe) - REST API, routing
+ Operations and monitoring (om) - logging, metrics, alarms etc.

Build
-----

    $ rebar3 compile

License and copyright
---------------------

See LICENSE in the project root.

