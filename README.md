CVMFS Repository Services
=========================

Gitlab CI: [![build status](https://travis-ci.org/cvmfs/cvmfs_services.svg?branch=master)](https://travis-ci.org/cvmfs/cvmfs_services.svg?branch=master)

CVMFS Repository Services (part of the CernVM File System)

The CVMFS Repository Services (http://github.com/cvmfs/cvmfs_services.git) is an umbrella project
(organized as an OTP release) which hosts the development of the different components of the service
architecture:

+ Authorization service (cvmfs_auth) - access control, user credentials
+ Lease service (cvmfs_lease) - manages leases (locks) on different sub-paths in the repository
+ Gateway service (cvmfs_gw) - interface to the Stratum 0 repository
+ Processing (cvmfs_proc) - backend, "business logic", compression and hashing, interaction with gateway
+ Front-end (cvmfs_fe) - REST API, routing
+ Operations and monitoring (cvmfs_om) - logging, metrics, alarms etc.

Each component is implemented as a separate OTP Application. While they are initially developed
together in the same Git repository, the OTP Application abstraction helps maintain loose coupling
between the different components. If the need arises to split certain components into separate
repositories, this can be done with minimal effort.

Build
-----

    $ rebar3 compile

License and copyright
---------------------

See LICENSE in the project root.

