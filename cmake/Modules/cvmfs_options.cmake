#
# Options provided to the user
#

option (BUILD_CVMFS             "Build the CernVM-FS FUSE module"                                  ON)
if (MACOSX)
  option (BUILD_LIBCVMFS        "Build the CernVM-FS client library"                               OFF)
  option (BUILD_LIBCVMFS_CACHE  "Build the cache plugin library"                                   OFF)
  option (BUILD_SERVER          "Build writer's end programs"                                      OFF)
  option (BUILD_RECEIVER        "Build the receiver program used with the gateway services"        OFF)
else (MACOSX)
  option (BUILD_LIBCVMFS        "Build the CernVM-FS client library"                               ON)
  option (BUILD_LIBCVMFS_CACHE  "Build the cache plugin library"                                   ON)
  option (BUILD_SERVER          "Build writer's end programs"                                      ON)
  option (BUILD_RECEIVER        "Build the receiver program used with the gateway services"        ON)
endif(MACOSX)
option (BUILD_PRELOADER         "Build the alien cache pre-loader"                                 OFF)
option (BUILD_OCTOPUS           "Build the octopus REST server"                                    OFF)
option (BUILD_SERVER_DEBUG      "Build writer's end programs with debug symbols and debug outputs" OFF)
option (BUILD_UNITTESTS         "Build the CernVM-FS unit test set"                                OFF)
option (BUILD_UNITTESTS_DEBUG   "Build the CernVM-FS unit test set with verbose output and -g"     OFF)
option (BUILD_UBENCHMARKS       "Build the CernVM-FS micro benchmarks"                             OFF)
option (BUILD_QC_TESTS          "Build the QuickCheck property random tests"                       OFF)
option (BUILD_DOCUMENTATION     "Build the CerVM-FS documentation using Doxygen"                   OFF)
option (BUILD_COVERAGE          "Compile to collect code coverage reports"                         OFF)
option (BUILD_ALL               "Build client, server, lib, preload, octopus, unit tests"          OFF)
option (INSTALL_UNITTESTS       "Install the unit test binary (mainly for packaging)"              OFF)
option (INSTALL_UNITTESTS_DEBUG "Install the unit test debug binary"                               OFF)
option (INSTALL_MOUNT_SCRIPTS   "Install CernVM-FS mount tools in /etc and /sbin (/usr/bin)"       ON)
option (INSTALL_PUBLIC_KEYS     "Install public key chain for CERN, EGI, and OSG"                  ON)
option (INSTALL_BASH_COMPLETION "Install bash completion rules for cvmfs* commands in /etc"        ON)

# By default, all the external third-party libraries are built and installed in
# ${CMAKE_SOURCE_DIR}/externals_install. If this variable is set to OFF, these
# libraries are picked up from the system
option (BUILTIN_EXTERNALS       "Use built-in versions of all third-party libraries"               ON)

if (BUILD_ALL)
  set (BUILD_CVMFS ON)
  set (BUILD_UNITTESTS ON)
  if (!MACOSX)
    set (BUILD_SERVER ON)
    set (BUILD_LIBCVMFS ON)
    set (BUILD_LIBCVMFS_CACHE ON)
    set (BUILD_PRELOADER ON)
    set (BUILD_OCTOPUS ON)
  endif (!MACOSX)
endif (BUILD_ALL)

