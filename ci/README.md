# CernVM-FS specific Continuous Integration Scrips
This directory contains build scripts for CernVM-FS and its related packages. They are targeted to be used for continuous integration systems like Jenkins (that is currently used at CERN). However, they shall be portable and therefore not contain any CI system specific logic. For testing purposes those scripts can also be run from the command line.

## Build Script Naming Convention
To simplify the integration with CI systems most of the scripts in this directory follow a naming and parameter convention: `${package_identifier}_${package_type}.sh ${source_location} ${build_location} $@`.

## Scripts
* `build_cvmfs_???.sh <source location> <build location>`
  * builds the CernVM-FS main packages (cvmfs, cvmfs-server, cvmfs-unittests). Depending on the target platform there might be different packages or individual packages might be omitted.
* `build_config_???.sh <source location> <build location> [<nightly number>]`
  * builds the cvmfs-config-default and cvmfs-config-none packages
* `build_incremental_multi.sh <source location> [<number of cores to use>]`
  * performs an incremental build without producing any packages. This script is meant to be platform independent and should not install anything into system paths.
