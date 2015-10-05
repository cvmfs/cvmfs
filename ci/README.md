# CernVM-FS specific Continuous Integration Scrips
This directory contains build scripts for CernVM-FS and its related packages. They are targeted to be used for continuous integration systems like Jenkins (that is currently used at CERN). However, they shall be portable and therefore not contain any CI system specific logic. For testing purposes those scripts can also be run from the command line.

## Build Script Conventions
To simplify the integration with CI systems most of the scripts in this directory follow a naming and parameter convention: `${package_identifier}/${package_type}.sh ${source_location} ${build_location} $@`.
I.e. scripts are grouped in sub directories according to the packages they build and are named after the package type they are building. Their first and second argument are always absolute paths to the CVMFS git repository's root and an empty build result directory. After that an arbitrary number of optional parameters can follow.

## Docker Integration
To simplify the necessary build machine infrastructure we base our builds on [Docker](https://www.docker.com/). Blueprints for all important build machines are provided in the sub directory `docker`.

## Additional Helper Scripts
* `build_on_docker.sh <source location> <build location> <docker image> <script invocation>`
  * lazily builds the specified docker container (name of the docker/<subdirectory>) and invokes the build script inside it. Note that you must not specify <source location> and <build location> for the script to be run!
* `build_package.sh <source location> <build location> <package name>`
  * builds the specified package (cvmfs, cvmfs-auto-setup, cvmfs-config, cvmfs-release) for the currently running platform. Note that not all package types are implemented for all package names.

## Example Usage for a Docker-based Build

```bash
# create a build destination
mkdir /tmp/cvmfs_build

# invoke the build script (assuming $PWD = git repository root)
ci/build_on_docker.sh $(pwd) /tmp/cvmfs_build el6 build_package.sh cvmfs
```
