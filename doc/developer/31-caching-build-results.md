# Caching build results

## CCache

`ccache` caches build results during compilation, and  can be very effective to speed up repeated builds of CVMFS (Partly because the build system is a bit inefficient and compiles some objects several times - To be fixed).

`ccache` is easy to install with common package managers and can be set up either via environment variable:

```bash
  export CMAKE_CXX_COMPILER_LAUNCHER=ccache
```

or cmake option: 

```bash
  cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache ..
```

The environment variable is particularly convenient when using the ci/build_package.sh script.

## Installation of Externals

CVMFS vendors some dependencies, and builds and installs them during the CMake configuration step.
The default install prefix is the repository root (where directories externals_build and externals_install will then be created, but it can be configured with the environment variable:

```bash
  export CVMFS_EXTERNALS_PREFIX=$HOME/.ccache
```

This is particularly useful to reuse externals_install when building packages with the ci/build_packages.sh script, which usually builds in its own workspace.

