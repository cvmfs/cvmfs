# Building from source

1) Get the source and create the build directory
  ```
  git clone https://github.com/cvmfs/cvmfs.git
  cd cvmfs
  mkdir build
  cd build
  ```
2) For the installation either
  - follow the [documentation](https://cvmfs.readthedocs.io/en/stable/cpt-quickstart.html#building-from-source) 
  - or read about extra options below

## Replace `make` with `ninja`

`ninja` has the advantage of running parallelized by default and only incrementally rebuilding resources that are affected by (code) changes.

```bash
  # be in directory cvmfs/build
  cmake -G Ninja -D BUILD_UNITTESTS=ON -D BUILD_GATEWAY=ON ../
  ninja
  sudo ninja install
```

## Useful build parameters

All parameters are listed in `cvmfs/cmake/Modules`. 
In particular file `cvmfs/cmake/Modules/cvmfs_options.cmake` contains most of them.
`cmake -LAH` can also be used to list all options and their description.

A non-exhaustive list of useful parameters is listed in the table below.

| Parameter | Function|
|---|---|
|`BUILD_UNITTESTS` | Build test suite|
|`BUILD_UNITTESTS_DEBUG` | Build test suite in debug mode|
|`BUILD_GATEWAY`   | Build gateway/publisher functionality|

Usage
```bash
  cmake -G Ninja -D BUILD_UNITTESTS=ON -D BUILD_GATEWAY=ON ../
```


## Use locally built libraries

CernVM-FS uses multiple external shared libraries.
It might be useful to switch them to a different version not provided by the system.
In that case, the library must be built locally and CernVM-FS must know of them during the build process.
For this, `cvmfs/cmake/Modules` contain scripts to include those external dependencies, which also describe the parameters used to locate the dependencies.

In case locally built libraries are used some additional steps are required.
These extra steps are needed because during the `install` process, `cmake` by default strips links to locally built libraries and replaces them - if possible - by default system libraries. 

To evade this problem, following solutions are possible

- During `cmake` configuration add `-DCMAKE_INSTALL_RPATH_USE_LINK_PATH:BOOL=ON`

or

- Copy respective `libcvmfs*` from `cvmfs/build/cvmfs/` to `/usr/lib/`


### Example: Locally build Fuse3 to have version 3.10

1) Build [libfuse](https://github.com/libfuse/libfuse/) from source 
 - Checkout branch/tag with the desired version
 - Follow build process as described by `libfuse`, including `make install`
 - These should be the default locations
   - Library `/usr/local/lib/x86_64-linux-gnu/`
   - Include `/usr/local/include/`
  
2) Build `cvmfs` using `fuse3`

 - `fuse3` is automatically selected during build process if the proper flags are set
    ```
        export FUSE3_INCLUDE_DIR_PATH=/usr/local/include/
        export FUSE3_LIBRARY_PATH=/usr/local/lib/x86_64-linux-gnu/libfuse3.so.3.10.5
        cmake -G Ninja -DCMAKE_INSTALL_RPATH_USE_LINK_PATH:BOOL=ON \
              -D FUSE3_INCLUDE_DIR=$FUSE3_INCLUDE_DIR_PATH \
              -D FUSE3_LIBRARY=$FUSE3_LIBRARY_PATH \
              ../
        ninja
        sudo ninja install
