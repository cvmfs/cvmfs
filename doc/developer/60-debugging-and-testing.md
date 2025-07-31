# Debugging


## Live Debugging

### Client
The easiest way of live debugging is to mount the client in debug (`-d`) and foreground (`-f`) using `cvmfs2`.
Mounting with `cvmfs2` allows also to set a few parameters, e.g. `libfuse=` to select `Fuse2` or 
`Fuse3`.

Example mounting with `Fuse3` on mount point `/mnt/test`
```bash
  export CVMFS_REPO=symlink.test.repo
  sudo /usr/bin/cvmfs2  -d -f \
                        -o rw,system_mount,fsname=cvmfs2,allow_other,grab_mountpoint,uid=`id -u cvmfs`,gid=`id -g cvmfs`,libfuse=3 \
                        $CVMFS_REPO \
                        /mnt/test
```

> **_NOTE_** &nbsp;
> The `uid` and `gid` are the `user id` and `group id` of the `cvmfs` user.
> They will be different on every system but the here provided code discovers them automatically.
> 
> A debug version of the client is automatically built. No special build parameters are needed.

### Server
Live debugging of the server is more complicated. For server and server unit tests a special debug build must be created using `BUILD_SERVER_DEBUG=ON`.

To run the server in debug mode, the environment variable `CVMFS_SERVER_DEBUG` must be set. It allows switching between 3 different debug modes 
- `CVMFS_SERVER_DEBUG=1`
  - In case something breaks, provide a GDB prompt
  - Underlying command: `gdb --quiet --eval-command=run --eval-command=quit --args cvmfs_swissknife_debug`
- `CVMFS_SERVER_DEBUG=2`
  - Attach GDB and provide a prompt WITHOUT actual running the program (the user starts the run, allows setting breakpoints first)
  - Underlying command: `gdb --quiet --args cvmfs_swissknife_debug`
- `CVMFS_SERVER_DEBUG=3`
  - Run debug version of the server (without GDB)
  - Underlying command: `cvmfs_swissknife_debug`

For a first lightweight debugging `CVMFS_SERVER_DEBUG=3` is useful because it prints all the debug messages that are normally suppressed.
For serious debugging `CVMFS_SERVER_DEBUG=2` is the preferred choice. `CVMFS_SERVER_DEBUG=1` only opens GDB prompt when exiting the program but in many cases `cvmfs` has proper error handling, exiting gracefully and as such finding the code section responsible for the error is not obvious.

Example command: `CVMFS_SERVER_DEBUG=2 cvmfs_server mkfs my.test.repo`

> **_NOTE_** &nbsp;
> `cvmfs_server mkfs` is a more complicated command than `cvmfs_server transaction`. If the error is not part of the `mkfs`-process we suggest that you create first a repository using a healthy `cvmfs`-version and afterwards debug the faulty version on `cvmfs_server transaction`.
> 
> Server must be built with `BUILD_SERVER_DEBUG=ON`. For unit tests it also helps to build with `BUILD_UNITTESTS_DEBUG=ON`


### Useful commands

All commands require `sudo`

| Command | Description |
|--|--|
| Destructive Operations | |
|`echo 3 > /proc/sys/vm/drop_caches` | Delete local kernel caches|
|`cvmfs_config wipecache`| Wipe CVMFS caches, reload client config|
|`cvmfs_config killall`| Helpful if `autofs` gets stuck or other reason to reset everything related to `cvmfs` |
| Informative Operations |
|`cvmfs_talk -i <repo> internal affairs` | List all internal counters of cvmfs |
|`cvmfs_talk -i <repo> parameters` | List all parameters the client uses |


## Integration Tests

Integration tests can be split in two groups: client tests and server tests.
Client tests are run against some CERN repo, e.g. `/cvmfs/grid.cern.ch`.
Server tests are test that create their own CernVM-FS repository during testing.
Both of them are found in `cvmfs/test/src`.
All tests with a number < 500 are client tests.
And all tests >= 500 are server tests.


### Running Integration Tests

Go to `cvmfs/test` and execute the following

- For client tests, e.g. run test `087-xattrs`
  ```bash
    # CVMFS_TEST_USER = user name executing the command
    CVMFS_TEST_USER=<user> ./run.sh /tmp/cvmfs-integration.log src/087-xattrs
  ```
- For server tests, e.g. run `701-xattr-catalog_counters`
  ```bash 
    # CVMFS_TEST_USER = user name executing the command
    # CVMFS_TEST_REPO = repo created for the test

    CVMFS_TEST_REPO=just.test.repo CVMFS_TEST_USER=<user> ./run.sh /tmp/cvmfs-integration.log src/701-xattr-catalog_counters
  ```


### Writing your own integration tests

Writing your own integration tests is done the following way:

- Decide what type of test you need: client or server test
- Create a new subfolder in `cvmfs/test/src/` with the appropriate number and name
    - Client test = number < 500
    - Server test = number >= 500
- Create a `main` script
    - It is a `bash`-script.
    - It has NO file ending
- Your test can be executed like all the other tests. No compilation of the `cvmfs` source code needed.


> **_Tips_** &nbsp;
> - `return` values must be handed up to the parent function `my_sub_func || return $?`
> - For readability it might be nice to split the test routines in multiple files
>     - Use the line `source ./src/701-xattr-catalog_counters/<filename>` to include another file in the file `main`. It should be positioned after the `cvmfs_test_suites` parameter
> - For `cvmfs_talk` when interacting with locally mounted repository (= *server test*) you have to use the socket of the repository and not the repository name
>   ```bash
>   sudo cvmfs_talk -p ${mntpnt}c/$CVMFS_TEST_REPO/cvmfs_io.$CVMFS_TEST_REPO internal affairs
>   ```        

## Unit Tests

- Unit tests use the [GoogleTest Framework](https://github.com/google/googletest).
- When configuring to include the unit tests with `cmake -D BUILD_UNITTESTS=ON`, the executable for it can be found in `cvmfs/build/test/unittests`.
- Using `-DBUILD_UNITTESTS_DEBUG=on` during the configuration process of `cmake` allows to properly debug the unittests using e.g. `gdb`.

### Running Unit Tests

Normally only a subset of unit tests need to be run. 
This can be done by using the argument `--gtest_filter=` which accepts `*` as wildcard.

```
  ./cvmfs_unittests --gtest_filter=T_CatalogManagerClient*
```

### Writing your own unit test

- Each time you change something in your unit test `cvmfs` needs to be rebuilt (e.g. by running `ninja`)
  - `sudo ninja install` is not necessary

**In C++**
- Unit test belong in `/cvmfs/test/unittests/` 
- They have a file name `t_<name-of-file-to-test>.cc`
- Test functions are defined by `TEST_F(<testClassName>, <funcName>)`
- New test classes must be registered in `cvmfs/test/unittests/CMakeLists.txt`
- To access `protected` and `private` class members of class `XY`
  - In the class `XY` declare the `unit test class` as `friend` using a macro
    ```c++
      FRIEND_TEST(<testClassName>, <funcName>);
    ```
  - In the class `XY` add `#include "duplex_testing.h"`
- Have stdcout for the test 
  ```c++
    #define GTEST_COUT std::cerr << "[ ] [ INFO ]"
  ```
  - Usage 
    ```c++
      GTEST_COUT << "Root catalog hash " << rootcatalog->hash().ToString() << std::endl;
    ```

