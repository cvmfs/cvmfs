# Parrot Connector to CernVM-FS

In case you want to access CernVM-FS repositories but the fuse module is not installed neither do you have the permissions to install it yourself, the [parrot](http://ccl.cse.nd.edu/software/parrot) toolkit provides you a means to "mount" CernVM-FS on Linux in pure user space.  Parrot sandboxes your application in a similar way `gdb` sandboxes your application.  But instead of debugging the application, parrot transparently rewrites file system calls and can effectively provide /cvmfs to your application.

This page provides examples on how to start an application in a parrot+cvmfs sandbox.  We recommend the precompiled parrot from the [cctools 5.2.0 package](http://ccl.cse.nd.edu/software/downloadfiles.php).  At the end of the page, there are instructions on how to compile parrot+cvmfs from sources.


## Basics

In order to sandbox a command `<CMD>` with options `<OPTIONS>` in parrot, we run it as

    <PATH>/<TO>/<PARROT>/parrot_run <PARROT_OPTIONS> <CMD> <OPTIONS>

Make sure the right version of parrot is executed by checking `parrot_run -v`.  In addition to the `parrot_run` binary you intend to use, there might be one present on the system or in your `$PATH` environment, which is taken instead.

Export the following environment variables:

    export PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES=yes
    export PARROT_CVMFS_REPO="<default-repositories>"
    export HTTP_PROXY='<SITE HTTP PROXY>'  # or 'DIRECT;' if not on a cluster or grid site

Test parrot with

    parrot_run bash
    ls /cvmfs/alice.cern.ch
    exit


## Non-Default repositories

Repositories that are not available by default from the builtin `<default-repositories>` list can be explicitly added to `PARROT_CVMFS_REPO`.  The repository name, a stratum 1 URL, and the public key of the repository need to be provided.  For instance, in order to add alice-ocdb.cern.ch and ilc.desy.de to the list of repositories, we can write

    export CERN_S1="http://cvmfs-stratum-one.cern.ch/cvmfs"
    export DESY_S1="http://grid-cvmfs-one.desy.de:8000"
    export PARROT_CVMFS_REPO="<default-repositories> \
      alice-ocdb.cern.ch:url=${CERN_S1}/alice-ocdb.cern.ch,pubkey=/etc/cvmfs/keys/cern.ch/cern-it1.cern.ch.pub \
      ilc.desy.de:url=${DESY_S1}/cvmfs/ilc.desy.de,pubkey=/etc/cvmfs/keys/desy.de/desy.de.pub"

given that the repository public keys are in the provided paths.


## LHC Experiment Frameworks

The experiment frameworks of the four large LHC experiments are supposed to run with parrot+cvmfs.  For a demonstration, you can have a look at the corresponding plugins for the `lhc.sh` script from the [CernVM-FS github space](https://github.com/cvmfs/parrot-test).

#### A Note on the CMS specific SITECONF Symbolic Link

Should the symbolic link /cvmfs/cms.cern.ch/SITECONF/local point to one of the directories in /cvmfs/cms.cern.ch/SITECONF/, it is sufficient to set the `CMS_LOCAL_SITE` environment variable, for instance

    export CMS_LOCAL_SITE=T2_CH_CERN_AI

If this symlink should point to a directory outside /cvmfs, it is a bit more difficult because symbolic links in a parrot file system cannot point outside its root directory.  We can use the `--mount` option of parrot to redirect this particular symlink to a local path, like

    parrot_run --mount=/cvmfs/cms.cern.ch/SITECONF/local=/etc/siteconf <CMD> <OPTIONS>


## Debugging

In case of problems, the debug log can often provide helpful information.  In order to activate CernVM-FS specific debug messages, use

    parrot_run -d cvmfs <CMD> <OPTIONS>


## Cache Location

By default, parrot uses a shared CernVM-FS cache for all parrot instances of the same user stored under /tmp/parrot.<user id>.  In order to place the CernVM-FS cache into a different directory, use

    export PARROT_CVMFS_ALIEN_CACHE=</path/to/cache>

In order to share this directory among multiple users, the users have to belong to the same UNIX group.


## Bugs and Problems

For feedback and bug reports, please write to either the [cctools mailing list](http://ccl.cse.nd.edu/software/help/) or to the [CernVM-FS support](https://sft.its.cern.ch/jira/browse/CVM).


## Compiling from Sources

If you want to compile parrot+cvmfs from sources, please use the `libcvmfs-stable` git branch from the [CernVM-FS github repository](https://github.com/cvmfs/cvmfs/tree/libcvmfs-stable).  The `libcvmfs-stable` branch reflects a state between the head of development (`devel`) and the latest released version (`master`).  Compile and install CernVM-FS as usual with the cmake option `-DBUILD_LIBCVMFS=yes`.  This will install the libcvmfs.h and libcvmfs.a files, which parrot needs to pick up.

Use the git branch that reflects the latest released version from the [cctools github repository](https://github.com/cooperative-computing-lab/cctools).  Configure and compile cctools as usual with the `./configure --with-cvmfs-path <CVMFS INSTALL PREFIX>`, e.g. ``./configure --with-cvmfs-path /usr`.

Note that the cvmfs-devel package contains libcvmfs.h and libcvmfs.a but not necessarily the latest known good version.
