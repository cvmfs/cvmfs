# CernVM-FS on Supercomputers

Typically there are two problems in using CernVM-FS on supercomputers.

  1. Individual nodes do not have Internet connectivity
  2. Fuse is not allowed on the individual nodes

These problems can be overcome by preloading a CernVM-FS cache on the shared cluster file system and, subsequently, using the [Parrot connector](http://cernvm.cern.ch/portal/filesystem/parrot) instead of the Fuse module.  In contrast to a plain copy of a CernVM-FS repository to a shared file system, this approach has the following advantages:

  * Millions of synchronized meta-data operations per node (path lookups, in particular) will not drown the shared cluster file system but resolve locally in the parrot-cvmfs clients.
  * The file system is always consistent; applications never see half-synchronized directories.
  * After initial preloading, only change sets need to be transfered to the shared file system.  This is much faster than `rsync`, which always has to browse the entire name space.
  * Identical files are internally de-duplicated.  While space of the order of terabytes is usually not an issue for HPC shared file systems, file system caches benefit from deduplication. It is also possible to preload only specific parts of a repository namespace.
  * Support for extra functionality implemented by CernVM-FS such as versioning and variant symlinks (symlinks resolved according to environment variables).

In the following, we assume a Supercomputer with a shared file system (e.g. Lustre or GPFS) that is accessible from all the normal nodes as well as from one or multiple _login nodes_. Only the login nodes need to have access to the Internet. They will be used to preload the CernVM-FS cache on the shared cluster file system.

## Preloading the CernVM-FS Cache

The [`cvmfs_preload` utility](http://cernvm.cern.ch/portal/filesystem/downloads) is used to preload a CernVM-FS cache.  Internally it uses the same code that used to replicate between CernVM-FS stratum 0 and stratum 1.  The `cvmfs_preload` command is a self-extracting binary with no further dependencies and should work on a majority of x86_64 Linux hosts.

The `cvmfs_preload` command replicates from a stratum 0 (not from a stratum 1). Because this induces significant load on the source server, stratum 0 administrators should be informed before using their server as a source.  As an example, in order to preload the ALICE repository into /shared/cache, one could run from a login node

    cvmfs_preload -u http://hcc-cvmfs-example.unl.edu:8000/cvmfs/alice.cern.ch -r /shared/cache

This will preload the entire repository.  In order to preload only specific parts of the namespace, you can create a _dirtab_ file with path prefixes.  The path prefixes must not involve symbolic links.  An example dirtab file for ALICE could look like

    /example/etc
    /example/x86_64-2.6-gnu-4.8.3/Modules
    /example/x86_64-2.6-gnu-4.8.3/Packages/GEANT3
    /example/x86_64-2.6-gnu-4.8.3/Packages/ROOT
    /example/x86_64-2.6-gnu-4.8.3/Packages/gcc
    /example/x86_64-2.6-gnu-4.8.3/Packages/AliRoot/v5*

The corresponding invokation of `cvmfs_preload` is

    cvmfs_preload -u http://hcc-cvmfs.unl.edu:8000/cvmfs/alice.cern.ch -r /shared/cache \
      -d </path/to/dirtab>

The initial preloading can take several hours to a few days.  Subsequent invokations of the same command only transfer a change set and typically finish within seconds or minutes. These subsequent invokations need to be either done manually when necessary or scheduled for instance with a cron job.

The `cvmfs_preload` command can preload files from multiple repositories
into the same cache directory.

## Access From the Nodes

In order to access a preloaded cache from the nodes, [set the path to the directory as an _Alien Cache_](http://cernvm.cern.ch/portal/filesystem/parrot).  Since there won't be cache misses, parrot or fuse clients do not need to download additional files from the network.

If clients do have network access, they might find a repository version online that is newer than the preloaded version in the cache.  This results in conflicts with `cvmfs_preload` or in errors if the cache directory is read-only.  Therefore, we recommend to explicitly disable network access for the parrot process on the nodes, for instance by setting

    HTTP_PROXY='INVALID-PROXY'

before the invocation of `parrot_run`.

## Compiling `cvmfs_preload` from Sources

In order to compile `cvmfs_preload` from sources, use the `-DCVMFS_PRELOADER=on` cmake option.
