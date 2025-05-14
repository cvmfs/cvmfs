# The CernVM Filesystem

## Description

The CernVM filesystem (CVMFS) is a global, readonly distributed file system optimized for software delivery - it lets you stream software from a remote server to your local machine on demand! 

CVMFS is a filesystem in userspace that presents files on a remote server as if they were already on a local filesystem, and downloads them on demand. Unlike sshfs and similar tools, CernVM-FS needs the files on the server to be in a special format, the "CVMFS repository", which includes file metadata. The cvmfs_server command can be used to author and add files to CVMFS repositories.

This lets 
