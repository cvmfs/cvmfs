# Setup of CernVM-FS Client

### Prerequisites

- `cvmfs` installed
- `local.test.repo` exist as CernVM-FS repository

### Goal

- Mount remote or local repository `local.test.repo` as a client
    - Set client configuration
    - Use different mounting techniques for remote and local

## Client configuration

- General client configuration, repository-independent is located in `/etc/cvmfs/default.conf`
- Repository-specific client Configuration is found in `/etc/cvmfs/conf.d/<myrepo>`
- Configuration files in `/etc/cvmfs/conf.d/<myrepo>` overwrites `/etc/cvmfs/default.conf`
- Configuration file ending on `.local` overwrites `.conf`


### Useful Parameters

List of useful parameters for developing and debugging.
All parameters can be found here

|Name | Description |
|--|--|
|`CVMFS_HTTP_PROXY`           | `DIRECT` for local repo or URL for remote|
|`CVMFS_KCACHE_TIMEOUT`       | Kernel cache timeout in seconds|
|`CVMFS_MAX_TTL`              | Catalog Time-To-Live in minutes|
|`CVMFS_HIDE_MAGIC_XATTRS=no` | Make all extended attributes available|

## Mounting

Default mounting of CernVM-FS repositories as client is done via `autofs`.
However, when accessing a local repository the preferred method is NOT to use `autofs` as it interferes with `cvmfs_server`.
Instead, `autofs` should be disabled and a separate mountpoint for the read-only client access should be used. 

First time setup
```bash
  sudo cvmfs_config setup
  cvmfs_config chksetup # should return --> OK
```
    


### Mounting remote CernVM-FS repository (default)

Example: Mount repository `sft.cern.ch`

- Create `/etc/cvmfs/config.d/sft.cern.ch.local` with the following input
  ```py
    CVMFS_HTTP_PROXY='http://ca-proxy-sft.cern.ch:3128;http://ca-proxy.cern.ch:3128'
  ```

- Access the repository via automount function of `autofs`
  ```py
    sudo systemctl start autofs # if autofs is not running

    ls /cvmfs/lhcb.cern.ch/
  ```



### Mounting local CernVM-FS repository (special)
*(this method is of course also possible for remote repositories)*

DO NOT use `autofs` if you also want to manipulate the server part via `cmvfs_server`. 
The reason is that `overlayfs` used by `cvmfs_server` mounts to the same point as `autofs` and *that is a really bad idea*.

<u>**Instead mount the local cvmfs client to a different location (not /cvmfs).**</u>

**Example: Mount local repository `local.test.repo`**

- Create `/etc/cvmfs/config.d/local.test.repo.local` with the following input
  ```py
    CVMFS_SERVER_URL=http://localhost/cvmfs/symlink.test.repo
    CVMFS_HTTP_PROXY=DIRECT
  ```

- Mount repository to mountpoint `/mnt/test` 
  (section Debugging has code for more verbose mounting)
  ```bash    
    mkdir /mnt/test
    mount -t cvmfs local.test.repo  /mnt/test
  ```

- Access repository
  ```    
    ls /mnt/test
  ```