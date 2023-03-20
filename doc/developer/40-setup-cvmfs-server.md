# Setup of CernVM-FS Server

### Prerequisites
- `cvmfs` is installed
- `autofs` is disabled

### Goal
 - Create, modify and delete CernVM-FS repository called `local.test.repo` 

> **_WARNING_** &nbsp;
>  If you do not disable `autofs` before any of the CernVM-FS server manipulation commands you can get in a broken state which can only be resolved by restarting the entire machine! (Independent of if you later on disable `autofs`)
> 
> In case `autofs` ends up in the broken state, try the following first before restarting:
> - `sudo cvmfs_config killall`
> - `sudo cvmfs_wipecache`
> - `sudo systemctl restart autofs`
> - If any of those commands hang, try to manually `umount` all `/cvmfs` repos and try again. If nothing helps: restart

```bash
    ####################################################
    # CREATE repo
    ####################################################
    sudo cvmfs_server mkfs local.test.rep

    ####################################################
    # START MODIFY files - this is done in a transaction
    ####################################################

    # start transaction
    sudo cvmfs_server transaction local.test.repo

    # Perform the file manipulations
    sudo cp /home/myuser/testfile*.txt /cvmfs/local.test.repo/
    sudo rm /cvmfs/local.test.repo/testfile2.txt

    # Finalize transaction
    ## 1) Publish results
    sudo cvmfs_server publish local.test.repo

    ## Or 2) Abort transaction and discard all changes
    sudo cvmfs_server abort local.test.repo

    ####################################################
    # END MODIFY files - this is done in a transaction
    ####################################################

    ####################################################
    # DELETE repo
    ####################################################
    sudo cvmfs_server rmfs local.test.repo

    # if it doesnt work use -f flag
    sudo cvmfs_server rmfs -f local.test.repo
```
