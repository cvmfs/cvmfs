# Uninstalling cvmfs-csi driver

The nodeplugin Pods store various resources on the node hosts they are running on:
* autofs mount and the respective inner CVMFS mounts,
* CVMFS client cache.

By default, the nodeplugin Pod leaves autofs and its respective inner mounts on the node
in `/var/cvmfs`. They may need to be unmounted recursively. To do that, you can set
`AUTOFS_TRY_CLEAN_AT_EXIT` environment variable to `true` in nodeplugin's DaemonSet and restart
the Pods. On the next exit, they will be unmounted.

    ```
    kubectl set env daemonset -l app=cvmfs-csi,component=nodeplugin AUTOFS_TRY_CLEAN_AT_EXIT=true
    # Restarting nodeplugin Pods needs attention, as this may break existing mounts.
    # They will be restored once the Pods come back up.
    kubectl delete pods -l app=cvmfs-csi,component=nodeplugin
    ```

The CVMFS client cache is stored by default in `/var/lib/cvmfs.csi.cern.ch/cache`.
This directory is not deleted automatically, and manual intervention is currently needed.
