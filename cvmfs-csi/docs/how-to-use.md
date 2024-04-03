# How to use CVMFS CSI driver in Kubernetes

Table of contents:
- [How to use CVMFS CSI driver in Kubernetes](#how-to-use-cvmfs-csi-driver-in-kubernetes)
  * [CVMFS automounts](#cvmfs-automounts)
    + [Example: Automounting CVMFS repositories](#example-automounting-cvmfs-repositories)
    + [Example: Mounting a single CVMFS repository using `subPath`](#example-mounting-a-single-cvmfs-repository-using-subpath)
    + [Example: Mounting single CVMFS repository using `repository` parameter](#example-mounting-single-cvmfs-repository-using-repository-attribute)
  * [Adding CVMFS repository configuration](#adding-cvmfs-repository-configuration)
    + [Example: adding ilc.desy.de CVMFS repository](#example-adding-ilcdesyde-cvmfs-repository)
  * [CVMFS mounts with per-volume configuration](#cvmfs-mounts-with-per-volume-configuration)
    + [Example: Mounting a repository snapshot at `CVMFS_REPOSITORY_DATE`](#example-mounting-a-repository-snapshot-at-cvmfs-repository-date)
  * [Troubleshooting](#troubleshooting)
    + [`Too many levels of symbolic links`](#too-many-levels-of-symbolic-links)
    + [`Transport endpoint is not connected` or repository directory empty](#transport-endpoint-is-not-connected-or-repository-directory-empty)

With CVMFS CSI, users can expose CVMFS repositories as PersistentVolume objects and mount those in Pods.

CVMFS CSI supports volume provisioning, however the provisioned volumes only fulfill the role of a reference to CVMFS repositories used inside the CO (e.g. Kubernetes), and are not modifying the CVMFS store in any way.

## CVMFS automounts

Assuming the CVMFS repository configuration is in place, users may mount any CVMFS repository just by accessing its respective directory in the autofs root. autofs will then mount (and automatically unmount, after some time of inactivity) CVMFS repositories on-demand, by simply accessing them (e.g. `cd /cvmfs/atlas.cern.ch`, where `/cvmfs` is the autofs-CVMFS root). To mount a single repository, users can use the [`subPath` directive](https://kubernetes.io/docs/concepts/storage/volumes/#using-subpath) when specifying `volumeMounts`.

With automounts, only a single PersistentVolume/PersistentVolumeClaim pair is needed per Kubernetes namespace to access all CVMFS repositories.

All examples below assume the configuration for accessing CERN's CVMFS repositories is in place (not shown here, as this is the default configuration).

### Example: Automounting CVMFS repositories

This example shows how to create a PVC using which we can mount CVMFS repositories on-demand.

First we define StorageClass and PersistentVolumeClaim that is fulfilled using this StorageClass. Note that if no `repository` parameter is specified in storage class parameters (or PersistentVolume attributes, i.e. `PersistentVolume.spec.csi.volumeAttributes`), automount mode is assumed.

```yaml
# ../example/volume-storageclass-pvc.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: cvmfs
provisioner: cvmfs.csi.cern.ch
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: cvmfs
spec:
  accessModes:
  - ReadOnlyMany
  resources:
    requests:
      # Volume size value has no effect and is ignored
      # by the driver, but must be non-zero.
      storage: 1
  storageClassName: cvmfs
```

```bash
kubectl create -f volume-storageclass-pvc.yaml
```

Second, we define a Pod that mounts the PVC we have just created. The PVC must be mounted with `HostToContainer` mount propagation for automounts to work correctly.

```yaml
# ../example/pod-all-repos.yaml
apiVersion: v1
kind: Pod
metadata:
  name: cvmfs-demo
spec:
  containers:
    - name: demo
      image: busybox
      imagePullPolicy: IfNotPresent
      command: [ "/bin/sh", "-c", "trap : TERM INT; (while true; do sleep 1000; done) & wait" ]
      volumeMounts:
        - name: my-cvmfs
          mountPath: /my-cvmfs
          # CVMFS automount volumes must be mounted with HostToContainer mount propagation.
          mountPropagation: HostToContainer
  volumes:
    - name: my-cvmfs
      persistentVolumeClaim:
        claimName: cvmfs
```

```bash
kubectl create -f pod-all-repos.yaml
```

Now we can try out accessing some cern.ch CVMFS repositories.

```
$ kubectl exec -it cvmfs-demo -- /bin/sh
~ # ls -l /my-cvmfs
total 0

(no CVMFS mounts yet, since we haven't accessed any repos)
(following commands may take some time until the CVMFS caches are populated)

~ # ls -l /my-cvmfs/atlas.cern.ch
total 1
drwxr-xr-x   10 999      997             16 Feb 29  2020 repo

~ # ls -l /my-cvmfs/cms.cern.ch
total 1282
drwxr-xr-x    8 999      997              4 Aug 19  2015 CMS@Home
drwxr-xr-x   19 999      997           4096 Apr 11 08:02 COMP
-rw-rw-r--    1 999      997            429 Feb 12  2016 README
-rw-rw-r--    1 999      997            282 Feb 18  2014 README.cmssw.git
-rw-rw-r--    1 999      997             61 Jul 13  2016 README.grid
-rw-r--r--    1 999      997            341 Apr 23  2019 README.lhapdf
-rw-r--r--    1 999      997           3402 Oct 29  2014 README.oo77
-rw-rw-r--    1 999      997            568 Nov 19  2013 README_mic
drwxr-xr-x  142 999      997             46 Aug 16 14:18 SITECONF
drwxr-xr-x    8 999      997             22 Jan 25  2022 alma8_aarch64_gcc11
...
```

### Example: Mounting a single CVMFS repository using `subPath`

This example shows how to mount a single CVMFS repository (or repositories) without having to define a PVC for each.

We start the same way as in the previous example, by creating an "automount" StorageClass and PersistentVolumeClaim. Once that is done, we can create a Pod that mounts the PVC. Note that we still need to define `mountPropagation` in addition to `subPath`.

```yaml
# pod-single-repo-subpath.yaml
apiVersion: v1
kind: Pod
metadata:
  name: cvmfs-alice
spec:
  containers:
    - name: idle
      image: busybox
      imagePullPolicy: IfNotPresent
      command: [ "/bin/sh", "-c", "trap : TERM INT; (while true; do sleep 1000; done) & wait" ]
      volumeMounts:
        - name: my-cvmfs
          subPath: alice.cern.ch
          mountPath: /my-alice-cvmfs
          mountPropagation: HostToContainer
  volumes:
    - name: my-cvmfs
      persistentVolumeClaim:
        claimName: cvmfs
```

```bash
kubectl create -f pod-cvmfs-demo.yaml
```

```
$ kubectl exec cvmfs-alice -- ls -l /my-alice-cvmfs
total 22
drwxrwxr-x    2 999      997             20 May 13 09:32 bin
lrwxrwxrwx    1 999      997             37 Mar 19  2014 calibration -> /cvmfs/alice-ocdb.cern.ch/calibration
drwxr-xr-x    4 999      997             16 May 29  2020 containers
drwxr-xr-x    4 999      997             22 Dec  8  2017 data
lrwxrwxrwx    1 999      997             20 Mar  9  2016 el5-x86_64 -> x86_64-2.6-gnu-4.1.2
drwxrwxr-x    4 999      997             37 Mar 10  2016 el6-x86_64
drwxrwxr-x    4 999      997             37 Mar 10  2016 el7-x86_64
drwxr-xr-x    4 999      997             27 Aug 13  2018 etc
drwxrwxr-x    3 999      997             20 Nov 10  2020 fah
drwxr-xr-x    5 999      997             17 Sep 29  2020 java
drwxr-xr-x    4 999      997             21 Dec 17  2020 noarch
drwxrwxr-x    3 999      997             19 Aug  9  2021 scripts
drwxr-xr-x    3 999      997             25 Jul 23 15:05 sitesonar
drwxr-xr-x    4 999      997              3 Apr 26  2016 ubuntu1404-x86_64
drwxr-xr-x    4 999      997              4 Nov  2  2016 ubuntu1604-x86_64
drwxr-xr-x    4 999      997             21 Dec 20  2012 x86_64-2.6-gnu-4.1.2
drwxrwxr-x    4 999      997           4096 Dec  5  2013 x86_64-2.6-gnu-4.7.2
drwxr-xr-x    4 999      997           4096 Sep 18  2014 x86_64-2.6-gnu-4.8.3
drwxr-xr-x    4 999      997           4096 Mar 18  2015 x86_64-2.6-gnu-4.8.4
```

### Example: Mounting single CVMFS repository using `repository` parameter

This example shows how to mount a single CVMFS repository using a dedicated PVC.

We start by creating StorageClass that sets `repository` storage class parameter, and a PersistentVolumeClaim that uses this class. Once that is done, we can create a Pod that mounts the PVC. Note that in this case no special `volumeMounts` configuration is needed (no `subPath` or `mountPropagation`).

```yaml
# ../example/pod-single-repo-parameter.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: cvmfs-atlas-nightlies
provisioner: cvmfs.csi.cern.ch
parameters:
  # Repository address goes here.
  repository: atlas-nightlies.cern.ch
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: cvmfs-atlas-nightlies
spec:
  accessModes:
  - ReadOnlyMany
  resources:
    requests:
      # Volume size value has no effect and is ignored
      # by the driver, but must be non-zero.
      storage: 1
  storageClassName: cvmfs-atlas-nightlies
```

```
kubectl create -f pvc-atlas-nightlies-default.yaml
```

Create a Pod that mounts the PVC.

```yaml
# ../example/pod-single-repo-parameter.yaml
apiVersion: v1
kind: Pod
metadata:
  name: cvmfs-atlas-nightlies
spec:
  containers:
    - name: idle
      image: busybox
      imagePullPolicy: IfNotPresent
      command: [ "/bin/sh", "-c", "trap : TERM INT; (while true; do sleep 1000; done) & wait" ]
      volumeMounts:
        - name: my-cvmfs-atlas-nightlies
          mountPath: /atlas-nightlies
          # Note that unlike in pod-single-repo-subpath.yaml, in this
          # case we don't set mountPropagation nor subPath.
  volumes:
    - name: my-cvmfs-atlas-nightlies
      persistentVolumeClaim:
        claimName: cvmfs-atlas-nightlies
```

```
kubectl create -f pod-cvmfs-atlas-nightlies.yaml
```

Now we can access the atlas-nightlies.cern.ch repository.

```
$ kubectl exec cvmfs-atlas-nightlies -- ls -l /atlas-nightlies/repo
total 2
drwxrwxr-x    3 999      997             22 Feb 20  2017 data
drwxrwxr-x    5 999      997             25 Oct 15  2020 lcg
drwxrwxr-x   57 999      997             68 Aug 13 03:43 sw
```

## Adding CVMFS repository configuration

All CVMFS client configuration is stored in two ConfigMaps (created in CVMFS CSI's namespace):
* ConfigMap `cvmfs-csi-default-local` mounted under `/etc/cvmfs/default.local` file,
* ConfigMap `cvmfs-csi-config-d` mounted under `/etc/cvmfs/config.d` directory.

To add or change repository configuration, run `kubectl edit configmap cvmfs-csi-config-d` and edit the ConfigMap items accordingly. Note that it may take some time until the updated ConfigMap contents get propagated to all nodes and CVMFS clients to pick up the new changes.

### Example: adding ilc.desy.de CVMFS repository

To add ilc.desy.de CVMFS repository, run `kubectl edit configmap cvmfs-csi-config-d` and add following to the `data` map:

```yaml
...

data:
  ilc.desy.de.conf: |
    CVMFS_SERVER_URL='http://grid-cvmfs-one.desy.de:8000/cvmfs/@fqrn@;http://cvmfs-stratum-one.cern.ch:8000/cvmfs/@fqrn@;http://cvmfs-egi.gridpp.rl.ac.uk:8000/cvmfs/@fqrn@'
    CVMFS_PUBLIC_KEY='/etc/cvmfs/config.d/ilc.desy.de.pub'

  ilc.desy.de.pub: |
    -----BEGIN PUBLIC KEY-----
    MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA3pgrEIimdCPWG9cuhQ0d
    ZWfYxvHRz5hL4HvQlmvikLIlHxs2EApnGyAWdaHAeQ4IiY+JXQnGsS8e5Gr2cZRb
    Y6Ya19GrjMY1wv8fc+uU9kXp7TbHpl3mSQxERG4+wqosN3+IUaPHdnWGP11idOQB
    I0HsJ7PTRk9quFjx1ppkVITZN07+OdGBIzLK6SyDjy49IfL6DVZUH/Oc99IiXg6E
    NDN2UecnnjDEmcvQh2UjGSQ+0NHe36ttQKEnK58GvcSj2reUEaVKLRvPcrzT9o7c
    ugxcbkBGB3VfqSgfun8urekGEHx+vTNwu8rufBkAbQYdYCPBD3AGqSg+Kgi7i/gX
    cwIDAQAB
    -----END PUBLIC KEY-----
```

Save the file and exit the editor. Once the ConfigMap contents get refreshed on the nodes, access the ilc.desy.de CVMFS repository:

```
$ kubectl exec -it cvmfs-demo -- ls -l /my-cvmfs/ilc.desy.de
total 25
drwxr-xr-x   55 999      997           4096 Oct  3  2016 clic
drwxrwxr-x   15 999      997           4096 Jul  1  2015 ilcsoft
lrwxrwxrwx    1 999      997             22 Jul 12  2013 initILCSOFT.sh -> ilcsoft/initILCSOFT.sh
drwxr-xr-x    3 999      997           4096 Aug  1 19:08 key4hep
-rw-r--r--    1 999      997            342 Sep 21  2009 setup_sidsoft.sh
drwxr-xr-x    3 999      997           4096 Jul  1  2015 sidsoft
drwxr-xr-x   11 999      997           4096 Oct  1  2020 sw
-rw-r--r--    1 999      997            887 Apr 20 12:13 test
```

## CVMFS mounts with per-volume configuration

Users can mount CVMFS repositories and supply configuration as a part of volume attributes. This is done by setting following three attributes:

* `clientConfig`: CVMFS client configuration passed to `cvmfs2 -o config=<stored clientConfig>`. See [CVMFS private mount points](https://cvmfs.readthedocs.io/en/stable/cpt-configure.html#sct-privatemount) for more details. Use either `clientConfig` or `clientConfigFilepath`.
* `clientConfigFilepath`: Path to CVMFS client configuration file passed to `cvmfs2 -o config=<stored clientConfig from clientConfigFilepath>`. The file must be accessible to the `singlemount` container (e.g. mounted as a ConfigMap). Use either `clientConfig` or `clientConfigFilepath`.
* `repository`: Repository to mount.
* `sharedMountID`: Optional. Arbirtrary, user-defined identifier. Volumes with matching `sharedMountID` will re-use the same CVMFS mount, saving resources on the node. This is useful for cases when there are multiple volumes describing a single CVMFS configuration-repository pair (e.g. PVCs in multiple Kubernetes namespaces for the same CVMFS repo). The volumes' attributes must be identical. Defaults to `PersistentVolume.spec.csi.volumeHandle`.

Following CVMFS config parameters are set by default:

* `CVMFS_RELOAD_SOCKETS`: `/var/lib/cvmfs.csi.cern.ch/single/<sharedMountID>`

### Example: Mounting a repository snapshot at `CVMFS_REPOSITORY_DATE`

First, create PV and PVC with `clientConfig` and `repository` defined:

```yaml
# ../example/volume-pv-pvc-20220301.yaml

...

      repository: atlas.cern.ch
      sharedMountID: atlas-20220301
      clientConfig: |
        CVMFS_SERVER_URL=http://cvmfs-stratum-one.cern.ch/cvmfs/atlas.cern.ch
        CVMFS_KEYS_DIR=/etc/cvmfs/keys/cern.ch
        CVMFS_HTTP_PROXY=DIRECT
        CVMFS_REPOSITORY_DATE=2022-03-01T00:00:00Z
```

Second, create a Pod that mounts the PVC:

```yaml
# ../example/pod-atlas-20220301.yaml

     volumeMounts:
       - name: atlas-20220301
         mountPath: /atlas.cern.ch
  volumes:
   - name: atlas-20220301
     persistentVolumeClaim:
       claimName: cvmfs-atlas-20220301
```

## Troubleshooting

### `Too many levels of symbolic links`

When accessing a CVMFS repository you may get `Too many levels of symbolic links` error (`ELOOP` error code). This is most likely caused by missing `mountPropagation: HostToContainer` in Pod's `volumeMounts` spec.

Please follow [Example: Automounting CVMFS repositories](#example-automounting-cvmfs-repositories) guide on how to define `volumeMounts` for automounts.

### `Transport endpoint is not connected` or repository directory empty

When accessing a CVMFS repository you may get `Transport endpoint is not connected` error (`ENOTCONN` error code), or an empty directory. This is most likely caused by the CVMFS CSI node plugin Pod having been restarted (e.g. due to a crash, DaemonSet update, etc.), which then means losing FUSE processes that managed the CVMFS mounts, making it impossible to access them again.

To fix this, restart all Pods (`kubectl delete pod ...`) on the affected node that were using CVMFS volumes.

### `Input/output error` when accessing large directories

When accessing a CVMFS directory with large amounts of data inside you may receive the following error (depending on how the `cvmfscatalog` has split the data):

```
ls /cvmfs/foo/bar/baz
ls: can't open '/cvmfs/foo/bar/baz': Input/output error
```

This may mean that the local cache is running out of space. You can check that's the case by looking into the CVMFS client logs.

There are two ways to resolve this:

* Increase the value of [CVMFS_QUOTA_LIMIT](https://cvmfs.readthedocs.io/en/stable/cpt-configure.html#cache-settings) in the `cvmfs-csi-default-local` ConfigMap (or use the Helm value `cache.local.cvmfsQuotaLimit`).
* Set up an [Alien cache volume](https://cvmfs.readthedocs.io/en/stable/cpt-configure.html#alien-cache) and use it with the `alien.cache` Helm chart value.

You can find more details and troubleshooting steps for this issue in <https://github.com/cvmfs/cvmfs-csi/issues/89>.
