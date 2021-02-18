# CVMFS Containerd Snapshotter

**It requires containerd >= 1.4.0-beta.1**

This repository contains a containerd snapshotter that exploits the CernVM-FS to provide the filesystem for the containers.

## Background

From version 1.4.0, containerd introduce the concept of remote snapshotter.
It allows starting containers in which the filesystem is provided externally from the containerd machinery.
Without the need to download all the layers for each image. And getting rid of the pulling time.

We exploit this new capability to mount OCI layers directly from a filesystem on the local machine.
We focus on layers provided by CernVM-FS, but with minor changes is possible to mount layers from any filesystem, like NFS.

Overall this new mechanism should bring down the time to start-up a new container image.

If the layers are not in the local filesystem, `containerd` simply follow the standard path and download the tarball.

## Configuration

This remote snapshotter communicates with `containerd` via gRPC over linux socket.
The default socket is `/run/containerd-cvmfs-grpc/containerd-cvmfs-grpc.sock`.
The socket is created automatically by the snapshotter if it does not exists.

It is necessary to configure containerd to use this new remote snapshotter.
A basic configuration would look like:

```
# tell containerd to use this particular snapshotter
[plugins."io.containerd.grpc.v1.cri".containerd]
  snapshotter = "cvmfs-snapshotter"
  disable_snapshot_annotations = false

# tell containerd how to communicate with this snapshotter
[proxy_plugins]
  [proxy_plugins.cvmfs-snapshotter]
    type = "snapshot"
    address = "/run/containerd-cvmfs-grpc/containerd-cvmfs-grpc.sock"
```

## Work in progress

This snapshotter is still a work in progress.

Feel free to fill issues and pull requests.

## Testing

This plugin is tested using `kind`.

```
$ docker build -t cvmfs-kind-node https://github.com/cvmfs/containerd-remote-snapshotter.git
$ cat kind-mount-cvmfs.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraMounts:
    - hostPath: /cvmfs/unpacked.cern.ch
      containerPath: /cvmfs/unpacked.cern.ch

$ kind create cluster --config kind-mount-cvmfs.yaml --image cvmfs-kind-node
```

At this point, it is possible to use `kubectl` to start containers.
If the filesystem of the container is available on the local filesystem used by the plugin, it won't download the tarball, but just mount the local filesystem.

### Many thanks

Thanks to @ktock and the containerd community for the work on a similar plugin and API.

[https://github.com/containerd/stargz-snapshotter/](https://github.com/containerd/stargz-snapshotter/)
