# Building CVMFS CSI from source

There are pre-built container images available from [cvmfsplugin repo at CERN registry](https://registry.cern.ch/harbor/projects/26/repositories/cvmfsplugin) (`docker pull registry.cern.ch/magnum/cvmfsplugin`). If however you need to build CVMFS CSI from source, you can follow this guide.

CVMFS CSI is written in Go. Make sure you have Go compiler and other related build tools installed before continuing.

Clone [github.com/cvmfs/cvmfs-csi](https://github.com/cvmfs/cvmfs-csi) repository:
```bash
git clone https://github.com/cvmfs/cvmfs-csi.git
cd cvmfs-csi
```

There are different build targets available in the provided Makefile. To build only the CVMFS CSI executable, run following command:
```bash
make
```

After building successfully, the resulting executable file can be found in `bin/csi-cvmfsplugin`.

You can also build container images. By default, Docker is used for building. To build a container image using e.g. Podman, run following command:
```bash
TARGETS=linux/amd64 IMAGE_BUILD_TOOL=podman make image
```
