CernVM-FS Repository Gateway
============================

The CernVM-FS repository gateway exposes a service API which is consumed by the CernVM-FS publisher tools, and mediates all interactions between publisher machines and the repository.

Building
--------

Go version 1.11.5 or newer is required to build `cvmfs-gateway`. The packages
uses standard Go modules for dependency management. To build the package, run:
```bash
$ go build
```

Vendored copies of all dependencies are provided in this repository, for use in CI environments where it is not desirable to depend on external package sources. To build using the vendored dependencies, run:
```bash
$ go build -mod=vendor
```

Running the testsuite
---------------------

```bash
$ go test -v ./...
```

License and copyright
---------------------

See LICENSE in the project root.

