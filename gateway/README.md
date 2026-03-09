CernVM-FS Repository Gateway
============================

The CernVM-FS repository gateway exposes a service API which is consumed by the CernVM-FS publisher tools, and mediates all interactions between publisher machines and the repository.

Building
--------

The Gateway uses standard Go modules for dependency management. To build the package, run:
```bash
$ go build
```


Running the testsuite
---------------------

```bash
$ go test -v ./...
```

License and copyright
---------------------

See LICENSE in the project root.

