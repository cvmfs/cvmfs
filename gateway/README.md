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

HTTP timeout
------------

Set `http_timeout` in the gateway configuration or pass `--http_timeout` to set the HTTP read and write timeout in seconds. Omitting it or setting it to zero uses `max_lease_time`, preserving the default behavior. The timeout does not change when leases expire.

License and copyright
---------------------

See LICENSE in the project root.

