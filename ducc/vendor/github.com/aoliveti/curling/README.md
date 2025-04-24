# curling

![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/aoliveti/curling)
![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/aoliveti/curling/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/aoliveti/curling)](https://pkg.go.dev/github.com/aoliveti/curling)
[![codecov](https://codecov.io/gh/aoliveti/curling/graph/badge.svg?token=3L9FOZMEJH)](https://codecov.io/gh/aoliveti/curling)
[![Go Report Card](https://goreportcard.com/badge/github.com/aoliveti/curling)](https://goreportcard.com/report/github.com/aoliveti/curling)
![GitHub License](https://img.shields.io/github/license/aoliveti/curling)

Curling is a Go library that converts [http.Request](https://pkg.go.dev/net/http#Request) objects
into [cURL](https://curl.se/) commands

## Install

```sh
go get -u github.com/aoliveti/curling
```

## Usage

The following Go code demonstrates how to create a command from an HTTP request object:

```go
package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/aoliveti/curling"
)

func main() {
	req, err := http.NewRequest(http.MethodGet, "https://www.google.com", nil)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Add("If-None-Match", "foo")

	cmd, err := curling.NewFromRequest(req)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(cmd)
}
```

```sh
curl -X 'GET' 'https://www.google.com' -H 'If-None-Match: foo'
```

### Options

```go
func NewFromRequest(r *http.Request, opts ...Option) (*Command, error) {
    ...
}
```

When creating a new command, you can provide these options:

| Option                          | Description                                       |
|---------------------------------|---------------------------------------------------|
| WithLongForm()                  | Enables the long form for cURL options            |
| WithFollowRedirects()           | Sets the flag -L, --location                      |
| WithInsecure()                  | Sets the flag -k, --insecure                      |
| WithSilent()                    | Sets the flag -s, --silent                        |
| WithCompressed()                | Sets the flag --compressed                        |
| WithMultiLine()                 | Generates a multiline snippet for unix-like shell |
| WithWindowsMultiLine()          | Generates a multiline snippet for Windows shell   |
| WithPowerShellMultiLine()       | Generates a multiline snippet for PowerShell      |
| WithDoubleQuotes()              | Uses double quotes to escape characters           |
| WithRequestTimeout(seconds int) | Sets the flag -m, --max-time                      |

## License

The library is released under the MIT license. See [LICENSE](LICENSE) file.