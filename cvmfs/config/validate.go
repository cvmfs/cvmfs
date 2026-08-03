package main

import (
	_ "embed"
	"flag"
	"fmt"
	"io"
	"os"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/errors"
	"cuelang.org/go/encoding/json"
)

// The client and server schemas are self-contained: each carries its own
// copy of the shared type definitions, so only one of them is ever
// compiled for a given validation.
//
//go:embed cvmfs_client.cue
var clientSchemaSource string

//go:embed cvmfs_server.cue
var serverSchemaSource string

// Schemas are searched in this order for the requested definition. The
// shared types resolve in the client schema; #ServerConfig only exists in
// the server schema.
var schemas = []struct {
	filename string
	source   string
}{
	{"cvmfs_client.cue", clientSchemaSource},
	{"cvmfs_server.cue", serverSchemaSource},
}

func fail(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}

func main() {
	definition := flag.String("d", "#ClientConfig",
		"schema definition to validate against")
	flag.Parse()

	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fail("error reading configuration from stdin: %v", err)
	}

	ctx := cuecontext.New()
	var defValue cue.Value
	for _, s := range schemas {
		schema := ctx.CompileString(s.source, cue.Filename(s.filename))
		if err := schema.Err(); err != nil {
			fail("error compiling embedded schema %s:\n%s", s.filename,
				errors.Details(err, nil))
		}
		// Note: Err() is not checked here; a bare definition legitimately
		// reports "incomplete" errors until the configuration is unified in.
		if v := schema.LookupPath(cue.ParsePath(*definition)); v.Exists() {
			defValue = v
			break
		}
	}
	if !defValue.Exists() {
		fail("unknown schema definition %s", *definition)
	}

	expr, err := json.Extract("stdin", input)
	if err != nil {
		fail("error parsing configuration JSON: %v", err)
	}
	config := ctx.BuildExpr(expr)
	if err := config.Err(); err != nil {
		fail("error processing configuration: %v", err)
	}

	if err := defValue.Unify(config).Validate(cue.Concrete(true)); err != nil {
		fmt.Fprint(os.Stderr, errors.Details(err, nil))
		os.Exit(1)
	}
}
