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

//go:embed cvmfs.cue
var schemaSource string

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
	schema := ctx.CompileString(schemaSource, cue.Filename("cvmfs.cue"))
	if err := schema.Err(); err != nil {
		fail("error compiling embedded schema:\n%s", errors.Details(err, nil))
	}
	// Note: Err() is not checked here; a bare definition legitimately
	// reports "incomplete" errors until the configuration is unified in.
	defValue := schema.LookupPath(cue.ParsePath(*definition))
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
