package main

import (
	_ "embed"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"regexp"
	"strconv"

	"cuelang.org/go/cue"
	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/errors"
	"cuelang.org/go/cue/token"
	"cuelang.org/go/encoding/json"
)

// Embed the two schema files into the binary
//
//go:embed cvmfs_client.cue
var clientSchemaSource string

//go:embed cvmfs_server.cue
var serverSchemaSource string

var schemas = []struct {
	filename string
	source   string
}{
	{"cvmfs_client.cue", clientSchemaSource},
	{"cvmfs_server.cue", serverSchemaSource},
}

// Ιntegers and booleans regular expressions
var (
	integerRe = regexp.MustCompile(`^-?[0-9]+$`)
	trueRe    = regexp.MustCompile(`^(?i:yes|on|1|true)$`)
	falseRe   = regexp.MustCompile(`^(?i:no|off|0|false)$`)
)

// fieldKind returns the type the schema expects for a parameter name.
func fieldKind(ctx *cue.Context, schema cue.Value, name string) cue.Kind {
	key := ctx.CompileString(fmt.Sprintf("{%q: _}", name))
	resolved := schema.Unify(key).LookupPath(cue.MakePath(cue.Str(name)))
	if !resolved.Exists() {
		return cue.BottomKind
	}
	return resolved.IncompleteKind()
}

// convertLit turns a string value into an int or bool if the schema wants
// one there, e.g. "42" -> 42 or "yes" -> true. Otherwise it's left alone.
func convertLit(lit *ast.BasicLit, kind cue.Kind) {
	if lit.Kind != token.STRING {
		return
	}
	value, err := strconv.Unquote(lit.Value)
	if err != nil {
		return
	}

	wantInt := kind&cue.IntKind != 0
	wantBool := kind&cue.BoolKind != 0

	if wantInt && integerRe.MatchString(value) {
		if n, ok := new(big.Int).SetString(value, 10); ok {
			lit.Kind, lit.Value = token.INT, n.String()
		}
	} else if wantBool && trueRe.MatchString(value) {
		lit.Kind, lit.Value = token.TRUE, "true"
	} else if wantBool && falseRe.MatchString(value) {
		lit.Kind, lit.Value = token.FALSE, "false"
	}
}

// convertConfig fixes up every value in the parsed configuration to match
// the schema, and drops empty ones.
func convertConfig(ctx *cue.Context, schema cue.Value, expr ast.Expr) {
	switch expr := expr.(type) {
	case *ast.StructLit:
		var kept []ast.Decl
		for _, decl := range expr.Elts {
			field, ok := decl.(*ast.Field)
			if !ok {
				kept = append(kept, decl)
				continue
			}

			lit, ok := field.Value.(*ast.BasicLit)
			if !ok {
				kept = append(kept, decl)
				continue
			}
			if lit.Kind == token.STRING {
				if value, err := strconv.Unquote(lit.Value); err == nil && value == "" {
					continue // drop empty values
				}
			}

			if name, _, err := ast.LabelName(field.Label); err == nil {
				convertLit(lit, fieldKind(ctx, schema, name))
			}
			kept = append(kept, decl)
		}
		expr.Elts = kept
	case *ast.BasicLit:
		// A bare scalar, e.g. when the test suite checks a single value
		// directly instead of a whole config struct.
		convertLit(expr, schema.IncompleteKind())
	}
}

func main() {
	definition := flag.String("d", "#ClientConfig",
		"schema definition to validate against")
	flag.Parse()

	ctx := cuecontext.New()

	// Find the requested schema definition, e.g. "#ClientConfig".
	var schema cue.Value
	for _, s := range schemas {
		compiled := ctx.CompileString(s.source, cue.Filename(s.filename))
		if err := compiled.Err(); err != nil {
			fmt.Fprintf(os.Stderr, "error compiling embedded schema %s:\n%s\n",
				s.filename, errors.Details(err, nil))
			os.Exit(2)
		}
		// Skips Err() here: a bare definition is expected to look
		// "incomplete" until the config is unified in below.
		if found := compiled.LookupPath(cue.ParsePath(*definition)); found.Exists() {
			schema = found
			break
		}
	}
	if !schema.Exists() {
		fmt.Fprintf(os.Stderr, "unknown schema definition %s\n", *definition)
		os.Exit(2)
	}

	// Read the configuration JSON from stdin and convert its values to
	// match the schema.
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading configuration from stdin: %v\n", err)
		os.Exit(2)
	}
	expr, err := json.Extract("stdin", input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing configuration JSON: %v\n", err)
		os.Exit(2)
	}
	convertConfig(ctx, schema, expr)

	config := ctx.BuildExpr(expr)
	if err := config.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "error processing configuration: %v\n", err)
		os.Exit(2)
	}

	if err := schema.Unify(config).Validate(cue.Concrete(true)); err != nil {
		fmt.Fprint(os.Stderr, errors.Details(err, nil))
		os.Exit(1)
	}
}
