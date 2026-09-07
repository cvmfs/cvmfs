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
	"cuelang.org/go/cue/build"
	"cuelang.org/go/cue/cuecontext"
	"cuelang.org/go/cue/errors"
	"cuelang.org/go/cue/parser"
	"cuelang.org/go/cue/token"
	"cuelang.org/go/encoding/json"
)

// Embed the schema files into the binary
//
//go:embed cvmfs_client.cue
var clientSchemaSource string

//go:embed cvmfs_server.cue
var serverSchemaSource string

//go:embed gen_client.cue
var genClientSchemaSource string

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

// fieldKinds returns the type the schema expects at each parameter name.
// Collected in one pass, because resolving them one at a time is far too slow.
func fieldKinds(ctx *cue.Context, schema cue.Value) func(name string) cue.Kind {
	declared := make(map[string]cue.Kind)
	if fields, err := schema.Fields(cue.Optional(true)); err == nil {
		for fields.Next() {
			declared[fields.Selector().Unquoted()] = fields.Value().IncompleteKind()
		}
	}

	return func(name string) cue.Kind {
		if kind, ok := declared[name]; ok {
			return kind
		}

		// A pattern may still match, e.g. CVMFS_CACHE_<name>_SIZE.
		key := ctx.CompileString(fmt.Sprintf("{%q: _}", name))
		resolved := schema.Unify(key).LookupPath(cue.MakePath(cue.Str(name)))
		if !resolved.Exists() {
			return cue.BottomKind
		}
		return resolved.IncompleteKind()
	}
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
func convertConfig(kindOf func(string) cue.Kind, schema cue.Value, expr ast.Expr) {
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
				convertLit(lit, kindOf(name))
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

// explain prints a parameter's name, whether it is required, and its
// documentation.
func explain(name, wanted string) int {
	ctx := cuecontext.New()
	schema := ctx.CompileString(clientSchemaSource, cue.Filename("cvmfs_client.cue")).
		Unify(ctx.CompileString(serverSchemaSource, cue.Filename("cvmfs_server.cue")))

	// The scopes name the definitions the way a user would, for message
	// wording.
	other, scope, otherScope := "#ServerConfig", "client", "server"
	if wanted == other {
		other, scope, otherScope = "#ClientConfig", "server", "client"
	}

	// Search the requested definition first, so that a parameter declared by
	// both, such as CVMFS_USER, resolves to the one that was asked for.
	for _, definition := range []string{wanted, other} {
		fields, err := schema.LookupPath(cue.ParsePath(definition)).
			Fields(cue.Optional(true))
		if err != nil {
			continue
		}
		for fields.Next() {
			if fields.Selector().Unquoted() != name {
				continue
			}
			if definition != wanted {
				fmt.Fprintf(os.Stderr, "%s is a %s parameter, not a %s one\n",
					name, otherScope, scope)
				return 1
			}

			// A field with a default can be left out too, so it reads optional.
			status := "required"
			if fields.IsOptional() {
				status = "optional"
			}
			if deflt, ok := fields.Value().Default(); ok {
				status = fmt.Sprintf("optional, default %v", deflt)
			}

			description := "No description available."
			attribute := fields.Value().Attribute("description")
			if text, err := attribute.String(0); err == nil {
				description = text
			}

			fmt.Printf("NAME\n       %s - %s configuration parameter, %s\n\n"+
				"DESCRIPTION\n       %s\n",
				name, scope, status, description)
			return 0
		}
	}
	fmt.Fprintf(os.Stderr, "no such configuration parameter: %s\n", name)
	return 1
}

// loadSchema builds the embedded schemas as one CUE package, so that
// gen_client.cue can use the client definitions.
func loadSchema(ctx *cue.Context) (cue.Value, error) {
	instance := build.NewContext().NewInstance("cvmfs", nil)
	for filename, source := range map[string]string{
		"cvmfs_client.cue": clientSchemaSource,
		"cvmfs_server.cue": serverSchemaSource,
		"gen_client.cue":   genClientSchemaSource,
	} {
		file, err := parser.ParseFile(filename, source)
		if err != nil {
			return cue.Value{}, err
		}
		if err := instance.AddSyntax(file); err != nil {
			return cue.Value{}, err
		}
	}
	value := ctx.BuildInstance(instance)
	return value, value.Err()
}

// generate writes a client configuration for the node facts given as JSON.
func generate(input []byte) int {
	ctx := cuecontext.New()

	schema, err := loadSchema(ctx)
	// catches compilation errors in the CUE files
	if err != nil {
		fmt.Fprintf(os.Stderr, "schema error: %v\n", err)
		return 2
	}

	// take the JSON input byte array and parse into a ast.Expr
	expr, err := json.Extract("facts", input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "json error: %v\n", err)
		return 2
	}

	// turn the ast into a cue object and place into the "facts" namespace
	schema = schema.FillPath(cue.ParsePath("facts"), ctx.BuildExpr(expr))

	tuned := schema.LookupPath(cue.ParsePath("tuned"))
	// verify that every calculation in tuned resolved to a concrete literal / value
	if err := tuned.Validate(cue.Concrete(true)); err != nil {
		fmt.Fprint(os.Stderr, errors.Details(err, nil))
		return 1
	}
	// check whether the unification resolved to a conflict
	if err := schema.LookupPath(cue.ParsePath("tunedConfig")).Validate(); err != nil {
		fmt.Fprint(os.Stderr, errors.Details(err, nil))
		return 1
	}

	// creates an iterator for tuned's fields
	fields, _ := tuned.Fields()
	client := schema.LookupPath(cue.ParsePath("#ClientConfig"))

	for fields.Next() {
		name, val := fields.Selector().Unquoted(), fields.Value()

		// extract and print description above the parameter if it exists
		attr := client.LookupPath(cue.MakePath(cue.Str(name).Optional())).
			Attribute("description")
		if desc, _ := attr.String(0); desc != "" {
			fmt.Printf("\n# %s\n", desc)
		}

		// turn value into a string
		str, err := val.String()
		if err != nil {
			// convert CUE ints and bools to strings
			str = fmt.Sprint(val)
		}
		// output the formatted bash line (KEY=VALUE)
		fmt.Printf("%s=%s\n", name, str)
	}

	return 0
}

func main() {
	definition := flag.String("d", "#ClientConfig",
		"schema definition to validate against")
	explainParam := flag.String("explain", "",
		"print the documentation of a configuration parameter and exit")
	generateConfig := flag.Bool("generate", false,
		"write a client configuration for the node facts read from stdin")
	flag.Parse()

	if *explainParam != "" {
		os.Exit(explain(*explainParam, *definition))
	}

	if *generateConfig {
		facts, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error reading node facts: %v\n", err)
			os.Exit(2)
		}
		os.Exit(generate(facts))
	}

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
	convertConfig(fieldKinds(ctx, schema), schema, expr)

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
