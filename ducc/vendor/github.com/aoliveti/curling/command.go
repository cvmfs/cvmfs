package curling

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

// A Command represents a cURL command based on an HTTP request.
//
// Returned by [NewFromRequest], a Command expects a pointer to [http.Request] as input.
// It generates a cURL command string, which by default uses a single line command,
// short form options, and single quote escaping.
type Command struct {
	// tokens is a set of lines that form the command.
	tokens []string

	// lineContinuation is the character used to break a single statement into multiple lines.
	lineContinuation string

	// location enables the option -L, --location.
	location bool

	// compressed enables the option --compressed.
	compressed bool

	// insecure enables the option -k, --insecure.
	insecure bool

	// useLongForm enables the long form for cURL options (example: --header instead of -H).
	useLongForm bool

	// useMultiLine splits the command across multiple lines.
	useMultiLine bool

	// silent enables the option -s, --silent.
	silent bool

	// useDoubleQuotes enables escaping using double quotes.
	useDoubleQuotes bool

	// requestTimeout enables the option -m, --max-time.
	requestTimeout int
}

// NewFromRequest returns a new [Command] that reads from r.
// If the request has an invalid URL, NewFromRequest returns an error.
// If NewFromRequest can't read the request body, it returns an error.
func NewFromRequest(r *http.Request, opts ...Option) (*Command, error) {
	var c Command

	if err := c.build(r, opts...); err != nil {
		return nil, err
	}

	return &c, nil
}

// String returns the cURL command.
func (c *Command) String() string {
	separator := " "
	if c.useMultiLine {
		separator = fmt.Sprintf(" %s\n", c.lineContinuation)
	}

	s := strings.Join(c.tokens, separator)
	return strings.TrimSpace(s)
}

// appendToken appends a new token into tokens.
func (c *Command) appendToken(s ...string) {
	token := strings.Join(s, " ")
	c.tokens = append(c.tokens, token)
}

// optionForm returns either the short or long form based on the useLongForm flag.
func (c *Command) optionForm(short, long string) string {
	if c.useLongForm {
		return long
	}

	return short
}

// escape takes a string as input and escapes it with single or double quotes based
// on the useDoubleQuotes option.
func (c *Command) escape(s string) string {
	if c.useDoubleQuotes {
		v := strings.ReplaceAll(s, "\"", "\\\"")
		return fmt.Sprintf("\"%s\"", v)
	}

	v := strings.ReplaceAll(s, "'", "'\\''")
	return fmt.Sprintf("'%s'", v)
}

// build produces tokens based on the supplied options and http request.
// If the request URL is nil, build returns an error.
// If build can't read the request body, it returns an error.
func (c *Command) build(r *http.Request, opts ...Option) error {
	for _, opt := range opts {
		opt(c)
	}

	if r.URL == nil {
		return fmt.Errorf("request url is nil")
	}

	c.buildCommand(r)
	c.buildHeaders(r)

	if err := c.buildData(r); err != nil {
		return err
	}

	return nil
}

// buildCommand produces the token representing the curl command and its related options.
func (c *Command) buildCommand(r *http.Request) {
	s := []string{"curl"}

	if c.silent {
		s = append(s, c.optionForm("-s", "--silent"))
	}

	if c.requestTimeout > 0 {
		s = append(s, c.optionForm("-m", "--max-time"), strconv.Itoa(c.requestTimeout))
	}

	if c.insecure {
		s = append(s, c.optionForm("-k", "--insecure"))
	}

	if c.compressed {
		s = append(s, "--compressed")
	}

	if c.location {
		s = append(s, c.optionForm("-L", "--location"))
	}

	var command string
	if len(s) > 0 {
		command = strings.Join(s, " ")
	}

	method := r.Method
	if method == "" {
		method = http.MethodGet
	}

	c.appendToken(
		command,
		c.optionForm("-X", "--request"),
		c.escape(method),
		c.escape(r.URL.String()),
	)
}

// buildHeaders produces one token for each request header.
func (c *Command) buildHeaders(r *http.Request) {
	if len(r.Header) == 0 {
		return
	}

	var headers []string
	for key, values := range r.Header {
		canonicalKey := http.CanonicalHeaderKey(key)
		headers = append(headers, fmt.Sprintf("%s: %s", canonicalKey, strings.Join(values, ", ")))
	}

	slices.Sort(headers)

	for _, header := range headers {
		c.appendToken(
			c.optionForm("-H", "--header"),
			c.escape(header),
		)
	}
}

// buildData produces the token representing the request body and its related option (-d or --data).
// If the request body is nil or [http.NoBody], no token is produced.
// If buildData can't read the request body, it returns an error.
func (c *Command) buildData(r *http.Request) error {
	if r.Body == nil || r.Body == http.NoBody {
		return nil
	}

	var b bytes.Buffer
	if _, err := b.ReadFrom(r.Body); err != nil {
		return fmt.Errorf("reading bytes from request body: %w", err)
	}

	// Reset request body for potential re-reads
	r.Body = io.NopCloser(bytes.NewBuffer(b.Bytes()))

	option := c.optionForm("-d", "--data")
	c.appendToken(option, c.escape(b.String()))

	return nil
}
