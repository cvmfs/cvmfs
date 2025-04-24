package curling

const (
	lineContinuationDefault    = "\\"
	lineContinuationWindows    = "^"
	lineContinuationPowerShell = "`"
)

type Option func(curling *Command)

// WithFollowRedirects enables the option -L, --location.
func WithFollowRedirects() Option {
	return func(curling *Command) {
		curling.location = true
	}
}

// WithCompression enables the option --compressed.
func WithCompression() Option {
	return func(curling *Command) {
		curling.compressed = true
	}
}

// WithInsecure enables the option -k, --insecure.
func WithInsecure() Option {
	return func(curling *Command) {
		curling.insecure = true
	}
}

// WithLongForm enables the long form for cURL options.
// Example: --header instead of -H.
func WithLongForm() Option {
	return func(curling *Command) {
		curling.useLongForm = true
	}
}

// WithSilent enables the option -s, --silent.
func WithSilent() Option {
	return func(curling *Command) {
		curling.silent = true
	}
}

// WithMultiLine splits the command across multiple lines.
// The default line continuation character is backslash.
func WithMultiLine() Option {
	return func(curling *Command) {
		curling.useMultiLine = true
		curling.lineContinuation = lineContinuationDefault
	}
}

// WithWindowsMultiLine splits the command across multiple lines.
// The line continuation character is caret.
func WithWindowsMultiLine() Option {
	return func(curling *Command) {
		curling.useMultiLine = true
		curling.lineContinuation = lineContinuationWindows
	}
}

// WithPowerShellMultiLine splits the command across multiple lines.
// The line continuation character is backtick.
func WithPowerShellMultiLine() Option {
	return func(curling *Command) {
		curling.useMultiLine = true
		curling.lineContinuation = lineContinuationPowerShell
	}
}

// WithDoubleQuotes enables escaping using double quotes.
func WithDoubleQuotes() Option {
	return func(curling *Command) {
		curling.useDoubleQuotes = true
	}
}

// WithRequestTimeout enables the option -m, --max-time.
// It sets the number of seconds the request should wait
// for a response before timing out.
// Negative value seconds will be silently ignored.
func WithRequestTimeout(seconds int) Option {
	return func(curling *Command) {
		if seconds < 0 {
			seconds = 0
		}

		curling.requestTimeout = seconds
	}
}
