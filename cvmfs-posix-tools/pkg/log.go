package pkg

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/mattn/go-isatty"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type leveledWriter struct {
	io.Writer
	ErrOut io.Writer
}

func (lw leveledWriter) WriteLevel(lvl zerolog.Level, txt []byte) (int, error) {
	if lvl > zerolog.InfoLevel {
		return lw.ErrOut.Write(txt)
	}
	return lw.Writer.Write(txt)
}

func LeveledPipeLogger(in *bufio.Scanner, level zerolog.Level) {
	for in.Scan() {
		log.WithLevel(level).Msg(in.Text())
	}
	if err := in.Err(); err != nil {
		log.Error().Err(err).Msg("Error in piped logging")
	}

}

// Takes in a map of regex patterns to extract, returning a map from name to the match strings
func LeveledPipeLoggerWithExtraction(in *bufio.Scanner, level zerolog.Level, extractionFields map[string]*regexp.Regexp) map[string][]string {
	extractedFields := map[string][]string{}
	for in.Scan() {
		log.WithLevel(level).Msg(in.Text())
		for name, r := range extractionFields {
			match := r.FindStringSubmatch(in.Text())
			if match != nil {
				extractedFields[name] = match
			}
		}
	}
	if err := in.Err(); err != nil {
		log.Error().Err(err).Msg("Error in piped logging")
	}
	return extractedFields
}

func prettyLogger(out io.Writer) zerolog.ConsoleWriter {
	output := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339, NoColor: !isatty.IsTerminal(os.Stdout.Fd())}
	output.FormatLevel = func(i interface{}) string {
		return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
	}
	output.FormatMessage = func(i interface{}) string {
		return fmt.Sprintf("%s", i)
	}
	output.FormatFieldName = func(i interface{}) string {
		return fmt.Sprintf(" | %s:", i)
	}
	output.FormatFieldValue = func(i interface{}) string {
		return fmt.Sprintf(" %s", i)
	}
	return output
}

func SetupLogger() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	outStd := prettyLogger(os.Stdout)
	outErr := prettyLogger(os.Stderr)
	multi := leveledWriter{outStd, outErr}
	log.Logger = zerolog.New(multi).With().Timestamp().Logger()
}

func SetupDebugLogger() {
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}
