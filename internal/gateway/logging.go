package gateway

import (
	"io"

	"github.com/rs/zerolog"
)

// LogLevel encodes the different log levels
type LogLevel zerolog.Level

// The various log levels
const (
	DebugLevel LogLevel = LogLevel(zerolog.DebugLevel)
	InfoLevel  LogLevel = LogLevel(zerolog.InfoLevel)
	ErrorLevel LogLevel = LogLevel(zerolog.ErrorLevel)
)

// Log is the application-wide logger
var Log zerolog.Logger

// InitLogging initializes the logger
func InitLogging(sink io.Writer) {
	Log = zerolog.New(sink)
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

// ConfigLogging updates the logging settings with
// values from a Config object (Meant to be called after ReadConfig)
func ConfigLogging(cfg *Config) {
	if cfg.LogTimestamps {
		Log = Log.With().Timestamp().Logger()
	}
	lev := zerolog.InfoLevel
	if l, err := zerolog.ParseLevel(cfg.LogLevel); err == nil {
		lev = l
	}
	zerolog.SetGlobalLevel(lev)
}
