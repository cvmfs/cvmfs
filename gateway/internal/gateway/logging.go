package gateway

import (
	"context"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// LogLevel encodes the different log levels
type LogLevel zerolog.Level

// The various log levels
const (
	LogDebug LogLevel = LogLevel(zerolog.DebugLevel)
	LogInfo  LogLevel = LogLevel(zerolog.InfoLevel)
	LogWarn  LogLevel = LogLevel(zerolog.WarnLevel)
	LogError LogLevel = LogLevel(zerolog.ErrorLevel)
)

// Logger is the application-wide logger
var Logger zerolog.Logger

// InitLogging initializes the logger
func InitLogging(sink io.Writer) {
	Logger = zerolog.New(sink)
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

// ConfigLogging updates the logging settings with values from a Config object
// (Meant to be called after ReadConfig)
func ConfigLogging(cfg *Config) {
	if cfg.LogTimestamps {
		Logger = Logger.With().Timestamp().Logger()
	}
	lev := zerolog.InfoLevel
	if l, err := zerolog.ParseLevel(cfg.LogLevel); err == nil {
		lev = l
	}
	zerolog.SetGlobalLevel(lev)
}

// LogC is a convenience wrapper on top of the global Logger of the gateway
// package. It takes a context, the component name (i.e. "http", "leasedb",
// etc.) and the log level, and returns a *zerolog.Event which is tagged with
// the component name, unique ID of the request and the time (in milliseconds as
// float) since the request was received. This event can be extended with new
// fields or logged using the Msg/Msgf methods
func LogC(ctx context.Context, component string, level LogLevel) *zerolog.Event {
	reqID, _ := ctx.Value(IDKey).(uuid.UUID)
	t0, _ := ctx.Value(T0Key).(time.Time)

	return Log(component, level).
		Str("req_id", reqID.String()).
		Dur("req_dt", time.Since(t0))
}

// Log is a convenience wrapper on top of the global Logger of the gateway
// package.
func Log(component string, level LogLevel) *zerolog.Event {
	var event *zerolog.Event
	switch level {
	case LogDebug:
		event = Logger.Debug()
	case LogInfo:
		event = Logger.Info()
	case LogError:
		event = Logger.Error()
	default:
		Logger.Error().
			Str("component", "logger").
			Msgf("unknown log level: %v", level)
		return nil
	}
	return event.Str("component", component)
}
