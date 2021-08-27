package gateway

import (
	"os"
	"os/signal"
	"syscall"
)

// ContextKey is a type alias for additional Context keys
type ContextKey int

// List of different context keys in use
const (
	IDKey ContextKey = iota
	T0Key
)

// SetupCloseHandler to run the specified actions on Ctrl-C
func SetupCloseHandler(actions []func()) chan struct{} {
	signal.Reset()
	c := make(chan os.Signal, 2)
	done := make(chan struct{}, 1)
	go func() {
		<-c
		Log("close_handler", LogInfo).Msg("interrupt received")
		for _, action := range actions {
			action()
		}
		close(done)
	}()
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	return done
}
