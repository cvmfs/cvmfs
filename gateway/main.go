package main

import (
	"errors"
	"fmt"
	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	fe "github.com/cvmfs/gateway/internal/gateway/frontend"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"syscall"
)

var Version = "development"

func main() {
	fmt.Println("CernVM-FS Gateway Service Version:\t", Version)
	gw.InitLogging(os.Stderr)
	cfg, err := gw.ReadConfig()
	if err != nil {
		gw.Log("main", gw.LogError).
			Msg("reading configuration failed")
		os.Exit(1)
	}
	gw.ConfigLogging(cfg)

	gw.Log("main", gw.LogInfo).
		Msgf("configuration read: %+v", cfg)

	gw.Log("main", gw.LogInfo).
		Msg("starting repository gateway")

	services, err := be.StartBackend(*cfg)
	if err != nil {
		gw.Log("main", gw.LogError).
			Err(err).
			Msg("could not start backend services")
		os.Exit(1)
	}
	defer services.Stop()

	// Start the default HTTP server for pprof requests (restricted to localhost)
	go func() {
		var (
			addr string
			ln   net.Listener
		)

		for port := cfg.PProfPort; port <= cfg.PProfPortRangeMax; port++ {
			var err error
			addr = fmt.Sprintf("localhost:%d", port)
			ln, err = net.Listen("tcp", addr)
			if err != nil {
				if isErrorAddressAlreadyInUse(err) {
					if port < cfg.PProfPortRangeMax {
						continue
					} // else: we are out of ports to try and report error.
				}
				gw.Log("main", gw.LogError).
					Err(err).
					Msg("pprof HTTP server failed")
				os.Exit(1)
			}
			break
		}

		gw.Log("main", gw.LogInfo).
			Msgf("pprof listening on %s", addr)

		if err := http.Serve(ln, nil); err != nil {
			gw.Log("main", gw.LogError).
				Err(err).
				Msg("pprof HTTP server failed")
			os.Exit(1)
		}
	}()

	go func() {
		timeout := services.Config.MaxLeaseTime
		if err := fe.Start(services, cfg.Port, timeout); err != nil {
			gw.Log("main", gw.LogError).
				Err(err).
				Msg("starting the HTTP front-end failed")
			os.Exit(1)
		}
	}()

	done := gw.SetupCloseHandler([]func(){})

	gw.Log("main", gw.LogInfo).Msg("waiting for interrupt")
	<-done
}

// From: https://stackoverflow.com/a/65865898
func isErrorAddressAlreadyInUse(err error) bool {
	var eOsSyscall *os.SyscallError
	if !errors.As(err, &eOsSyscall) {
		return false
	}
	var errErrno syscall.Errno // doesn't need a "*" (ptr) because it's already a ptr (uintptr)
	if !errors.As(eOsSyscall, &errErrno) {
		return false
	}
	if errErrno == syscall.EADDRINUSE {
		return true
	}
	return false
}
