package main

import (
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	fe "github.com/cvmfs/gateway/internal/gateway/frontend"
)

func main() {
	gw.InitLogging(os.Stderr)
	cfg, err := gw.ReadConfig()
	if err != nil {
		gw.Log("main", gw.LogError).
			Msg("reading configuration failed")
		os.Exit(1)
	}
	gw.ConfigLogging(cfg)

	gw.Log("main", gw.LogDebug).
		Msgf("configuration read: %+v", cfg)

	gw.Log("main", gw.LogInfo).
		Msg("starting repository gateway")

	services, err := be.StartBackend(cfg)
	if err != nil {
		gw.Log("main", gw.LogError).
			Err(err).
			Msg("could not start backend services")
		os.Exit(1)
	}
	defer services.Stop()

	// Write PID file before passing control to the HTTP server
	pidFile := "/var/run/cvmfs-gateway.pid"
	if err := gw.WritePIDFile(pidFile); err != nil {
		gw.Log("main", gw.LogError).
			Err(err).
			Msg("could not write PID file")
		os.Exit(1)
	}
	defer os.RemoveAll(pidFile)

	timeout := services.Config.MaxLeaseTime
	if err := fe.Start(services, cfg.Port, timeout); err != nil {
		gw.Log("main", gw.LogError).
			Err(err).
			Msg("starting the HTTP front-end failed")
		os.Exit(1)
	}
}
