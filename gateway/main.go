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
		gw.Log.Error().
			Str("component", "main").
			Err(err).
			Msg("reading configuration failed")
		os.Exit(1)
	}
	gw.ConfigLogging(cfg)

	gw.Log.Debug().
		Str("component", "main").
		Msgf("configuration read: %+v", cfg)

	gw.Log.Info().
		Str("component", "main").
		Msg("starting repository gateway")

	services, err := be.Start(cfg)
	if err != nil {
		gw.Log.Error().
			Str("component", "main").
			Err(err).
			Msg("could not start backend services")
		os.Exit(1)
	}
	defer services.Close()

	timeout := services.Config.MaxLeaseTime
	if err := fe.Start(services, cfg.Port, timeout); err != nil {
		gw.Log.Error().
			Str("component", "main").
			Err(err).
			Msg("starting the HTTP front-end failed")
		os.Exit(1)
	}
}
