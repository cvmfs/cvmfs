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
			Err(err).
			Msg("reading configuration failed")
		os.Exit(1)
	}
	gw.ConfigLogging(cfg)

	gw.Log.Debug().Msgf("configuration read: %+v", cfg)

	gw.Log.Info().Msg("starting repository gateway")

	services, err := be.Start(cfg)
	if err != nil {
		gw.Log.Error().
			Err(err).
			Msg("could not start backend services")
		os.Exit(1)
	}

	if err := fe.Start(services, cfg.Port, cfg.MaxLeaseTime); err != nil {
		gw.Log.Error().
			Err(err).
			Msg("starting the HTTP front-end failed")
		os.Exit(1)
	}
}
