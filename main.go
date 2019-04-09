package main

import (
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"
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

	ac := gw.NewAccessConfig()
	if err := ac.Load(cfg.AccessConfigFile); err != nil {
		gw.Log.Error().
			Err(err).
			Msg("loading repository access configuration failed")
		os.Exit(1)
	}

	gw.Log.Info().Msg("starting repository gateway")

	if err := fe.Start(cfg.Port, cfg.MaxLeaseTime); err != nil {
		gw.Log.Error().
			Err(err).
			Msg("starting the HTTP front-end failed")
	}
}
