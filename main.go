package main

import (
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

func main() {
	gw.InitLogging(os.Stderr)
	cfg, err := gw.ReadConfig()
	if err != nil {
		gw.Log.Error().Err(err).Msg("reading configuration failed")
	}
	gw.ConfigLogging(cfg)

	gw.Log.Info().Msgf("Configuration read: %+v", cfg)

	gw.Log.Info().Msg("starting repository gateway")
}
