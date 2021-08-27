package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	fe "github.com/cvmfs/gateway/internal/gateway/frontend"
)

var Version = "development"

func main() {
	fmt.Println("CernVM-FS Gateway Service Version:\t", Version, "\n")
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

	// Start the default HTTP server for pprof requests (restricted to localhost)
	go func() {
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
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
