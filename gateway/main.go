package main

import (
	"os"
	"runtime/pprof"

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

	closeActions := []func(){}
	if cfg.CPUProfile != "" {
		gw.Log("main", gw.LogInfo).Msg("start CPU profiling")
		if err := gw.EnableCPUProfiling(cfg.CPUProfile); err != nil {
			gw.Log("main", gw.LogError).
				Err(err).
				Msg("could not create profiling output file")
			os.Exit(1)
		}
		closeActions = append(closeActions, func() {
			gw.Log("main", gw.LogInfo).Msg("stop CPU profiling")
			pprof.StopCPUProfile()
		})
	}

	go func() {
		timeout := services.Config.MaxLeaseTime
		if err := fe.Start(services, cfg.Port, timeout); err != nil {
			gw.Log("main", gw.LogError).
				Err(err).
				Msg("starting the HTTP front-end failed")
			os.Exit(1)
		}
	}()

	done := gw.SetupCloseHandler(closeActions)

	gw.Log("main", gw.LogInfo).Msg("waiting for interrupt")
	<-done
}
