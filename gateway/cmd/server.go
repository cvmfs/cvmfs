package cmd

import (
	"net/http"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	fe "github.com/cvmfs/gateway/internal/gateway/frontend"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(serverCmd)
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the cvmfs-gateway server",
	Long:  "Start the cvmfs-gateway server",
	Run: func(cmd *cobra.Command, args []string) {
		gw.Log("cmd-server", gw.LogInfo).
			Msgf("current configuration: %+v", Config)

		gw.Log("cmd-server", gw.LogInfo).
			Msg("starting CVMFS gateway server")

		services, err := be.StartBackend(*Config, true)
		if err != nil {
			gw.Log("cmd-server", gw.LogError).
				Err(err).
				Msg("could not start backend services")
			os.Exit(1)
		}
		defer services.Stop()

		// Start the default HTTP server for pprof requests (restricted to localhost)
		go func() {
			if err := http.ListenAndServe("localhost:6060", nil); err != nil {
				gw.Log("cmd-server", gw.LogError).
					Err(err).
					Msg("pprof HTTP server failed")
				os.Exit(1)
			}
		}()

		go func() {
			timeout := services.Config.MaxLeaseTime
			if err := fe.Start(services, Config.Port, timeout); err != nil {
				gw.Log("cmd-server", gw.LogError).
					Err(err).
					Msg("starting the HTTP front-end failed")
				os.Exit(1)
			}
		}()

		done := gw.SetupCloseHandler([]func(){})

		gw.Log("cmd-server", gw.LogInfo).Msg("waiting for interrupt")
		<-done
	},
}
