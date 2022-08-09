package cmd

import (
	"fmt"
	_ "net/http/pprof"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"

	"github.com/spf13/cobra"
)

var Version = "development"

var Config *gw.Config

var rootCmd = &cobra.Command{
	Use:   "cvmfs-gateway",
	Short: "The CVMFS Gateway allows using multiple publishers for a single CVMFS repository",
	Long:  "The CVMFS gateway allows using multiple publishers for a single CVMFS repository",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("CernVM-FS Gateway Server Version:\t", Version)
	},
}

func init() {
	cobra.OnInitialize(initConfig)
}

func initConfig() {
	gw.InitLogging(os.Stderr)
	cfg, err := gw.ReadConfig()
	if err != nil {
		gw.Log("init", gw.LogError).
			Msg("reading configuration failed")
		os.Exit(1)
	}
	Config = cfg
	gw.ConfigLogging(Config)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		gw.Log("cmd-root", gw.LogError).
			Err(err).
			Msg("could not start gateway")
		os.Exit(1)
	}
}
