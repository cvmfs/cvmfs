package cmd

import (
	_ "net/http/pprof"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Config *gw.Config

var rootCmd = &cobra.Command{
	Use:   "cvmfs-gateway",
	Short: "The CVMFS Gateway allows using multiple publishers for a single CVMFS repository",
	Long:  "The CVMFS gateway allows using multiple publishers for a single CVMFS repository",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		gw.InitLogging(os.Stderr)
		cfg, err := gw.ReadConfig()
		if err != nil {
			gw.Log("init", gw.LogError).
				Msg("reading configuration failed")
			os.Exit(1)
		}
		Config = cfg
		gw.ConfigLogging(Config)
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&gw.ConfigFile, "user_config_file", "/etc/cvmfs/gateway/user.json", "config file with user modifiable settings")
	viper.BindPFlag("user_config_file", rootCmd.PersistentFlags().Lookup("user_config_file"))

	rootCmd.PersistentFlags().String("access_config_file", "/etc/cvmfs/gateway/repo.json", "repository access configuration file")
	viper.BindPFlag("access_config_file", rootCmd.PersistentFlags().Lookup("access_config_file"))

	rootCmd.PersistentFlags().Int("port", 4929, "HTTP frontend port")
	viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))

	rootCmd.PersistentFlags().Int("max_lease_time", 7200, "maximum lease time in seconds")
	viper.BindPFlag("max_lease_time", rootCmd.PersistentFlags().Lookup("max_lease_time"))

	rootCmd.PersistentFlags().String("log_level", "info", "log level (debug|info|warn|error|fatal|panic)")
	viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log_level"))

	rootCmd.PersistentFlags().Bool("log_timestamps", false, "enable timestamps in logging output")
	viper.BindPFlag("log_timestamps", rootCmd.PersistentFlags().Lookup("log_timestamps"))

	rootCmd.PersistentFlags().Int("num_receivers", 1, "number of parallel cvmfs_receiver processes to run")
	viper.BindPFlag("num_receivers", rootCmd.PersistentFlags().Lookup("num_receivers"))

	rootCmd.PersistentFlags().String("receiver_path", "/usr/bin/cvmfs_receiver", "the path of the cvmfs_receiver executable")
	viper.BindPFlag("receiver_path", rootCmd.PersistentFlags().Lookup("receiver_path"))

	rootCmd.PersistentFlags().String("work_dir", "/var/lib/cvmfs-gateway", "the working directory for database files")
	viper.BindPFlag("work_dir", rootCmd.PersistentFlags().Lookup("work_dir"))

	rootCmd.PersistentFlags().Bool("mock_receiver", false, "enable the mocked implementation of the receiver process (for testing)")
	viper.BindPFlag("mock_receiver", rootCmd.PersistentFlags().Lookup("mock_receiver"))
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		gw.Log("cmd-root", gw.LogError).
			Err(err).
			Msg("could not start gateway")
		os.Exit(1)
	}
}
