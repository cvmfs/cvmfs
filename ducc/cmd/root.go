package cmd

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/config"
)

var oldTempDirString string

func init() {
	// We no longer allow user to change the full temporary directory.
	// This is because it is used for inter-process communication and locks.
	// Instead, we allow the user to set the downloads directory.
	rootCmd.PersistentFlags().StringVarP(&oldTempDirString, "temporary-dir", "t", config.DownloadsDir, "Temporary directory to store downloads needed during conversion. Can grow large.")
	oldTempDirFlag := rootCmd.PersistentFlags().Lookup("temporary-dir")
	oldTempDirFlag.Deprecated = "Use --downloads-dir instead"
	oldTempDirFlag.Hidden = true
	rootCmd.PersistentFlags().StringVar(&config.TempDir, "downloads-dir", config.DownloadsDir, "Temporary directory to store downloads needed during conversion. Can grow large.")
}

var rootCmd = &cobra.Command{
	DisableFlagsInUseLine: false,
	Use:                   os.Args[0],
	Short:                 "Daemon for Unpacking Container images into Cvmfs (DUCC)",
	SilenceUsage:          true,
	Run: func(cmd *cobra.Command, args []string) {
		if oldTempDirString != config.DownloadsDir {
			config.DownloadsDir = oldTempDirString
		}
	},
}

func EntryPoint() {
	rootCmd.Execute()
}
