package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&lib.TemporaryBaseDir, "temporary-dir", "t", "", "Temporary directory to store files necessary during the conversion of images, it can grow large ~1G. If not set we use the standard of the system $TMP, usually /tmp")
	if lib.TemporaryBaseDir == "" {
		lib.TemporaryBaseDir = os.Getenv("DUCC_TMP_DIR")
	}
}

var rootCmd = &cobra.Command{
	Use:   "cvmfs_ducc",
	Short: "Show the several commands available.",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func EntryPoint() {
	rootCmd.Execute()
}

func AliveMessage() {
	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for range ticker.C {
			lib.Log().Info("Process alive")
		}
	}()
}
