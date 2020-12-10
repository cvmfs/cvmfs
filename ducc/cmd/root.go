package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	l "github.com/cvmfs/ducc/log"
	"github.com/cvmfs/ducc/temp"
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&temp.TemporaryBaseDir, "temporary-dir", "t", "", "Temporary directory to store files necessary during the conversion of images, it can grow large ~1G. If not set we use the standard of the system $TMP, usually /tmp")
	if temp.TemporaryBaseDir == "" {
		temp.TemporaryBaseDir = os.Getenv("DUCC_TMP_DIR")
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
			l.Log().Info("Process alive")
		}
	}()
}
