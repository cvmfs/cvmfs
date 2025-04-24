package cmd

import (
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"

	lib "github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
	"github.com/cvmfs/ducc/temp"
	"github.com/sirupsen/logrus"
)

var v string

func init() {
	rootCmd.PersistentFlags().StringVarP(&temp.TemporaryBaseDir, "temporary-dir", "t", "", "Temporary directory to store files necessary during the conversion of images, it can grow large ~1G. If not set we use the standard of the system $TMP, usually /tmp")
	if temp.TemporaryBaseDir == "" {
		temp.TemporaryBaseDir = os.Getenv("DUCC_TMP_DIR")
	}
	rootCmd.PersistentFlags().StringVarP(&lib.NotificationFile, "notification-file", "n", "", "File where to publish notification about DUCC progression")
	rootCmd.PersistentFlags().StringVarP(&v, "verbosity", "v", logrus.InfoLevel.String(), "Log level (trace, debug, info, warn, error, fatal, panic")
}

var rootCmd = &cobra.Command{
	Use:   "cvmfs_ducc",
	Short: "Show the several commands available.",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		lib.SetupNotification()
		lib.SetupRegistries()
		setUpLogs(os.Stdout, v)
	},
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		lib.StopNotification()
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

func setUpLogs(out io.Writer, level string) error {
	logrus.SetOutput(out)
	lvl, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}
	logrus.SetLevel(lvl)
	return nil
}
