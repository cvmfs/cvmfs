package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

var (
	username string
)

func init() {
	downloadManifestCmd.Flags().StringVarP(&username, "username", "u", "", "username to use to log in into the registry.")
	rootCmd.AddCommand(downloadManifestCmd)
}

var downloadManifestCmd = &cobra.Command{
	Use:     "download-manifest",
	Short:   "Download the manifest of the image, if sucessful it will print the manifest itself, otherwise will show what went wrong.",
	Aliases: []string{"get-manifest"},
	Args:    cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		img, err := lib.ParseImage(args[0])
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if img.Tag == "" && img.Digest == "" {
			log.Fatal("Please provide either the image tag or the image digest")
		}
		if username != "" {
			img.User = username
		}

		manifest, err := img.GetManifest()
		if err != nil {
			l.LogE(err).Fatal("Error in getting the manifest")
		}
		text, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			l.LogE(err).Fatal("Error in encoding the manifest as JSON")
		}
		fmt.Println(string(text))
	},
}
