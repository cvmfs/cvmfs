package cmd

import (
	"encoding/json"
	"fmt"

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
	Use:     "download-manifest <image>",
	Short:   "Download the manifest of the image, if successful it will print the manifest itself, otherwise will show what went wrong.",
	Aliases: []string{"get-manifest"},
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		img, err := lib.ParseImage(args[0])
		if err != nil {
			return err
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
			return err
		}
		text, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			l.LogE(err).Fatal("Error in encoding the manifest as JSON")
			return err
		}
		fmt.Println(string(text))
		return nil
	},
}
