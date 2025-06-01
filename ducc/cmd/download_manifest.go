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
	arch     string
	variant  string
)

func init() {
	downloadManifestCmd.Flags().StringVarP(&username, "username", "u", "", "username to use to log in into the registry.")
	downloadManifestCmd.Flags().StringVarP(&arch, "arch", "a", "amd64", "architecture to download, in case of multi-arch iamges")
	downloadManifestCmd.Flags().StringVarP(&variant, "variant", "", "", "architecture variant to download. leave empty to get all")
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

		manifestList, err := img.GetManifestList()
		if err != nil {
			l.LogE(err).Fatal("Error in getting the manifest")
			return err
		}
		if arch == "all" {
			text, err := json.MarshalIndent(manifestList, "", "  ")
			if err != nil {
				l.LogE(err).Fatal("Error in encoding the manifest as JSON")
				return err
			}
			fmt.Println(string(text))
			return nil
		}
		found := false
		for _, manifest := range manifestList.Manifests {
			var targetVariant string
			if manifest.Platform.Architecture == arch {
				if manifest.Platform.Variant != nil {
					targetVariant = *manifest.Platform.Variant
				} else {
					targetVariant = ""
				}
				if variant == "" || variant == targetVariant {
					text, err := json.MarshalIndent(manifest.Manifest, "", "  ")
					if err != nil {
						l.LogE(err).Fatal("Error in encoding the manifest as JSON")
						return err
					}
					found = true
					fmt.Println(string(text))
				}
			}
		}
		if found {
			return nil
		} else {
			return fmt.Errorf("Could not find arch %s in manifestlist. Use --arch all to see the manifestlist with available architectures", arch)
		}
	},
}
