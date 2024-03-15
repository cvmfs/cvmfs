package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/daemon"
	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
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
	RunE: func(cmd *cobra.Command, args []string) error {
		parsedInput, err := daemon.ParseImageURL(args[0])
		if err != nil {
			return fmt.Errorf("error parsing the image URL: %w", err)
		}

		image := db.Image{
			ID:             db.ImageID(uuid.New()),
			RegistryScheme: parsedInput.Scheme,
			RegistryHost:   parsedInput.Registry,
			Repository:     parsedInput.Repository,
			Tag:            parsedInput.Tag,
			Digest:         parsedInput.Digest,
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := registry.InitRegistriesFromEnv(ctx); err != nil {
			return fmt.Errorf("error initializing registries: %w", err)
		}
		manifest, _, _, err := registry.FetchAndParseManifestAndList(image)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error fetching the manifest: %s\n", err)
			return err
		}
		os.Stdout.Write(manifest.ManifestBytes)
		return nil
	},
}
