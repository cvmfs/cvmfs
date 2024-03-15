package cmd

import (
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/daemon"
	"github.com/cvmfs/ducc/db"
)

var (
	machineFriendly bool
)

func init() {
	checkImageSyntaxCmd.Flags().BoolVarP(&machineFriendly, "machine-friendly", "z", false, "produce machine friendly output, one line of csv")
	rootCmd.AddCommand(checkImageSyntaxCmd)
}

var checkImageSyntaxCmd = &cobra.Command{
	Use:     "check-image-syntax",
	Short:   "Check that the provide image has a valid syntax, the same checks are applied before any command in the converter.",
	Aliases: []string{"check-image", "image-check"},
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		parsedInput, err := daemon.ParseImageURL(args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing the image URL: %s\n", err)
			fmt.Println(err)
			return err
		}

		image := db.Image{
			ID:             db.ImageID(uuid.New()),
			RegistryScheme: parsedInput.Scheme,
			RegistryHost:   parsedInput.Registry,
			Repository:     parsedInput.Repository,
			Tag:            parsedInput.Tag,
			Digest:         parsedInput.Digest,
		}

		printImage(image, machineFriendly, true)
		return nil
	},
}

// TODO: Do we really want to keep the tablewriter dependency?
// Could also just print the JSON

func printImage(img db.Image, machineFriendly, csv_header bool) {
	// TODO: Find out what to do with the user and is_thin fields
	if machineFriendly {
		if csv_header {
			//fmt.Printf("name,user,scheme,registry,repository,tag,digest,is_thin\n")
			fmt.Printf("name,scheme,registry,repository,tag,digest\n")
		}
		fmt.Printf("%s,%s,%s,%s,%s,%s\n",
			img.WholeName(),
			img.RegistryScheme,
			img.RegistryHost,
			img.Repository,
			img.Tag,
			img.Digest)
	} else {
		table := tablewriter.NewWriter(os.Stdout)
		table.SetAlignment(tablewriter.ALIGN_LEFT)
		table.SetHeader([]string{"Key", "Value"})
		table.Append([]string{"Name", img.WholeName()})
		//table.Append([]string{"User", img.User})
		table.Append([]string{"Scheme", img.RegistryScheme})
		table.Append([]string{"Registry", img.RegistryHost})
		table.Append([]string{"Repository", img.Repository})
		table.Append([]string{"Tag", img.Tag})
		table.Append([]string{"Digest", img.Digest.String()})
		//var is_thin string
		//if img.IsThin {
		//	is_thin = "true"
		//} else {
		//	is_thin = "false"
		//}
		//table.Append([]string{"IsThin", is_thin})
		table.Render()
	}
}
