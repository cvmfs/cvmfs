package cmd

import (
	"fmt"
	"io"
	"net/rpc"
	"os"
	"time"

	"github.com/cvmfs/ducc/config"
	"github.com/cvmfs/ducc/daemon"
	daemonCommands "github.com/cvmfs/ducc/daemon/commands"
	daemonRpc "github.com/cvmfs/ducc/daemon/rpc"
	"github.com/cvmfs/ducc/db"
	"github.com/spf13/cobra"
)

var cmdRecipeApply_Wishlist string
var cmdRecipeApply_RemoveMissing bool
var cmdRecipeApply_UpdateExisting bool

func init() {
	rootCmd.AddCommand(cmdRecipe)

	cmdRecipe.AddCommand(cmdRecipeApply)
	cmdRecipeApply.Flags().StringVar(&cmdRecipeApply_Wishlist, "wishlist", "default", "apply the recipe to the specified wishlist")
	cmdRecipeApply.Flags().BoolVar(&cmdRecipeApply_RemoveMissing, "remove-missing", false, "remove wishes from the wishlist if they are not in the recipe")
	cmdRecipeApply.Flags().BoolVar(&cmdRecipeApply_UpdateExisting, "update-existing", true, "update settings of wishes that are already in the wishlist")
	cmdRecipeApply.Flags().BoolVar(&createLayers, "create-layers", config.DEFAULT_CREATELAYERS, "explicitly enable or disable the creation of layers for the wishes in this recipe")
	cmdRecipeApply.Flags().BoolVar(&createFlat, "create-flat", config.DEFAULT_CREATEFLAT, "explicitly enable or disable the creation of flat images for the wishes in this recipe")
	cmdRecipeApply.Flags().BoolVar(&createPodman, "create-podman", config.DEFAULT_CREATEPODMAN, "explicitly enable or disable the creation of podman images for the wishes in this recipe")
	cmdRecipeApply.Flags().BoolVar(&createThin, "create-thin", config.DEFAULT_CREATETHINIMAGE, "explicitly enable or disable the creation of thin images for the wishes in this recipe")
	cmdRecipeApply.Flags().StringVar(&updateIntervalStr, "update-interval", "", "explicitly set the update interval for the wishes in this recipe (e.g. 1h, 30m, 1h30m)")
	cmdRecipeApply.Flags().BoolVar(&webhookEnabled, "webhook-enabled", true, "explicitly enable or disable the webhook for the wishes in this recipe")

	cmdRecipe.AddCommand(cmdRecipeCheck)

}

var cmdRecipe = &cobra.Command{
	Use:   "recipe",
	Short: "Manage recipes",
}

var cmdRecipeApply = &cobra.Command{
	Use:          "apply <recipe file> [flags]",
	Short:        "Apply a recipe",
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Parse arguments
		var updateInterval time.Duration
		if updateIntervalStr != "" {
			interval, err := time.ParseDuration(updateIntervalStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Could not parse update interval: %s", err)
				return err
			}
			updateInterval = interval
		}

		// Open recipe file
		file, err := os.Open(args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not open recipe file: %s", err)
			return err
		}
		defer file.Close()
		recipeBytes, err := io.ReadAll(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not read recipe file: %s", err)
			return err
		}

		// Parse recipe file. This is also done by the daemon later, but we do it here to give the user early feedback
		_, err = daemon.ParseYamlRecipeV1(recipeBytes, "")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not parse recipe file: %s", err)
			return err
		}

		outputOptions := db.DefaultWishOutputOptions()
		scheduleOptions := db.DefaultWishScheduleOptions()

		// If user has set any of the flags, override the default values
		if cmd.Flags().Changed("create-layers") {
			outputOptions.CreateLayers = db.ValueWithDefault[bool]{Value: createLayers, IsDefault: false}
		}
		if cmd.Flags().Changed("create-flat") {
			outputOptions.CreateFlat = db.ValueWithDefault[bool]{Value: createFlat, IsDefault: false}
		}
		if cmd.Flags().Changed("create-podman") {
			outputOptions.CreatePodman = db.ValueWithDefault[bool]{Value: createPodman, IsDefault: false}
		}
		if cmd.Flags().Changed("create-thin") {
			outputOptions.CreateThinImage = db.ValueWithDefault[bool]{Value: createThin, IsDefault: false}
		}
		if cmd.Flags().Changed("update-interval") {
			scheduleOptions.UpdateInterval = db.ValueWithDefault[time.Duration]{Value: updateInterval, IsDefault: false}
		}
		if cmd.Flags().Changed("webhook-enabled") {
			scheduleOptions.WebhookEnabled = db.ValueWithDefault[bool]{Value: webhookEnabled, IsDefault: false}
		}

		rpcArgs := daemonCommands.ApplyRecipeArgs{
			Recipe:          string(recipeBytes),
			WishList:        cmdRecipeApply_Wishlist,
			RemoveMissing:   cmdRecipeApply_RemoveMissing,
			UpdateExisting:  cmdRecipeApply_UpdateExisting,
			OutputOptions:   outputOptions,
			ScheduleOptions: scheduleOptions,
		}
		rpcReply := daemonCommands.ApplyRecipeResponse{}

		// Connect to daemon
		client, err := rpc.Dial("unix", daemonRpc.RpcAddress)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not connect to daemonCommands. Is it running?: %s \n", err)
			return err
		}

		err = client.Call("CommandService.ApplyRecipe", rpcArgs, &rpcReply)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not apply recipe: %s \n", err)
			return err
		}
		fmt.Printf("Successfully applied recipe\n")
		fmt.Printf("%d wishes were added, %d wishes were updated, %d wishes were removed\n", len(rpcReply.Created), len(rpcReply.Updated), len(rpcReply.Removed))

		return nil
	},
}

var cmdRecipeCheck = &cobra.Command{
	Use:   "check <recipe file>",
	Short: "Check the syntax of a recipe file",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		file, err := os.Open(args[0])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not open recipe file: %s", err)
			os.Exit(1)
		}
		defer file.Close()

		recipeBytes, err := io.ReadAll(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not read recipe file: %s", err)
			os.Exit(1)
		}
		recipe, err := daemon.ParseYamlRecipeV1(recipeBytes, "")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not parse recipe file: %s", err)
			os.Exit(1)
		}

		fmt.Printf("Recipe is valid, it contains %d wish(es)\n", len(recipe.Wishes))
	},
}
