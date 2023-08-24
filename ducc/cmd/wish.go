package cmd

import (
	"fmt"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/cvmfs/ducc/config"
	"github.com/cvmfs/ducc/daemon"
	"github.com/cvmfs/ducc/db"
	"github.com/spf13/cobra"
)

var source string

var createLayers bool
var createFlat bool
var createPodman bool
var createThin bool

var updateIntervalStr string
var webhookEnabled bool

var longList bool

var updateAllImages bool

func init() {
	rootCmd.AddCommand(cmdWish)

	cmdWish.AddCommand(cmdWishAdd)
	cmdWishAdd.Flags().StringVar(&source, "source", "cli", "source used for the wish list entry")
	cmdWishAdd.Flags().BoolVar(&createLayers, "create-layers", config.DEFAULT_CREATELAYERS, "")
	cmdWishAdd.Flags().BoolVar(&createFlat, "create-flat", config.DEFAULT_CREATEFLAT, "")
	cmdWishAdd.Flags().BoolVar(&createPodman, "create-podman", config.DEFAULT_CREATEPODMAN, "")
	cmdWishAdd.Flags().BoolVar(&createThin, "create-thin", config.DEFAULT_CREATETHINIMAGE, "")
	cmdWishAdd.Flags().StringVar(&updateIntervalStr, "update-interval", "", "update interval for the wish list entry, e.g. 1h30m. If blank, automatic updates are disabled.")
	cmdWishAdd.Flags().BoolVar(&webhookEnabled, "webhook-enabled", true, "enable triggering updates via incoming webhook (experimental)")

	cmdWish.AddCommand(cmdWishLs)

	cmdWish.AddCommand(cmdWishUpdate)
	cmdWishUpdate.Flags().BoolVarP(&updateAllImages, "all", "a", false, "trigger an instant update for all images matching the wish, not just any new tags")
}

var cmdWish = &cobra.Command{
	Use:   "wish",
	Short: "Manage the DUCC wish list",
}

var cmdWishAdd = &cobra.Command{
	Use:   "add <image identifier> <cvmfs repository> [flags]",
	Short: "Add a single image to the DUCC wish list",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		// Parse arguments
		var updateInterval time.Duration
		if updateIntervalStr != "" {
			interval, err := time.ParseDuration(updateIntervalStr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Could not parse update interval: %s", err)
				os.Exit(1)
			}
			updateInterval = interval
		}

		// Connect to daemon
		client, err := rpc.Dial("unix", daemon.RpcAddress)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not connect to daemon. Is it running?: %s \n", err)
			os.Exit(1)
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

		rpcArgs := daemon.AddWishArgs{
			ImageIdentifier: args[0],
			CvmfsRepository: args[1],

			OutputOptions:   outputOptions,
			ScheduleOptions: scheduleOptions,
		}

		rpcResult := daemon.AddWishResponse{}
		err = client.Call("CommandService.AddWish", rpcArgs, &rpcResult)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running command: %s\n", err)
			os.Exit(1)
		}

		fmt.Printf("%s\n", string(rpcResult.ID.String()))
	},
}

var cmdWishLs = &cobra.Command{
	Use:   "ls",
	Short: "List all wishes",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		// Connect to daemon
		client, err := rpc.Dial("unix", daemon.RpcAddress)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not connect to daemon. Is it running?: %s \n", err)
			os.Exit(1)
		}

		rpcArgs := daemon.ListWishesArgs{}
		rpcResult := daemon.ListWishesResponse{}

		err = client.Call("CommandService.ListWishes", rpcArgs, &rpcResult)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running command: %s\n", err)
			os.Exit(1)
		}

		wishes := rpcResult.Wishes
		// Sort wishes by source and input string
		sort.Slice(wishes, func(i, j int) bool {
			if rpcResult.Wishes[i].Identifier.Source == rpcResult.Wishes[j].Identifier.Source {
				return rpcResult.Wishes[i].Identifier.InputString() < rpcResult.Wishes[j].Identifier.InputString()
			}
			return rpcResult.Wishes[i].Identifier.Source < rpcResult.Wishes[j].Identifier.Source
		})

		for _, wish := range wishes {
			fmt.Printf("%s\t%s\t%s\n", wish.ID.String()[:8], wish.Identifier.InputString(), wish.Identifier.Source)
		}

	},
}

var cmdWishUpdate = &cobra.Command{
	Use:   "update <wish id>",
	Short: "Update a spesific wish",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// Connect to daemon
		client, err := rpc.Dial("unix", daemon.RpcAddress)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not connect to daemon. Is it running?: %s \n", err)
			os.Exit(1)
		}

		rpcArgs := daemon.UpdateWishArgs{
			ID:                args[0],
			ForceUpdateImages: updateAllImages,
		}
		rpcResult := daemon.UpdateWishResponse{}
		err = client.Call("CommandService.UpdateWish", rpcArgs, &rpcResult)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running command: %s\n", err)
			os.Exit(1)
		}

		if !rpcResult.Success {
			if len(rpcResult.MatchedWishes) == 0 {
				fmt.Fprintf(os.Stderr, "No wishes matched the given ID\n")
				os.Exit(1)
			} else {
				fmt.Fprintf(os.Stderr, "Multiple wishes matched the given ID:\n")
				for _, wish := range rpcResult.MatchedWishes {
					fmt.Fprintf(os.Stderr, "%s\t%s\t%s\n", wish.ID.String()[:8], wish.Identifier.InputString(), wish.Identifier.Source)
					os.Exit(1)
				}
			}
		}

		wish := rpcResult.MatchedWishes[0]
		fmt.Printf("%s\t%s\t%s\n", wish.ID.String()[:8], wish.Identifier.InputString(), wish.Identifier.Source)
		fmt.Printf("%s\n", string(rpcResult.Trigger.ID.String()))
	},
}
