package cmd

import (
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"sort"
	"time"

	"github.com/cvmfs/ducc/config"
	daemonCommands "github.com/cvmfs/ducc/daemon/commands"
	daemonRpc "github.com/cvmfs/ducc/daemon/rpc"
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

var syncAllImages bool
var wishlist string

func init() {
	rootCmd.AddCommand(cmdWish)

	cmdWish.AddCommand(cmdWishAdd)
	cmdWishAdd.Flags().StringVar(&wishlist, "wishlist", "default", "add the wish to the specified wishlist")
	cmdWishAdd.Flags().StringVar(&source, "source", "cli", "source used for the wish list entry")
	cmdWishAdd.Flags().BoolVar(&createLayers, "create-layers", config.DEFAULT_CREATELAYERS, "explicitly enable or disable the creation of layers for this wish")
	cmdWishAdd.Flags().BoolVar(&createFlat, "create-flat", config.DEFAULT_CREATEFLAT, "explicitly enable or disable the creation of flat images for this wish")
	cmdWishAdd.Flags().BoolVar(&createPodman, "create-podman", config.DEFAULT_CREATEPODMAN, "explicitly enable or disable the creation of podman images for this wish")
	cmdWishAdd.Flags().BoolVar(&createThin, "create-thin", config.DEFAULT_CREATETHINIMAGE, "explicitly enable or disable the creation of thin images for this wish")
	cmdWishAdd.Flags().StringVar(&updateIntervalStr, "update-interval", "", "explicitly set the update interval for this wish (e.g. 1h, 30m, 1h30m)")
	cmdWishAdd.Flags().BoolVar(&webhookEnabled, "webhook-enabled", true, "explicitly enable or disable the webhook for this wish")

	cmdWish.AddCommand(cmdWishLs)

	cmdWish.AddCommand(cmdWishSync)
	cmdWishSync.Flags().BoolVarP(&syncAllImages, "all", "a", false, "trigger a sync for all images matching the wish, not just any new tags")
	cmdWishSync.Flags().StringVar(&wishlist, "wishlist", "default", "add the wish to the specified wishlist")

	cmdWish.AddCommand(cmdWishRm)
}

var cmdWish = &cobra.Command{
	Use:   "wish",
	Short: "Manage wishes",
}

var cmdWishAdd = &cobra.Command{
	Use:   "add <image identifier> <cvmfs repository> [flags]",
	Short: "Add a single image to a DUCC wishlist",
	Args:  cobra.ExactArgs(2),
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

		// Connect to daemon
		client, err := rpc.Dial("unix", daemonRpc.RpcAddress)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not connect to daemonCommands. Is it running?: %s \n", err)
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

		rpcArgs := daemonCommands.AddWishArgs{
			ImageIdentifier: args[0],
			CvmfsRepository: args[1],
			Wishlist:        wishlist,

			OutputOptions:   outputOptions,
			ScheduleOptions: scheduleOptions,
		}

		rpcResult := daemonCommands.AddWishResponse{}
		err = client.Call("CommandService.AddWish", rpcArgs, &rpcResult)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running command: %s\n", err)
			return err
		}

		fmt.Printf("%s\n", string(rpcResult.ID.String()))
		return nil
	},
}

var cmdWishLs = &cobra.Command{
	Use:   "ls",
	Short: "List all wishes",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Connect to daemon
		client, err := rpc.Dial("unix", daemonRpc.RpcAddress)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not connect to daemonCommands. Is it running?: %s \n", err)
			return err
		}

		rpcArgs := daemonCommands.ListWishesArgs{}
		rpcResult := daemonCommands.ListWishesResponse{}

		err = client.Call("CommandService.ListWishes", rpcArgs, &rpcResult)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running command: %s\n", err)
			return err
		}

		wishes := rpcResult.Wishes
		// Sort wishes by source and input string
		sort.Slice(wishes, func(i, j int) bool {
			if rpcResult.Wishes[i].Identifier.Wishlist == rpcResult.Wishes[j].Identifier.Wishlist {
				return rpcResult.Wishes[i].Identifier.InputString() < rpcResult.Wishes[j].Identifier.InputString()
			}
			return rpcResult.Wishes[i].Identifier.Wishlist < rpcResult.Wishes[j].Identifier.Wishlist
		})

		for _, wish := range wishes {
			fmt.Printf("%s\t%s\t%s\n", wish.ID.String()[:8], wish.Identifier.InputString(), wish.Identifier.Wishlist)
		}
		return nil
	},
}

var cmdWishSync = &cobra.Command{
	Use:   "sync <wish id>...",
	Short: "trigger a sync for the specified wish(es)",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Connect to daemon
		client, err := rpc.Dial("unix", daemonRpc.RpcAddress)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not connect to daemonCommands. Is it running?: %s \n", err)
			return err
		}

		rpcArgs := daemonCommands.SyncWishesArgs{
			IDs:               args,
			ForceUpdateImages: syncAllImages,
		}
		rpcResult := daemonCommands.SyncWishesResponse{}
		err = client.Call("CommandService.UpdateWish", rpcArgs, &rpcResult)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running command: %s\n", err)
			return err
		}

		shouldNewline := false

		if len(rpcResult.Synced) > 0 {
			shouldNewline = true
			fmt.Printf("Triggered sync for %d wish(es):\n", len(rpcResult.Synced))
			for _, wish := range rpcResult.Synced {
				fmt.Printf("%s\t%s\t%s\n", wish.ID.String()[:8], wish.Identifier.InputString(), wish.Identifier.Wishlist)
			}
		}

		if len(rpcResult.NotFound) > 0 {
			if shouldNewline {
				fmt.Printf("\n")
			}
			shouldNewline = true
			fmt.Printf("%d input arguments did not match any wish:\n", len(rpcResult.NotFound))
			for _, id := range rpcResult.NotFound {
				fmt.Printf("%s\n", id)
			}
		}

		if len(rpcResult.Ambiguous) > 0 {
			if shouldNewline {
				fmt.Printf("\n")
			}
			shouldNewline = true
			fmt.Printf("%d input arguments matched multiple wishes:\n", len(rpcResult.Ambiguous))
			for _, ambiguous := range rpcResult.Ambiguous {
				fmt.Printf("%s matched %d wishes\n", ambiguous.Input, len(ambiguous.MatchedWishes))
				for _, wish := range ambiguous.MatchedWishes {
					fmt.Printf("\t%s\t%s\t%s\n", wish.ID.String()[:8], wish.Identifier.InputString(), wish.Identifier.Wishlist)
				}
			}

		}
		if (len(rpcResult.NotFound) > 0) || (len(rpcResult.Ambiguous) > 0) {
			return errors.New("")
		}
		return nil
	},
}

var cmdWishRm = &cobra.Command{
	Use:   "rm <wish id>...",
	Short: "Delete one or more spesific wish(es) by id",
	Args:  cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Connect to daemon
		client, err := rpc.Dial("unix", daemonRpc.RpcAddress)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not connect to daemonCommands. Is it running?: %s \n", err)
			return err
		}
		rpcArgs := daemonCommands.DeleteWishesArgs{
			IDs: args,
		}
		rpcResult := daemonCommands.DeleteWishesResponse{}
		err = client.Call("CommandService.DeleteWishes", rpcArgs, &rpcResult)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running command: %s\n", err)
			return err
		}

		shouldNewline := false

		if len(rpcResult.Deleted) > 0 {
			shouldNewline = true
			fmt.Printf("Deleted %d wish(es):\n", len(rpcResult.Deleted))
			for _, wish := range rpcResult.Deleted {
				fmt.Printf("%s\t%s\t%s\n", wish.ID.String()[:8], wish.Identifier.InputString(), wish.Identifier.Wishlist)
			}
		}

		if len(rpcResult.NotFound) > 0 {
			if shouldNewline {
				fmt.Printf("\n")
			}
			shouldNewline = true
			fmt.Printf("%d input arguments did not match any wish:\n", len(rpcResult.NotFound))
			for _, id := range rpcResult.NotFound {
				fmt.Printf("%s\n", id)
			}
		}

		if len(rpcResult.Ambiguous) > 0 {
			if shouldNewline {
				fmt.Printf("\n")
			}
			shouldNewline = true
			fmt.Printf("%d input arguments matched multiple wishes:\n", len(rpcResult.Ambiguous))
			for _, ambiguous := range rpcResult.Ambiguous {
				fmt.Printf("%s matched %d wishes\n", ambiguous.Input, len(ambiguous.MatchedWishes))
				for _, wish := range ambiguous.MatchedWishes {
					fmt.Printf("\t%s\t%s\t%s\n", wish.ID.String()[:8], wish.Identifier.InputString(), wish.Identifier.Wishlist)
				}
			}
		}
		if len(rpcResult.Error) > 0 {
			if shouldNewline {
				fmt.Printf("\n")
			}
			shouldNewline = true
			fmt.Printf("error(s) occured while deleting %d wishes:\n", len(rpcResult.Error))
			for _, wish := range rpcResult.Error {
				fmt.Printf("%s\t%s\t%s\t - %s\n", wish.Wish.ID.String()[:8], wish.Wish.Identifier.InputString(), wish.Wish.Identifier.Wishlist, wish.Err)
			}
		}

		if (len(rpcResult.NotFound) > 0) || (len(rpcResult.Ambiguous) > 0) || (len(rpcResult.Error) > 0) {
			return errors.New("")
		}

		return nil
	},
}
