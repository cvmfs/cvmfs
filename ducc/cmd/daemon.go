package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/cvmfs/ducc/daemon"
	"github.com/cvmfs/ducc/daemon/rpc"
	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
	"github.com/cvmfs/ducc/rest"

	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"
)

var restAPI bool
var restAPIPort int
var restAPIInterface string

var automaticScheduling bool
var databasePath string

func init() {
	daemonCmd.Flags().BoolVarP(&restAPI, "rest-api", "", true, "Run the REST API server")
	daemonCmd.Flags().IntVarP(&restAPIPort, "rest-api-port", "", 8080, "Port on which to bind the REST API server")
	daemonCmd.Flags().StringVarP(&restAPIInterface, "rest-api-interface", "", "0.0.0.0", "Interface on which to bind the REST API server")

	daemonCmd.Flags().BoolVarP(&automaticScheduling, "automatic-scheduling", "", true, "Automatic scheduling of operations")

	daemonCmd.Flags().StringVarP(&databasePath, "database-path", "", "/var/lib/cvmfs/ducc/ducc.db", "Path to the DUCC database")

	rootCmd.AddCommand(daemonCmd)
}

var daemonCmd = &cobra.Command{
	Use:    "daemon [flags]",
	Short:  "Start DUCC in daemon mode",
	Args:   cobra.ExactArgs(0),
	Hidden: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Init Registries
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		if err := registry.InitRegistriesFromEnv(ctx); err != nil {
			return fmt.Errorf("error initializing registries: %w", err)
		}

		if err := os.MkdirAll(filepath.Dir(databasePath), 0755); err != nil {
			return fmt.Errorf("error creating directory for database: %w", err)
		}
		database, err := sql.Open("sqlite3", databasePath)
		if err != nil {
			return fmt.Errorf("error opening database: %w", err)
		}

		if err := db.Init(database); err != nil {
			return fmt.Errorf("error initializing database: %w", err)
		}
		defer db.Close()

		var restServer *http.Server
		var restServerErrorChan <-chan error
		var restServerCleanupDone chan any
		if restAPI {
			// Start the REST API server
			restServerCleanupDone = make(chan any) // We create a new channel here, so we can close it later
			restServer, restServerErrorChan = rest.StartRestServer(restServerCleanupDone, restAPIPort, restAPIInterface)
			// Give the rest server a little time to start up, check for errors
			select {
			case err := <-restServerErrorChan:
				return fmt.Errorf("error starting REST API server: %w", err)
			case <-time.After(100 * time.Millisecond):
			}

			fmt.Printf("Running REST API on http://%s:%d\n", restAPIInterface, restAPIPort)
		}

		// Start the daemon
		err = daemon.Init(automaticScheduling)
		if err != nil {
			return fmt.Errorf("error initializing daemon: %w", err)
		}
		daemonCtx, daemonCancelFunc := context.WithCancel(context.Background())
		daemonCleanupDone := make(chan any)
		go daemon.Run(daemonCtx, daemonCleanupDone)

		// Start RPC
		rpcCleanupDone := make(chan any)
		rpcCtx, rpcCancelFunc := context.WithCancel(context.Background())
		go rpc.RunRpcServer(rpcCtx, rpcCleanupDone)

		// Handle signals
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

		for {
			signal := <-signals
			switch signal {
			case syscall.SIGINT, syscall.SIGTERM:
				fmt.Printf("Received %s, shutting down...\n", signal)
				if restServer != nil {
					// After 5 seconds, we give up and force the server to shut down
					ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
					restServer.Shutdown(ctx)
					<-restServerCleanupDone
				}
				daemonCancelFunc()
				<-daemonCleanupDone

				rpcCancelFunc()
				<-rpcCleanupDone
				return nil
				/*
					TODO: Implement reloading of configuration
					case syscall.SIGHUP:
					fmt.Println("Received SIGHUP, reloading configuration...")
				*/
			}
		}
	},
}
