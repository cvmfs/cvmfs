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
	"github.com/cvmfs/ducc/errorcodes"
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
	Run: func(cmd *cobra.Command, args []string) {
		// Init Registries
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		registry.InitRegistries(ctx, nil, nil)

		if err := os.MkdirAll(filepath.Dir(databasePath), 0755); err != nil {
			fmt.Printf("Error creating directory for database: %s\n", err)
			os.Exit(errorcodes.OpenDatabaseFileError)
		}
		database, err := sql.Open("sqlite3", databasePath)
		if err != nil {
			fmt.Printf("Error opening database: %s\n", err)
			os.Exit(errorcodes.OpenDatabaseFileError)
		}

		if err := db.Init(database); err != nil {
			fmt.Printf("Error initializing database: %s\n", err)
			os.Exit(errorcodes.DatabaseError)
		}
		defer db.Close()

		var restServer *http.Server
		var restServerCleanupDone chan any
		if restAPI {
			// Start the REST API server
			restServerCleanupDone = make(chan any) // We create a new channel here, so we can close it later
			restServer = rest.StartRestServer(restServerCleanupDone, restAPIPort, restAPIInterface)
			fmt.Printf("Running REST API on http://%s:%d\n", restAPIInterface, restAPIPort)
		}

		// Start the daemon
		err = daemon.Init(automaticScheduling)
		if err != nil {
			fmt.Printf("Error initializing daemon: %s\n", err)
			os.Exit(errorcodes.InternalLogicError)
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
				fmt.Println("Received SIGINT or SIGTERM, shutting down...")
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
				return
				/*
					TODO: Implement reloading of configuration
					case syscall.SIGHUP:
					fmt.Println("Received SIGHUP, reloading configuration...")
				*/
			}
		}
	},
}
