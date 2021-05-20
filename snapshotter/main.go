/*
   Copyright The containerd Authors.
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"context"
	"flag"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"google.golang.org/grpc"

	"github.com/BurntSushi/toml"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/contrib/snapshotservice"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/snapshots"
	snbase "github.com/containerd/stargz-snapshotter/snapshot"
	"github.com/sirupsen/logrus"

	"github.com/cvmfs/cvmfs/snapshotter/cvmfs"
)

const (
	defaultAddress  = "/run/containerd-cvmfs-grpc/containerd-cvmfs-grpc.sock"
	defaultLogLevel = logrus.InfoLevel
	defaultRootDir  = "/var/lib/containerd-cvmfs-grpc"
)

var (
	address    = flag.String("address", defaultAddress, "address for the snapshotter's GRPC server")
	configPath = flag.String("config", "", "path to the configuration file")
	logLevel   = flag.String("log-level", defaultLogLevel.String(), "set the logging level [trace, debug, info, warn, error, fatal, panic]")
	rootDir    = flag.String("root", defaultRootDir, "path to the root directory for this snapshotter")
)

func main() {
	flag.Parse()
	lvl, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		log.L.WithError(err).Fatal("failed to prepare logger")
	}
	logrus.SetLevel(lvl)
	logrus.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: log.RFC3339NanoFixed,
	})

	var (
		ctx    = log.WithLogger(context.Background(), log.L)
		config = &cvmfs.Config{}
	)

	// Get configuration from specified file
	if *configPath != "" {
		if _, err := toml.DecodeFile(*configPath, &config); err != nil {
			log.G(ctx).WithError(err).Fatalf("failed to load config file %q", *configPath)
		}
	}

	// Configure filesystem and snapshotter
	fs, err := cvmfs.NewFilesystem(ctx, filepath.Join(*rootDir, "cvmfs"), config)
	if err != nil {
		log.G(ctx).WithError(err).Fatalf("failed to configure filesystem")
	}
	var rs snapshots.Snapshotter
	rs, err = snbase.NewSnapshotter(ctx, filepath.Join(*rootDir, "snapshotter"), fs, snbase.AsynchronousRemove)
	if err != nil {
		log.G(ctx).WithError(err).Warning("failed to configure snapshotter using the previous configuration")
		os.RemoveAll(filepath.Join(*rootDir, "snapshotter"))
		rs, err = snbase.NewSnapshotter(ctx, filepath.Join(*rootDir, "snapshotter"), fs, snbase.AsynchronousRemove)
		if err != nil {
			log.G(ctx).WithError(err).Fatalf("failed to configure snapshotter starting from a clean configuration")
		}
	}
	defer func() {
		log.G(ctx).Debug("Closing the snapshotter")
		rs.Close()
		log.G(ctx).Info("Exiting")
	}()

	// Create a gRPC server
	rpc := grpc.NewServer()

	// Convert the snapshotter to a gRPC service,
	service := snapshotservice.FromSnapshotter(rs)

	// Register the service with the gRPC server
	snapshotsapi.RegisterSnapshotsServer(rpc, service)

	// Prepare the directory for the socket
	if err := os.MkdirAll(filepath.Dir(*address), 0700); err != nil {
		log.G(ctx).WithError(err).Fatalf("failed to create directory %q", filepath.Dir(*address))
	}

	// Try to remove the socket file to avoid EADDRINUSE
	if err := os.RemoveAll(*address); err != nil {
		log.G(ctx).WithError(err).Fatalf("failed to remove %q", *address)
	}

	// Listen and serve
	l, err := net.Listen("unix", *address)
	if err != nil {
		log.G(ctx).WithError(err).Fatalf("error on listen socket %q", *address)
	}
	go func() {
		if err := rpc.Serve(l); err != nil {
			log.G(ctx).WithError(err).Fatalf("error on serving via socket %q", *address)
		}
	}()
	waitForSignal(fs.(*cvmfs.Filesystem))
	log.G(ctx).Info("Got SIGINT")
}

func waitForSignal(fs *cvmfs.Filesystem) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	fs.UnmountAll(context.TODO())
}
