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
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"

	"google.golang.org/grpc"

	"github.com/BurntSushi/toml"
	snapshotsapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/v2/contrib/snapshotservice"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/log"
	snbase "github.com/containerd/stargz-snapshotter/snapshot"
	"github.com/moby/sys/mountinfo"
	"github.com/sirupsen/logrus"

	"github.com/cvmfs/cvmfs/snapshotter/cvmfs"
)

const (
	defaultAddress  = "/run/containerd-cvmfs-grpc/containerd-cvmfs-grpc.sock"
	defaultLogLevel = logrus.InfoLevel
	defaultRootDir  = "/var/lib/containerd-cvmfs-grpc"
)

var (
	address      = flag.String("address", defaultAddress, "address for the snapshotter's GRPC server")
	configPath   = flag.String("config", "/etc/containerd-cvmfs-grpc/config.toml", "path to the configuration file")
	logLevel     = flag.String("log-level", defaultLogLevel.String(), "set the logging level [trace, debug, info, warn, error, fatal, panic]")
	rootDir      = flag.String("root", defaultRootDir, "path to the root directory for this snapshotter")
	printVersion = flag.Bool("version", false, "print the version number")
)

var Version = "development"

func main() {
	flag.Parse()

	if *printVersion {
		fmt.Println("CernVM-FS Containerd Snapshotter:\t", Version)
		os.Exit(0)
	}

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
	detachLayerMounts(ctx, filepath.Join(*rootDir, "snapshotter", "snapshots"))
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

// detachLayerMounts lazily unmounts layer bind mounts left by a snapshotter
// that did not shut down cleanly. The stargz restore step would unmount them
// with MNT_FORCE, which aborts the host's cvmfs FUSE connection. Running
// containers keep their own references to the layers.
func detachLayerMounts(ctx context.Context, snapshotsDir string) {
	mounts, err := mountinfo.GetMounts(mountinfo.PrefixFilter(snapshotsDir))
	if err != nil {
		log.G(ctx).WithError(err).Warn("failed to list leftover layer mounts")
		return
	}
	// Unmount nested mounts before their parents
	sort.Slice(mounts, func(i, j int) bool {
		return len(mounts[i].Mountpoint) > len(mounts[j].Mountpoint)
	})
	for _, m := range mounts {
		log.G(ctx).WithField("mountpoint", m.Mountpoint).Info("Detaching leftover layer mount")
		if err := syscall.Unmount(m.Mountpoint, syscall.MNT_DETACH); err != nil {
			log.G(ctx).WithError(err).WithField("mountpoint", m.Mountpoint).Warn("failed to detach leftover layer mount")
		}
	}
}

func waitForSignal(fs *cvmfs.Filesystem) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	fs.UnmountAll(context.TODO())
}
