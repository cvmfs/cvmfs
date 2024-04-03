// Copyright CERN.
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/cvmfs/cvmfs-csi/internal/cvmfs/automount/reconciler"
	"github.com/cvmfs/cvmfs-csi/internal/log"
	cvmfsversion "github.com/cvmfs/cvmfs-csi/internal/version"

	"k8s.io/klog/v2"
)

var (
	version = flag.Bool("version", false, "Print driver version and exit.")
	period  = flag.Duration("period", time.Second*30, "How often to check and reconcile autofs-managed CVMFS mounts.")
)

func main() {
	// Handle flags and initialize logging.

	klog.InitFlags(nil)
	if err := flag.Set("logtostderr", "true"); err != nil {
		klog.Exitf("failed to set logtostderr flag: %v", err)
	}
	flag.Parse()

	if *version {
		fmt.Println("automount-reconciler for CVMFS CSI plugin version", cvmfsversion.FullVersion())
		os.Exit(0)
	}

	// Initialize and run mount-reconciler.

	log.Infof("automount-reconciler for CVMFS CSI plugin version %s", cvmfsversion.FullVersion())
	log.Infof("Command line arguments %v", os.Args)

	// Run blocking.

	err := mountreconcile.RunBlocking(&mountreconcile.Opts{
		Period: *period,
	})
	if err != nil {
		log.Fatalf("Failed to run mount-reconciler: %v", err)
	}

	os.Exit(0)
}
