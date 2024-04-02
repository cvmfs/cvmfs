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

	"github.com/cvmfs-contrib/cvmfs-csi/internal/cvmfs/singlemount"
	"github.com/cvmfs-contrib/cvmfs-csi/internal/log"
	cvmfsversion "github.com/cvmfs-contrib/cvmfs-csi/internal/version"

	"k8s.io/klog/v2"
)

var (
	version  = flag.Bool("version", false, "Print singlemount-runner version and exit.")
	endpoint = flag.String("endpoint", "unix:///var/lib/cvmfs.cern.ch/singlemount-runner.sock", "singlemount-runner endpoint.")
)

func main() {
	// Handle flags and initialize logging.

	klog.InitFlags(nil)
	if err := flag.Set("logtostderr", "true"); err != nil {
		klog.Exitf("failed to set logtostderr flag: %v", err)
	}
	flag.Parse()

	if *version {
		fmt.Println("singlemount-runner for CVMFS CSI plugin version", cvmfsversion.FullVersion())
		os.Exit(0)
	}

	// Initialize and run singlemount-runner.

	log.Infof("singlemount-runner for CVMFS CSI plugin version %s", cvmfsversion.FullVersion())
	log.Infof("Command line arguments %v", os.Args)

	if err := singlemount.CreateSingleMountsDir(); err != nil {
		log.Fatalf("Failed to create metadata directory in %s: %v", singlemount.SinglemountsDir, err)
	}

	opts := singlemount.Opts{
		Endpoint: *endpoint,
	}

	if err := singlemount.RunBlocking(opts); err != nil {
		log.Fatalf("Failed to run singlemount-runner: %v", err)
	}

	os.Exit(0)
}
