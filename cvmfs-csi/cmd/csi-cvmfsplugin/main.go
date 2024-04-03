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
	"strings"

	"github.com/cvmfs/cvmfs-csi/internal/cvmfs/driver"
	"github.com/cvmfs/cvmfs-csi/internal/log"
	cvmfsversion "github.com/cvmfs/cvmfs-csi/internal/version"

	"k8s.io/klog/v2"
)

type rolesFlag []driver.ServiceRole

func (rf rolesFlag) String() string {
	return fmt.Sprintf("%v", []driver.ServiceRole(rf))
}

var (
	knownServiceRoles = map[driver.ServiceRole]struct{}{
		driver.IdentityServiceRole:   {},
		driver.NodeServiceRole:       {},
		driver.ControllerServiceRole: {},
	}
)

func (rf *rolesFlag) Set(newRoleFlag string) error {
	for _, part := range strings.Split(newRoleFlag, ",") {
		if _, ok := knownServiceRoles[driver.ServiceRole(part)]; !ok {
			return fmt.Errorf("unknown role %s", part)
		}

		*rf = append(*rf, driver.ServiceRole(part))
	}

	return nil
}

var (
	endpoint   = flag.String("endpoint", fmt.Sprintf("unix:///var/lib/kubelet/plugins/%s/csi.sock", driver.DefaultName), "CSI endpoint.")
	driverName = flag.String("drivername", driver.DefaultName, "Name of the driver.") //nolint
	nodeId     = flag.String("nodeid", "", "Node id.")
	version    = flag.Bool("version", false, "Print driver version and exit.")
	roles      rolesFlag

	hasAlienCache             = flag.Bool("has-alien-cache", false, "(DEPRECATED: use automount-runner --has-alien-cache) CVMFS client is using alien cache volume")
	startAutomountDaemon      = flag.Bool("start-automount-daemon", true, "(DEPRECATED: use automount-runner) start automount daemon when initializing CVMFS CSI driver")
	singlemountRunnerendpoint = flag.String("singlemount-runner-endpoint", "unix:///var/lib/cvmfs.cern.ch/singlemount-runner.sock", "singlemount-runner endpoint.")

	automountDaemonStartupTimeoutSeconds   = flag.Int("automount-startup-timeout", 10, "number of seconds to wait for automount daemon to start up before giving up and exiting. '0' means wait forever")
	automountDaemonUnmountAfterIdleSeconds = flag.Int("automount-unmount-timeout", 300, "(DEPRECATED: use automount-runner --unmount-timeout) number of seconds of idle time after which an autofs-managed CVMFS mount will be unmounted. '0' means never unmount, '-1' leaves automount default option.")
)

func main() {
	// Handle flags and initialize logging.

	flag.Var(&roles, "role", "Enable driver service role (comma-separated list or repeated --role flags). Allowed values are: 'identity', 'node', 'controller'.")

	klog.InitFlags(nil)
	if err := flag.Set("logtostderr", "true"); err != nil {
		klog.Exitf("failed to set logtostderr flag: %v", err)
	}
	flag.Parse()

	if *version {
		fmt.Println("CVMFS CSI plugin version", cvmfsversion.FullVersion())
		os.Exit(0)
	}

	// Initialize and run the driver.

	log.Infof("CVMFS CSI plugin version %s", cvmfsversion.FullVersion())
	log.Infof("Command line arguments %v", os.Args)

	driverRoles := make(map[driver.ServiceRole]bool, len(roles))
	for _, role := range roles {
		driverRoles[role] = true
	}

	driver, err := driver.New(&driver.Opts{
		DriverName:                *driverName,
		CSIEndpoint:               *endpoint,
		SinglemountRunnerEndpoint: *singlemountRunnerendpoint,
		NodeID:                    *nodeId,
		Roles:                     driverRoles,

		AutomountDaemonStartupTimeoutSeconds: *automountDaemonStartupTimeoutSeconds,
	})

	if err != nil {
		log.Fatalf("Failed to initialize the driver: %v", err)
	}

	err = driver.Run()
	if err != nil {
		log.Fatalf("Failed to run the driver: %v", err)
	}

	os.Exit(0)
}
