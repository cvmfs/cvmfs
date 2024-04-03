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

package driver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cvmfs/cvmfs-csi/internal/cvmfs/automount"
	"github.com/cvmfs/cvmfs-csi/internal/cvmfs/controller"
	"github.com/cvmfs/cvmfs-csi/internal/cvmfs/identity"
	"github.com/cvmfs/cvmfs-csi/internal/cvmfs/node"
	"github.com/cvmfs/cvmfs-csi/internal/grpcutils"
	"github.com/cvmfs/cvmfs-csi/internal/log"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/util/validation"
)

type (
	// Service role name.
	ServiceRole string

	// Opts holds init-time driver configuration.
	Opts struct {
		// DriverName is the name of this CSI driver that's then
		// advertised via NodeGetPluginInfo RPC.
		DriverName string

		// CSIEndpoint is URL path to the UNIX socket where the driver
		// will serve requests.
		CSIEndpoint string

		// SinglemountRunnerEndpoint is URL path to the UNIX socket
		// for connecting to the singlemount-runner.
		SinglemountRunnerEndpoint string

		// NodeID is unique identifier of the node on which this
		// CVMFS CSI node plugin pod is running.
		NodeID string

		// Role under which will the driver operate.
		Roles map[ServiceRole]bool

		// How many seconds to wait for automount daemon to start up.
		// Zero means no timeout.
		AutomountDaemonStartupTimeoutSeconds int
	}

	// Driver holds CVMFS-CSI driver runtime state.
	Driver struct {
		*Opts
	}
)

const (
	IdentityServiceRole   = "identity"   // Enable identity service role.
	NodeServiceRole       = "node"       // Enable node service role.
	ControllerServiceRole = "controller" // Enable controller service role.
)

const (
	// CVMFS-CSI driver name.
	DefaultName = "cvmfs.csi.cern.ch"

	// Maximum driver name length as per CSI spec.
	maxDriverNameLength = 63
)

var (
	errTimeout = errors.New("timed out waiting for condition")
)

func (o *Opts) validate() error {
	required := func(name, value string) error {
		if value == "" {
			return fmt.Errorf("%s is a required parameter", name)
		}

		return nil
	}

	if err := required("drivername", o.DriverName); err != nil {
		return err
	}

	if len(o.DriverName) > maxDriverNameLength {
		return fmt.Errorf("driver name too long: is %d characters, maximum is %d",
			len(o.DriverName), maxDriverNameLength)
	}

	// As per CSI spec, driver name must follow DNS format.
	if errMsgs := validation.IsDNS1123Subdomain(strings.ToLower(o.DriverName)); len(errMsgs) > 0 {
		return fmt.Errorf("driver name is invalid: %v", errMsgs)
	}

	if err := required("endpoint", o.CSIEndpoint); err != nil {
		return err
	}

	if err := required("nodeid", o.NodeID); err != nil {
		return err
	}

	return nil
}

// New creates a new instance of Driver.
func New(opts *Opts) (*Driver, error) {
	if err := opts.validate(); err != nil {
		return nil, fmt.Errorf("invalid driver options: %v", err)
	}

	return &Driver{
		Opts: opts,
	}, nil
}

func setupIdentityServiceRole(s *grpc.Server, d *Driver) error {
	log.Debugf("Registering Identity server")
	csi.RegisterIdentityServer(
		s,
		identity.New(
			d.DriverName,
			d.Opts.Roles[ControllerServiceRole],
		),
	)

	return nil
}

func tryWithTimeout(description string, timeoutSecs int, f func() (bool, error)) error {
	var (
		done bool
		err  error
	)

	for i := 0; i < timeoutSecs || timeoutSecs == 0; i++ {
		done, err = f()
		if err != nil {
			return err
		}

		if done {
			log.Debugf("%s ready", description)
			break
		}

		log.Debugf("Waiting for %s...", description)
		time.Sleep(time.Second)
	}

	if !done {
		return fmt.Errorf("timed-out waiting for %s", description)
	}

	return nil
}

func setupNodeServiceRole(s *grpc.Server, d *Driver) error {
	// First wait until autofs in /cvmfs is ready.

	err := tryWithTimeout(
		"autofs in /cvmfs",
		d.Opts.AutomountDaemonStartupTimeoutSeconds,
		func() (bool, error) { return automount.IsAutofs("/cvmfs") },
	)
	if err != nil {
		return err
	}

	// We can register node server now.

	ns := node.New(d.NodeID, d.Opts.SinglemountRunnerEndpoint)

	caps, err := ns.NodeGetCapabilities(
		context.TODO(),
		&csi.NodeGetCapabilitiesRequest{},
	)
	if err != nil {
		return fmt.Errorf("failed to get Node server capabilities: %v", err)
	}

	log.Debugf("Registering Node server with capabilities %+v", caps.GetCapabilities())
	csi.RegisterNodeServer(s, ns)

	return nil
}

func setupControllerServiceRole(s *grpc.Server, d *Driver) error {
	cs := controller.New()

	caps, err := cs.ControllerGetCapabilities(
		context.TODO(),
		&csi.ControllerGetCapabilitiesRequest{},
	)
	if err != nil {
		return fmt.Errorf("failed to get Controller server capabilities: %v", err)
	}

	log.Debugf("Registering Controller server with capabilities %+v", caps.GetCapabilities())
	csi.RegisterControllerServer(s, cs)

	return nil
}

// Run starts CSI services and blocks.
func (d *Driver) Run() error {
	log.Infof("Driver: %s", d.DriverName)

	s, err := grpcutils.NewServer(d.CSIEndpoint, grpc.UnaryInterceptor(grpcLogger))
	if err != nil {
		return fmt.Errorf("failed to create GRPC server: %v", err)
	}

	if d.Opts.Roles[IdentityServiceRole] {
		if err = setupIdentityServiceRole(s.GRPCServer, d); err != nil {
			return fmt.Errorf("failed to setup identity service role: %v", err)
		}
	}

	if d.Opts.Roles[NodeServiceRole] {
		if err = setupNodeServiceRole(s.GRPCServer, d); err != nil {
			return fmt.Errorf("failed to setup node service role: %v", err)
		}
	}

	if d.Opts.Roles[ControllerServiceRole] {
		if err = setupControllerServiceRole(s.GRPCServer, d); err != nil {
			return fmt.Errorf("failed to setup controller service role: %v", err)
		}
	}

	return s.Serve()
}
