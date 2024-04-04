package sanity

import (
	"fmt"
	"os"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/cvmfs/csi/internal/cvmfs/driver"
	"github.com/kubernetes-csi/csi-test/v5/pkg/sanity"
)

func TestDriverSuite(t *testing.T) {
	socket := fmt.Sprintf("/var/lib/kubelet/plugins/%s/csi.sock", driver.DefaultName)
	endpoint := "unix://" + socket
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		t.Fatalf("failed to remove unix domain socket file %s, error: %s", socket, err)
	}

	var roles = []driver.ServiceRole{driver.IdentityServiceRole, driver.NodeServiceRole, driver.ControllerServiceRole}
	driverRoles := make(map[driver.ServiceRole]bool, len(roles))
	for _, role := range roles {
		driverRoles[role] = true
	}
	driver := driver.Driver{Opts: &driver.Opts{
		DriverName:                driver.DefaultName,
		CSIEndpoint:               endpoint,
		SinglemountRunnerEndpoint: "unix:///var/lib/cvmfs.cern.ch/singlemount-runner.sock",
		Roles:                     driverRoles,

		NodeID:                               "1",
		AutomountDaemonStartupTimeoutSeconds: 10,
	}}

	go func() {
		driver.Run()
	}()

	cfg := sanity.NewTestConfig()
	if err := os.RemoveAll(cfg.TargetPath); err != nil {
		t.Fatalf("failed to delete target path %s: %s", cfg.TargetPath, err)
	}
	if err := os.RemoveAll(cfg.StagingPath); err != nil {
		t.Fatalf("failed to delete staging path %s: %s", cfg.StagingPath, err)
	}
	cfg.Address = endpoint
	cfg.IdempotentCount = 0
	sc := sanity.NewTestContext(&cfg)
	sanity.TestVolumeCapabilityWithAccessType(sc, csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY)
}
