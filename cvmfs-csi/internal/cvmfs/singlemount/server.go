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

package singlemount

import (
	"bytes"
	"context"
	"fmt"
	"os"
	goexec "os/exec"
	"sync"

	pb "github.com/cvmfs-contrib/cvmfs-csi/internal/cvmfs/singlemount/pb/v1"
	"github.com/cvmfs-contrib/cvmfs-csi/internal/exec"
	"github.com/cvmfs-contrib/cvmfs-csi/internal/grpcutils"
	"github.com/cvmfs-contrib/cvmfs-csi/internal/log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	singleMountServer struct {
		pb.UnimplementedSingleServer

		// Track pending Mount/Unmount calls (key'd by mount ID).
		// Return status.Aborted if such a call is pending.
		pendingOps sync.Map
	}

	Opts struct {
		Endpoint string
	}
)

func cvmfsVersion() (string, error) {
	out, err := exec.CombinedOutput(goexec.Command("cvmfs2", "--version"))
	if err != nil {
		return "", fmt.Errorf("failed to get CVMFS version: %v", err)
	}

	return string(bytes.TrimSpace(out)), nil
}

func RunBlocking(o Opts) error {
	ver, err := cvmfsVersion()
	if err != nil {
		return err
	}

	log.Infof("%s", ver)

	s, err := grpcutils.NewServer(o.Endpoint, grpc.UnaryInterceptor(grpcLogger))
	if err != nil {
		return err
	}

	pb.RegisterSingleServer(s.GRPCServer, &singleMountServer{})

	return s.Serve()
}

func checkNotEmpty(value, name string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", name)
	}
	return nil
}

func validateMountSingleRequest(req *pb.MountSingleRequest) error {
	if err := checkNotEmpty(req.MountId, "mountId"); err != nil {
		return err
	}

	if err := checkNotEmpty(req.Repository, "repository"); err != nil {
		return err
	}

	if (req.Config == "" && req.ConfigFilepath == "") ||
		(req.Config != "" && req.ConfigFilepath != "") {
		return fmt.Errorf("exactly one of config and config_filepath must be non-empty")
	}

	if err := checkNotEmpty(req.Config, "config"); err != nil {
		return err
	}

	if err := checkNotEmpty(req.Target, "target"); err != nil {
		return err
	}

	return nil
}

func populateMountSingleRequest(req *pb.MountSingleRequest) error {
	if req.ConfigFilepath != "" {
		configContents, err := os.ReadFile(req.ConfigFilepath)
		if err != nil {
			return fmt.Errorf("failed to read config file from request: %v", err)
		}

		req.Config = string(configContents)
	}

	return nil
}

func validateUnmountSingleRequest(req *pb.UnmountSingleRequest) error {
	if err := checkNotEmpty(req.Mountpoint, "mountpoint"); err != nil {
		return err
	}

	return nil
}

func (s *singleMountServer) Mount(
	ctx context.Context,
	req *pb.MountSingleRequest,
) (*pb.MountSingleResponse, error) {
	var err error

	if err = validateMountSingleRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err = populateMountSingleRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if _, isPending := s.pendingOps.LoadOrStore(req.MountId, true); isPending {
		return nil, status.Errorf(codes.Aborted, "operation for %s already in progress", req.Target)
	}
	defer s.pendingOps.Delete(req.MountId)

	if err = checkMountMetadataMatches(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			"bad request for mount ID %s: %v", req.MountId, err)
	}

	if err = addMountpointMetadata(req.Target, req.MountId); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register mount target: %v", err)
	}

	// Clean up after addMountpointMetadata().
	defer ifErr(
		&err,
		func() {
			if err2 := deleteMountpointMetadata(req.Target); err2 != nil {
				log.Errorf("failed to clean up mountpoint metadata: %v", err2)
			}
		},
	)

	if err = makeSharedMount(req); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &pb.MountSingleResponse{}, nil
}

func (s *singleMountServer) Unmount(
	ctx context.Context,
	req *pb.UnmountSingleRequest,
) (*pb.UnmountSingleResponse, error) {
	if err := validateUnmountSingleRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := (bindMounterUnmounter{}).unmount(req.Mountpoint); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to unbind %s: %v", req.Mountpoint, err)
	}

	mountID, err := getMountIDForMountpoint(req.Mountpoint)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read target metadata: %v", err)
	}

	if mountID == "" {
		// No such mountpoint registered, assume it was already unmounted.
		return &pb.UnmountSingleResponse{}, nil
	}

	if _, isPending := s.pendingOps.LoadOrStore(mountID, true); isPending {
		return nil, status.Errorf(codes.Aborted, "operation for %s already in progress", req.Mountpoint)
	}
	defer s.pendingOps.Delete(mountID)

	lastBindMount, err := deleteBindMetadata(req, mountID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed delete bindmount metadata: %v", err)
	}

	if lastBindMount {
		// We need to clean up the CVMFS mount and the singlemount directory.

		if err := (cvmfsMounterUnmounter{}).unmount(fmtMountpointPath(mountID)); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to unmount CVMFS volume: %v", err)
		}

		if err := os.RemoveAll(fmtMountSingleBasePath(mountID)); err != nil {
			if !os.IsNotExist(err) {
				return nil, status.Errorf(codes.Internal, "failed to remove volume directory: %v", err)
			}
		}

		if err := deleteMountpointMetadata(req.Mountpoint); err != nil {
			return nil, status.Errorf(codes.Internal,
				"failed to unregister %s from target metadata: %v", req.Mountpoint, req)
		}
	}

	return &pb.UnmountSingleResponse{}, nil
}
