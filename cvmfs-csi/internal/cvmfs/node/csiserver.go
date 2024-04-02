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

package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/cvmfs-contrib/cvmfs-csi/internal/cvmfs/singlemount"
	singlemountv1 "github.com/cvmfs-contrib/cvmfs-csi/internal/cvmfs/singlemount/pb/v1"
	"github.com/cvmfs-contrib/cvmfs-csi/internal/mountutils"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements csi.NodeServer interface.
type Server struct {
	nodeID                    string
	singlemountRunnerEndpoint string
	caps                      []*csi.NodeServiceCapability
}

const (
	// autofs-managed CVMFS root mountpoint.
	cvmfsRoot = "/cvmfs"
)

var (
	_ csi.NodeServer = (*Server)(nil)
)

func New(nodeID, singlemountRunnerEndpoint string) *Server {
	enabledCaps := []csi.NodeServiceCapability_RPC_Type{
		csi.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME,
	}

	var caps []*csi.NodeServiceCapability
	for _, c := range enabledCaps {
		caps = append(caps, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: c,
				},
			},
		})
	}

	return &Server{
		nodeID:                    nodeID,
		singlemountRunnerEndpoint: singlemountRunnerEndpoint,
		caps:                      caps,
	}
}

func (srv *Server) NodeGetCapabilities(
	ctx context.Context,
	req *csi.NodeGetCapabilitiesRequest,
) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: srv.caps,
	}, nil
}

func (srv *Server) NodeGetInfo(
	ctx context.Context,
	req *csi.NodeGetInfoRequest,
) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: srv.nodeID,
	}, nil
}

func (srv *Server) NodePublishVolume(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
) (*csi.NodePublishVolumeResponse, error) {
	if err := validateNodePublishVolumeRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	targetPath := req.GetTargetPath()
	volCtx, err := newVolumeContext(req.GetVolumeContext(), req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			"failed to parse volume context: %v", err)
	}

	if err := os.MkdirAll(targetPath, 0700); err != nil {
		return nil, status.Errorf(codes.Internal,
			"failed to create mountpoint directory at %s: %v", targetPath, err)
	}

	mntState, err := mountutils.GetState(targetPath)
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"failed to probe mountpoint %s: %v", targetPath, err)
	}

	switch mntState {
	case mountutils.StNotMounted:
		if err := srv.doVolumePublish(ctx, req, volCtx); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to bind mount: %v", err)
		}
		fallthrough
	case mountutils.StMounted:
		return &csi.NodePublishVolumeResponse{}, nil
	default:
		return nil, status.Errorf(codes.Internal,
			"unexpected mountpoint state in %s: expected %s or %s, got %s",
			targetPath, mountutils.StNotMounted, mountutils.StMounted, mntState)
	}
}

func (srv *Server) doVolumePublish(
	ctx context.Context,
	req *csi.NodePublishVolumeRequest,
	volCtx *volumeContext,
) error {
	if volCtx.hasVolumeConfig() {
		// When client config is set, we assume the CVMFS repo was mounted
		// by singlemount-runner into stagingTargetPath.

		client, err := singlemount.NewClient(ctx, srv.singlemountRunnerEndpoint)
		if err != nil {
			return fmt.Errorf("failed to initialize client for singlemount-runner: %v", err)
		}
		defer client.Close()

		err = srv.ensureMountInStagingTargetPath(ctx, client, req.GetStagingTargetPath(), volCtx)
		if err != nil {
			return err
		}

		return bindMount(req.GetStagingTargetPath(), req.GetTargetPath())
	}

	// Otherwise we assume autofs-managed mounts.

	if volCtx.repository != "" {
		// Mount a single repository.
		return bindMount(path.Join(cvmfsRoot, volCtx.repository), req.TargetPath)
	}

	// Mount the whole autofs-CVMFS root.
	return slaveRecursiveBind(cvmfsRoot, req.GetTargetPath())
}

func (srv *Server) ensureMountInStagingTargetPath(
	ctx context.Context,
	cl singlemountv1.SingleClient,
	stagingPath string,
	volCtx *volumeContext,
) error {
	mntState, err := mountutils.GetState(stagingPath)
	if err != nil {
		return fmt.Errorf("failed to probe mountpoint %s: %v", stagingPath, err)
	}

	switch mntState {
	case mountutils.StCorrupted:
		// Detected mount corruption (i.e. cvmfs2 exited). Try to remount.
		_, err := cl.Unmount(ctx, &singlemountv1.UnmountSingleRequest{
			Mountpoint: stagingPath,
		})
		if err != nil {
			return fmt.Errorf("failed to unmount %s during mount recovery: %v", stagingPath, err)
		}
		fallthrough
	case mountutils.StNotMounted:
		_, err := cl.Mount(ctx, mountSingleRequestFromVolCtx(stagingPath, volCtx))
		if err != nil {
			return fmt.Errorf("failed to bind mount: %v", err)
		}
		fallthrough
	case mountutils.StMounted:
		return nil
	default:
		return fmt.Errorf("unexpected mountpoint state in %s: expected %s or %s, got %s",
			stagingPath, mountutils.StNotMounted, mountutils.StMounted, mntState)
	}
}

func (srv *Server) NodeUnpublishVolume(
	ctx context.Context,
	req *csi.NodeUnpublishVolumeRequest,
) (*csi.NodeUnpublishVolumeResponse, error) {
	if err := validateNodeUnpublishVolumeRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	targetPath := req.GetTargetPath()

	mntState, err := mountutils.GetState(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			// This can happen e.g. when a node was rebooted.
			// The volume is no longer mounted, so return success.
			return &csi.NodeUnpublishVolumeResponse{}, nil
		}

		return nil, status.Errorf(codes.Internal,
			"failed to probe for mountpoint %s: %v", targetPath, err)
	}

	if mntState != mountutils.StNotMounted {
		if err := recursiveUnmount(targetPath); err != nil {
			return nil, status.Errorf(codes.Internal,
				"failed to unmount %s: %v", targetPath, err)
		}
	}

	err = os.Remove(targetPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

func (srv *Server) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest,
) (*csi.NodeStageVolumeResponse, error) {
	if err := validateNodeStageVolumeRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	volCtx, err := newVolumeContext(req.GetVolumeContext(), req.GetVolumeId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument,
			"failed to parse volume context: %v", err)
	}

	// When client config is set, we cannot use automounts and need
	// to delegate the mount to its own cvmfs2 call instead. We use
	// the singlemount-runner for that.

	if !volCtx.hasVolumeConfig() {
		// No client config in volume context means we can proceed
		// to bindmounting the autofs-CVMFS root.
		return &csi.NodeStageVolumeResponse{}, nil
	}

	err = srv.doSingleMount(ctx, mountSingleRequestFromVolCtx(req.StagingTargetPath, volCtx))
	if err != nil {
		return nil, err
	}

	return &csi.NodeStageVolumeResponse{}, nil
}

func (srv *Server) doSingleMount(ctx context.Context, req *singlemountv1.MountSingleRequest) error {
	client, err := singlemount.NewClient(ctx, srv.singlemountRunnerEndpoint)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to initialize client for singlemount-runner: %v", err)
	}
	defer client.Close()

	_, err = client.Mount(ctx, req)

	return err
}

func (srv *Server) doSingleUnmount(ctx context.Context, req *singlemountv1.UnmountSingleRequest) error {
	client, err := singlemount.NewClient(ctx, srv.singlemountRunnerEndpoint)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to initialize client for singlemount-runner: %v", err)
	}
	defer client.Close()

	_, err = client.Unmount(ctx, req)

	return err
}

func (srv *Server) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest,
) (*csi.NodeUnstageVolumeResponse, error) {
	if err := validateNodeUnstageVolumeRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// Only volumes that have client config defined in their volume context
	// have been mounted in NodeStageVolume. We have no way of knowing if this
	// is such a volume (the request doesn't contain vol ctx). Try to unmount it.

	err := srv.doSingleUnmount(ctx, &singlemountv1.UnmountSingleRequest{
		Mountpoint: req.StagingTargetPath,
	})
	if err != nil {
		return nil, err
	}

	return &csi.NodeUnstageVolumeResponse{}, nil
}

func (srv *Server) NodeGetVolumeStats(
	ctx context.Context,
	req *csi.NodeGetVolumeStatsRequest,
) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (srv *Server) NodeExpandVolume(
	ctx context.Context,
	req *csi.NodeExpandVolumeRequest,
) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func validateNodePublishVolumeRequest(req *csi.NodePublishVolumeRequest) error {
	if req.GetVolumeId() == "" {
		return errors.New("volume ID missing in request")
	}

	if req.GetVolumeCapability() == nil {
		return errors.New("volume capability missing in request")
	}

	if req.GetVolumeCapability().GetBlock() != nil {
		return errors.New("volume access type Block is unsupported")
	}

	if req.GetVolumeCapability().GetMount() == nil {
		return errors.New("volume access type must by Mount")
	}

	if req.GetTargetPath() == "" {
		return errors.New("volume target path missing in request")
	}

	// We're not checking for staging target path, as older versions
	// of the driver didn't support STAGE_UNSTAGE_VOLUME capability.

	if req.GetVolumeCapability().GetAccessMode().GetMode() !=
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		return fmt.Errorf("volume access mode must be ReadOnlyMany")
	}

	if volCtx := req.GetVolumeContext(); len(volCtx) > 0 {
		unsupportedVolumeParams := []string{"hash", "tag"}

		for _, volParam := range unsupportedVolumeParams {
			if _, ok := volCtx[volParam]; ok {
				return fmt.Errorf("volume parameter %s is not supported, please use clientConfig instead", volParam)
			}
		}
	}

	return nil
}

func validateNodeUnpublishVolumeRequest(req *csi.NodeUnpublishVolumeRequest) error {
	if req.GetVolumeId() == "" {
		return errors.New("volume ID missing in request")
	}

	if req.GetTargetPath() == "" {
		return errors.New("target path missing in request")
	}

	return nil
}

func validateNodeStageVolumeRequest(req *csi.NodeStageVolumeRequest) error {
	if req.GetVolumeId() == "" {
		return errors.New("volume ID missing in request")
	}

	if req.GetVolumeCapability() == nil {
		return errors.New("volume capability missing in request")
	}

	if req.GetVolumeCapability().GetBlock() != nil {
		return errors.New("volume access type Block is unsupported")
	}

	if req.GetVolumeCapability().GetMount() == nil {
		return errors.New("volume access type must by Mount")
	}

	if req.GetStagingTargetPath() == "" {
		return errors.New("volume staging target path missing in request")
	}

	if req.GetVolumeCapability().GetAccessMode().GetMode() !=
		csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
		return fmt.Errorf("volume access mode must be ReadOnlyMany")
	}

	if volCtx := req.GetVolumeContext(); len(volCtx) > 0 {
		unsupportedVolumeParams := []string{"hash", "tag"}

		for _, volParam := range unsupportedVolumeParams {
			if _, ok := volCtx[volParam]; ok {
				return fmt.Errorf("volume parameter %s is not supported", volParam)
			}
		}
	}

	return nil
}

func validateNodeUnstageVolumeRequest(req *csi.NodeUnstageVolumeRequest) error {
	if req.GetVolumeId() == "" {
		return errors.New("volume ID missing in request")
	}

	if req.GetStagingTargetPath() == "" {
		return errors.New("staging target path missing in request")
	}

	return nil
}

func mountSingleRequestFromVolCtx(mountPath string, volCtx *volumeContext) *singlemountv1.MountSingleRequest {
	return &singlemountv1.MountSingleRequest{
		MountId:        volCtx.sharedMountID,
		Config:         volCtx.clientConfig,
		ConfigFilepath: volCtx.clientConfigFilepath,
		Repository:     volCtx.repository,
		Target:         mountPath,
	}
}
