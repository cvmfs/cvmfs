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

package controller

import (
	"context"
	"errors"
	"fmt"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements csi.ControllerServer interface.
type Server struct {
	caps []*csi.ControllerServiceCapability
}

var (
	_ csi.ControllerServer = (*Server)(nil)
)

func New() *Server {
	enabledCaps := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
	}

	var caps []*csi.ControllerServiceCapability
	for _, c := range enabledCaps {
		caps = append(caps, &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: c,
				},
			},
		})
	}

	return &Server{
		caps: caps,
	}
}

func (srv *Server) CreateVolume(
	ctx context.Context,
	req *csi.CreateVolumeRequest,
) (*csi.CreateVolumeResponse, error) {
	if err := validateCreateVolumeRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      req.GetName(),
			VolumeContext: req.GetParameters(),
		},
	}, nil
}

func (srv *Server) DeleteVolume(
	ctx context.Context,
	req *csi.DeleteVolumeRequest,
) (*csi.DeleteVolumeResponse, error) {
	if err := validateDeleteVolumeRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (srv *Server) ControllerPublishVolume(
	context.Context,
	*csi.ControllerPublishVolumeRequest,
) (*csi.ControllerPublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (srv *Server) ControllerUnpublishVolume(
	context.Context,
	*csi.ControllerUnpublishVolumeRequest,
) (*csi.ControllerUnpublishVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (srv *Server) ValidateVolumeCapabilities(
	ctx context.Context,
	req *csi.ValidateVolumeCapabilitiesRequest,
) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if err := validateValidateVolumeCapabilitiesRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := validateVolumeParameters(req.GetParameters()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := validateVolumeParameters(req.GetVolumeContext()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := validateVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid volume capability: %v", err)
	}

	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csi.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
			VolumeContext:      req.GetVolumeContext(),
			Parameters:         req.GetParameters(),
		},
	}, nil
}

func (srv *Server) ListVolumes(
	context.Context,
	*csi.ListVolumesRequest,
) (*csi.ListVolumesResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (srv *Server) GetCapacity(
	context.Context,
	*csi.GetCapacityRequest,
) (*csi.GetCapacityResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (srv *Server) ControllerGetCapabilities(
	context.Context,
	*csi.ControllerGetCapabilitiesRequest,
) (*csi.ControllerGetCapabilitiesResponse, error) {
	return &csi.ControllerGetCapabilitiesResponse{
		Capabilities: srv.caps,
	}, nil
}

func (srv *Server) CreateSnapshot(
	context.Context,
	*csi.CreateSnapshotRequest,
) (*csi.CreateSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (srv *Server) DeleteSnapshot(
	context.Context,
	*csi.DeleteSnapshotRequest,
) (*csi.DeleteSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (srv *Server) ListSnapshots(
	context.Context,
	*csi.ListSnapshotsRequest,
) (*csi.ListSnapshotsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (srv *Server) ControllerExpandVolume(
	context.Context,
	*csi.ControllerExpandVolumeRequest,
) (*csi.ControllerExpandVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (srv *Server) ControllerGetVolume(
	context.Context,
	*csi.ControllerGetVolumeRequest,
) (*csi.ControllerGetVolumeResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func validateCreateVolumeRequest(req *csi.CreateVolumeRequest) error {
	if req.GetName() == "" {
		return errors.New("volume name cannot be empty")
	}

	reqCaps := req.GetVolumeCapabilities()
	if len(reqCaps) == 0 {
		return errors.New("volume capabilities cannot be empty")
	}

	if err := validateVolumeCapabilities(reqCaps); err != nil {
		return err
	}

	if req.GetVolumeContentSource() != nil {
		return errors.New("volume content source is not supported")
	}

	if req.GetAccessibilityRequirements() != nil {
		return errors.New("volume accessibility requirements are not supported")
	}

	if err := validateVolumeParameters(req.GetParameters()); err != nil {
		return err
	}

	return nil
}

func validateDeleteVolumeRequest(req *csi.DeleteVolumeRequest) error {
	if req.GetVolumeId() == "" {
		return errors.New("volume ID cannot be empty")
	}

	return nil
}

func validateValidateVolumeCapabilitiesRequest(req *csi.ValidateVolumeCapabilitiesRequest) error {
	if req.GetVolumeId() == "" {
		return errors.New("volume ID missing in request")
	}

	if len(req.GetVolumeCapabilities()) == 0 {
		return errors.New("volume capabilities cannot be nil or empty")
	}

	return nil
}

func validateVolumeParameters(volParams map[string]string) error {
	if len(volParams) > 0 {
		unsupportedVolumeParams := []string{"hash", "tag"}

		for _, volParam := range unsupportedVolumeParams {
			if _, ok := volParams[volParam]; ok {
				return fmt.Errorf("volume parameter %s is not supported", volParam)
			}
		}
	}

	return nil
}

func validateVolumeCapabilities(volCaps []*csi.VolumeCapability) error {
	for _, cap := range volCaps {
		if cap == nil {
			return errors.New("volume capability cannot be nil")
		}

		if cap.GetBlock() != nil {
			return errors.New("volume access type Block is unsupported")
		}

		if cap.GetMount() == nil {
			return errors.New("volume access type must by Mount")
		}

		accessMode := cap.GetAccessMode()

		if accessMode == nil {
			return errors.New("volume access mode cannot be nil")
		}

		if accessMode.GetMode() != csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY {
			return errors.New("volume access mode must be MULTI_NODE_READER_ONLY (ReadOnlyMany)")
		}
	}

	return nil
}
