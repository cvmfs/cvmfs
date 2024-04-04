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

package identity

import (
	"context"

	"github.com/cvmfs/csi/internal/version"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// Server implements csi.IdentityServer interface.
type Server struct {
	driverName string
	caps       []*csi.PluginCapability
}

var _ csi.IdentityServer = (*Server)(nil)

func New(driverName string, hasControllerService bool) *Server {
	supportedRpcs := []csi.PluginCapability_Service_Type{
		csi.PluginCapability_Service_UNKNOWN,
	}

	if hasControllerService {
		supportedRpcs = append(supportedRpcs, csi.PluginCapability_Service_CONTROLLER_SERVICE)
	}

	var caps []*csi.PluginCapability
	for _, c := range supportedRpcs {
		caps = append(caps, &csi.PluginCapability{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: c,
				},
			},
		})
	}

	return &Server{
		driverName: driverName,
		caps:       caps,
	}
}

func (srv *Server) GetPluginInfo(
	ctx context.Context,
	req *csi.GetPluginInfoRequest,
) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          srv.driverName,
		VendorVersion: version.Version(),
	}, nil
}

func (srv *Server) GetPluginCapabilities(
	ctx context.Context,
	req *csi.GetPluginCapabilitiesRequest,
) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: srv.caps,
	}, nil
}

func (srv *Server) Probe(
	ctx context.Context,
	req *csi.ProbeRequest,
) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}
