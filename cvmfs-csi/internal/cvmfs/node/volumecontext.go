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
	"fmt"
)

type volumeContext struct {
	// Explicit repository to mount.
	repository string

	// CVMFS client configuration.
	clientConfig string

	// File for sourcing CVMFS client configuration.
	clientConfigFilepath string

	// A mount can be shared between multiple logical CSI volumes,
	// saving on resources on the node. If none is provided,
	// volume ID is used as a default value.
	sharedMountID string
}

func newVolumeContext(m map[string]string, volumeID string) (*volumeContext, error) {
	const (
		repositoryKey           = "repository"
		clientConfigKey         = "clientConfig"
		clientConfigFilepathKey = "clientConfigFilepath"
		sharedMountIDKey        = "sharedMountID"
	)

	if (m[clientConfigKey] != "" || m[clientConfigFilepathKey] != "") &&
		m[repositoryKey] == "" {
		return nil, fmt.Errorf("%s must be set too when specifying %s",
			repositoryKey, clientConfigKey)
	}

	if m[clientConfigKey] != "" && m[clientConfigFilepathKey] != "" {
		return nil, fmt.Errorf("only one of %s and %s may be defined",
			clientConfigKey, clientConfigFilepathKey)
	}

	if m[clientConfigKey] != "" && m[sharedMountIDKey] == "" {
		m[sharedMountIDKey] = volumeID
	}

	return &volumeContext{
		repository:           m[repositoryKey],
		clientConfig:         m[clientConfigKey],
		clientConfigFilepath: m[clientConfigFilepathKey],
		sharedMountID:        m[sharedMountIDKey],
	}, nil
}

func (volCtx *volumeContext) hasVolumeConfig() bool {
	return volCtx.clientConfig != "" || volCtx.clientConfigFilepath != ""
}
