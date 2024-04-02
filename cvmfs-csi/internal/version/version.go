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

// Package version holds version metadata for the csi-cvmfsplugin.
package version

import (
	"fmt"
	"time"
)

// Values to be injected during build (ldflags).
var (
	buildTime = time.Now()
	version   = "unreleased"
	commit    string
	treestate string
	metadata  string
)

// Version returns the csi-cvmfsplugin version. It is expected this is defined
// as a semantic version number, or 'unreleased' for unreleased code.
func Version() string {
	return version
}

// Commit returns the git commit SHA for the code that the plugin was built from.
func Commit() string {
	return commit
}

// TreeState returns the git tree state. Can be "clean" or "dirty".
func TreeState() string {
	return treestate
}

// Metadata returns metadata passed during build.
func Metadata() string {
	return metadata
}

// BuildTime returns the date the package was built.
func BuildTime() time.Time {
	return buildTime
}

// FullVersion constructs a string with full version information.
func FullVersion() string {
	return fmt.Sprintf("%s (commit: %s (%s); build time: %s; metadata: %s)",
		Version(), Commit(), TreeState(), BuildTime(), Metadata())
}
