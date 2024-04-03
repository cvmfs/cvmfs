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

package mountutils

import (
	"bytes"
	goexec "os/exec"

	"github.com/cvmfs/cvmfs-csi/internal/exec"
)

func Unmount(mountpoint string, extraArgs ...string) error {
	out, err := exec.CombinedOutput(goexec.Command("umount", append(extraArgs, mountpoint)...))
	if err != nil {
		// There are no well-defined exit codes for cases of "not mounted"
		// and "doesn't exist". We need to check the output.
		if bytes.HasSuffix(out, []byte(": not mounted")) ||
			bytes.Contains(out, []byte("No such file or directory")) {
			return nil
		}
	}

	return err
}
