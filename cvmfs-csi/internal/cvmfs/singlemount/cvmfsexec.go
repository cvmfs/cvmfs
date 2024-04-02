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
	"container/ring"
	"fmt"
	goexec "os/exec"
	"strings"

	"github.com/cvmfs-contrib/cvmfs-csi/internal/exec"
	"github.com/cvmfs-contrib/cvmfs-csi/internal/log"
)

var cvmfsErrMessages = []string{
	"all proxies failed, trying host fail-over)",
	"already mounted)",
	"bad certificate, failed to verify repository manifest)",
	"bad data received)",
	"bad signature, failed to verify repository manifest)",
	"bad whitelist)",
	"cache directory/plugin problem)",
	"cannot run FUSE event loop)",
	"catalog root path mismatch)",
	"certificate blacklisted)",
	"certificate is not whitelisted)",
	"certificate not on whitelist)",
	"corrupted data received)",
	"decompression failed)",
	"DNS query timeout)",
	"double mount)",
	"empty whitelist)",
	"empty whitelist (pkcs7))",
	"expired whitelist)",
	"failed to download)",
	"failed to download whitelist)",
	"failed to download whitelist (pkcs7))",
	"failed to load shared library)",
	"failed to mount)",
	"failed to resolve host address)",
	"failed to resolve proxy address)",
	"failed to verify CA chain)",
	"file catalog failure)",
	"history init failure)",
	"host connection problem)",
	"host data transfer cut short)",
	"host returned HTTP error)",
	"host serving data too slowly)",
	"illegal options)",
	"incompatible library version)",
	"incomplete manifest)",
	"internal error, not yet resolved)",
	"invalid Base64 input)",
	"invalid certificate)",
	"invalid host name to resolve)",
	"invalid resolver addresses)",
	"invalid whitelist (pkcs7))",
	"invalid whitelist signature)",
	"invalid whitelist signer (pkcs7))",
	"letter expired)",
	"letter malformed)",
	"local I/O failure)",
	"maintenance mode)",
	"malformed DNS request)",
	"malformed URL)",
	"malformed whitelist)",
	"malformed whitelist (pkcs7))",
	"manifest name doesn't match)",
	"manifest signature is invalid)",
	"network failure)",
	"NFS maps init failure)",
	"no IP address for host)",
	"object not found)",
	"outdated manifest)",
	"peering problem)",
	"permission denied)",
	"proxy auto-discovery failed)",
	"proxy connection problem)",
	"proxy data transfer cut short)",
	"proxy returned HTTP error)",
	"proxy serving data too slowly)",
	"quota init failure)",
	"repository name mismatch)",
	"repository name mismatch on whitelist)",
	"request canceled)",
	"resource too big to download)",
	"revision blacklisted)",
	"signature verification failed)",
	"signature verification failure)",
	"state restore failure)",
	"state saving failure)",
	"S3: failed to resolve host address)",
	"S3: forbidden)",
	"S3: host connection problem)",
	"S3: local I/O failure)",
	"S3: malformed URL (bad request))",
	"S3: not found)",
	"S3: service not available)",
	"S3: too many requests, service asks for backoff and retry)",
	"S3: unknown service error, perhaps wrong authentication protocol)",
	"talk socket failure)",
	"unable to init loader talk socket)",
	"unknown host name)",
	"unknown name resolving error)",
	"unknown network error)",
	"Unsupported URL in protocol)",
	"watchdog failure)",
	"workspace already locked)",
}

func runCvmfs2AndTryCaptureErr(arg ...string) error {
	// Holds up to 10 last lines of cvmfs2 output.
	// Let's hope the final error message will be
	// somewhere in there...
	logRing := ring.New(10)

	err := exec.RunAndDoCombined(
		goexec.Command("cvmfs2", arg...),
		func(execID uint64, line string) {
			if line == "" {
				return
			}

			log.Infof(exec.FmtLogMsg(execID, line))

			logRing.Value = line
			logRing = logRing.Next()
		},
	)

	if err == nil {
		return nil
	}

	// cvmfs2 failed. Search the list of log messages and try
	// to find the final error message.

	var errMsgs []string
	logRing.Do(func(line any) {
		lineStr, ok := line.(string)
		if !ok {
			return
		}

		for _, errMsgSuffix := range cvmfsErrMessages {
			if strings.Contains(lineStr, errMsgSuffix) {
				errMsgs = append(errMsgs, lineStr)
			}
		}
	})

	if len(errMsgs) != 0 {
		return fmt.Errorf("failed to mount repository: %v", errMsgs)
	}

	return fmt.Errorf("failed to mount repository, please see cvmfs-csi logs for details (%v)", err)
}
