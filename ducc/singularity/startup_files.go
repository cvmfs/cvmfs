// Copyright (c) 2018-2019, Sylabs Inc. All rights reserved.
// This software is licensed under a 3-clause BSD license.
/* Original license
Copyright (c) 2015-2017, Gregory M. Kurtzer. All rights reserved.
Copyright (c) 2016-2017, The Regents of the University of California. All right reserved.
Copyright (c) 2017, SingularityWare, LLC. All rights reserved.
Copyright (c) 2018-2019, Sylabs, Inc. All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
1. Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.
3. Neither the name of the copyright holder nor the names of its
   contributors may be used to endorse or promote products derived from this
   software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
*/

package singularity

import (
	"fmt"
	"os"
	"strings"

	"github.com/docker/docker/image"
)

// The two functions below come from:
// https://raw.githubusercontent.com/hpcng/singularity/0eca74e26dc6bc26e86ce203f704d46e22c68e55/internal/pkg/util/shell/escape.go

// ArgsQuoted concatenates a slice of string shell args, quoting each item
func ArgsQuoted(a []string) (quoted string) {
	for _, val := range a {
		quoted = quoted + `"` + Escape(val) + `" `
	}
	quoted = strings.TrimRight(quoted, " ")
	return
}

// Escape performs escaping of shell quotes, backticks and $ characters
func Escape(s string) string {
	escaped := strings.Replace(s, `\`, `\\`, -1)
	escaped = strings.Replace(escaped, `"`, `\"`, -1)
	escaped = strings.Replace(escaped, "`", "\\`", -1)
	escaped = strings.Replace(escaped, `$`, `\$`, -1)
	return escaped
}

// This other functions are adaptations of:
// https://raw.githubusercontent.com/hpcng/singularity/0eca74e26dc6bc26e86ce203f704d46e22c68e55/internal/pkg/build/sources/conveyorPacker_oci.go

func InsertRunScript(rootPath string, ociImage image.Image) (err error) {
	imgConfig := ociImage.Config
	f, err := os.Create(rootPath + "/.singularity.d/runscript")
	if err != nil {
		return
	}

	defer f.Close()

	_, err = f.WriteString("#!/bin/sh\n")
	if err != nil {
		return
	}

	if len(imgConfig.Entrypoint) > 0 {
		_, err = f.WriteString("OCI_ENTRYPOINT='" + ArgsQuoted(imgConfig.Entrypoint) + "'\n")
		if err != nil {
			return
		}
	} else {
		_, err = f.WriteString("OCI_ENTRYPOINT=''\n")
		if err != nil {
			return
		}
	}

	if len(imgConfig.Cmd) > 0 {
		_, err = f.WriteString("OCI_CMD='" + ArgsQuoted(imgConfig.Cmd) + "'\n")
		if err != nil {
			return
		}
	} else {
		_, err = f.WriteString("OCI_CMD=''\n")
		if err != nil {
			return
		}
	}

	_, err = f.WriteString(`CMDLINE_ARGS=""
# prepare command line arguments for evaluation
for arg in "$@"; do
    CMDLINE_ARGS="${CMDLINE_ARGS} \"$arg\""
done
# ENTRYPOINT only - run entrypoint plus args
if [ -z "$OCI_CMD" ] && [ -n "$OCI_ENTRYPOINT" ]; then
    if [ $# -gt 0 ]; then
        SINGULARITY_OCI_RUN="${OCI_ENTRYPOINT} ${CMDLINE_ARGS}"
    else
        SINGULARITY_OCI_RUN="${OCI_ENTRYPOINT}"
    fi
fi
# CMD only - run CMD or override with args
if [ -n "$OCI_CMD" ] && [ -z "$OCI_ENTRYPOINT" ]; then
    if [ $# -gt 0 ]; then
        SINGULARITY_OCI_RUN="${CMDLINE_ARGS}"
    else
        SINGULARITY_OCI_RUN="${OCI_CMD}"
    fi
fi
# ENTRYPOINT and CMD - run ENTRYPOINT with CMD as default args
# override with user provided args
if [ $# -gt 0 ]; then
    SINGULARITY_OCI_RUN="${OCI_ENTRYPOINT} ${CMDLINE_ARGS}"
else
    SINGULARITY_OCI_RUN="${OCI_ENTRYPOINT} ${OCI_CMD}"
fi
# Evaluate shell expressions first and set arguments accordingly,
# then execute final command as first container process
eval "set ${SINGULARITY_OCI_RUN}"
exec "$@"
`)
	if err != nil {
		return
	}

	f.Sync()

	err = os.Chmod(rootPath+"/.singularity.d/runscript", 0755)
	if err != nil {
		return
	}

	return nil
}

func InsertEnv(rootPath string, ociImage image.Image) (err error) {
	imgConfig := ociImage.Config
	f, err := os.Create(rootPath + "/.singularity.d/env/10-docker2singularity.sh")
	if err != nil {
		return
	}

	defer f.Close()

	_, err = f.WriteString("#!/bin/sh\n")
	if err != nil {
		return
	}

	for _, element := range imgConfig.Env {
		export := ""
		envParts := strings.SplitN(element, "=", 2)
		if len(envParts) == 1 {
			export = fmt.Sprintf("export %s=\"${%s:-}\"\n", envParts[0], envParts[0])
		} else {
			if envParts[0] == "PATH" {
				export = fmt.Sprintf("export %s=%q\n", envParts[0], Escape(envParts[1]))
			} else {
				export = fmt.Sprintf("export %s=\"${%s:-%q}\"\n", envParts[0], envParts[0], Escape(envParts[1]))
			}
		}
		_, err = f.WriteString(export)
		if err != nil {
			return
		}
	}

	f.Sync()

	err = os.Chmod(rootPath+"/.singularity.d/env/10-docker2singularity.sh", 0755)
	if err != nil {
		return
	}

	return nil
}
