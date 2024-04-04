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

package env

import (
	"fmt"
	"os"
	"strconv"
)

const (
	// Boolean value. By default, when exiting, automount daemon is sent
	// SIGKILL signal forcing it to skip its clean up procedure, leaving
	// the autofs mount behind. This is needed for the daemon to be able
	// to reconnect to the autofs mount when the nodeplugin Pod is being
	// restarted.
	//
	// Setting the value of this environment value to TRUE overrides this,
	// and allows the daemon to do the clean up. This is useful when
	// e.g. uninstalling the eosxd-csi driver.
	AutofsTryCleanAtExit = "AUTOFS_TRY_CLEAN_AT_EXIT"
)

func GetAutofsTryCleanAtExit() bool {
	strVal := os.Getenv(AutofsTryCleanAtExit)
	boolVal, _ := strconv.ParseBool(strVal)

	return boolVal
}

func StringAutofsTryCleanAtExit() string {
	return fmt.Sprintf("%s=\"%v\"", AutofsTryCleanAtExit, GetAutofsTryCleanAtExit())
}
