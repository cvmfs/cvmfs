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
	"encoding/json"
	"os"
)

func ifErr(err *error, do func()) {
	if *err != nil {
		do()
	}
}

func fromJSONFile[V any](filepath string, defaultValue V) (V, error) {
	jsonData, err := os.ReadFile(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return defaultValue, nil
		}

		return defaultValue, err
	}

	var val V
	if err = json.Unmarshal(jsonData, &val); err != nil {
		return defaultValue, err
	}

	return val, err
}

func toJSONFile(filepath string, val any) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath, jsonData, 0644)
}
