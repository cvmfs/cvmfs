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
	mount "k8s.io/mount-utils"
)

type (
	State int
)

const (
	StUnknown State = iota
	StNotMounted
	StMounted
	StCorrupted
)

var (
	dummyMounter = mount.New("")
)

func (s State) String() string {
	return [...]string{
		"UNKNOWN",
		"NOT_MOUNTED",
		"MOUNTED",
		"CORRUPTED",
	}[int(s)]
}

func GetState(p string) (State, error) {
	isNotMnt, err := mount.IsNotMountPoint(dummyMounter, p)
	if err != nil {
		if mount.IsCorruptedMnt(err) {
			return StCorrupted, nil
		}

		return StUnknown, err
	}

	if !isNotMnt {
		return StMounted, nil
	}

	return StNotMounted, nil
}
