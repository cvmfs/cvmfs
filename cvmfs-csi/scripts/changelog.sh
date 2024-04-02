#!/usr/bin/env bash
#
# Copyright CERN.
#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Based on the Helm script file:
# github.com/helm/helm/scripts/coverage.sh

set -euo pipefail

if ! [ -x "$(command -v github_changelog_generator)" ]; then
  gem install --user-install github_changelog_generator
  export PATH=$PATH:$(find ~/.gem -name bin | tail -1)
fi

github_changelog_generator -u cvmfs-contrib -p cvmfs-csi --token $GITHUB_TOKEN --since-tag $(git describe --abbrev=0 --tags `git rev-list --tags --skip=1 --max-count=1`)
