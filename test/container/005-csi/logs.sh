#!/bin/bash
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


CONTAINER_NAME=csi-cvmfsplugin
POD_NAME=$(minikube kubectl -- get pods -A -l app=$CONTAINER_NAME -o=name | head -n 1)
echo "POD name is $POD_NAME"

function get_pod_status() {
	echo -n $(minikube kubectl -- get --namespace=cvmfs $POD_NAME -o jsonpath="{.status.phase}")
}

while [[ "$(get_pod_status)" != "Running" ]]; do
	sleep 1
	echo "Waiting for $POD_NAME (status $(get_pod_status))"
done

minikube kubectl -- logs -f $POD_NAME -c $CONTAINER_NAME
