# This file is part of the CernVM File System

k8s_create() {
  local version=${1:=latest}

  if [ `get_number_of_cpu_cores` -lt 2 ]; then
    echo "requires at least 2 CPUs (available: `get_number_of_cpu_cores`)"
    return 1
  fi
  minikube start --kubernetes-version=$version
  minikube kubectl -- get componentstatuses
  minikube kubectl -- get nodes
  eval $(minikube docker-env)
  docker ps
}

k8s_destroy() {
  minikube delete --all
}

k8s_kubectl() {
  minikube kubectl -- $@
}
