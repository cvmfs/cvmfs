# This file is part of the CernVM File System

k8s_create() {
  local version=${1:=latest}
  minikube start --kubernetes-version=$version
}

k8s_destroy() {
  minikube delete --all
}

k8s_kubectl() {
  minikube kubectl -- $@
}
