#!/bin/sh

transaction_before_hook() {
  local repository_name=$1

  echo "Start transaction for $repository_name"
}


transaction_after_hook() {
  local repository_name=$1
}


abort_before_hook() {
  local repository_name=$1
}


abort_after_hook() {
  local repository_name=$1

  echo "Aborted transaction for $repository_name"
}


publish_before_hook() {
  local repository_name=$1
}


publish_after_hook() {
  local repository_name=$1

  echo "Published changeset for $repository_name"
}
