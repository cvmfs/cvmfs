#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server list" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh
# - cvmfs_server_ssl.sh


cvmfs_server_list() {
  for repository in /etc/cvmfs/repositories.d/*; do
    if [ "x$repository" = "x/etc/cvmfs/repositories.d/*" ]; then
      return 0
    fi
    if cvmfs_sys_file_is_regular $repository ; then
      echo "Warning: unexpected file '$repository' in directory /etc/cvmfs/repositories.d/"
      continue
    fi
    local name=$(basename $repository)
    load_repo_config $name

    # figure out the schema version of the repository
    local version_info=""
    local creator_version=$(repository_creator_version $name)
    local version_info=""
    if ! check_repository_compatibility $name "nokill"; then
      version_info="(created with INCOMPATIBLE layout revision $(mangle_version_string $creator_version))"
    fi

    # collect additional information about aliased stratum1 repos
    local stratum1_info=""
    if is_stratum1 $name; then
      if [ "$CVMFS_REPOSITORY_NAME" != "$name" ]; then
        stratum1_info="-> $CVMFS_REPOSITORY_NAME"
      fi
    fi

    # find out if the repository is currently in a transaction
    local transaction_info=""
    if is_stratum0 $name && is_in_transaction $name; then
      transaction_info=" - in transaction"
    fi

    # check if the repository whitelist is accessible and expired
    local whitelist_info=""
    if is_stratum0 $name; then
      local retval=0
      check_expiry $name $CVMFS_STRATUM0 2>/dev/null || retval=$?
      if [ $retval -eq 100 ]; then
        whitelist_info=" - whitelist unreachable"
      elif [ $retval -ne 0 ]; then
        whitelist_info=" - whitelist expired"
      fi
    fi

    # check if the repository is healthy
    local health_info=""
    if ! health_check -q $name; then
      health_info=" - unhealthy"
    fi

    # print the checked out tag and branch
    local checkout_info=""
    if is_checked_out $name; then
      local tag_name=$(get_checked_out_tag $name)
      local branch_name=$(get_checked_out_branch $name)
      checkout_info=" [checked out tag '$tag_name' on branch '$branch_name']"
    fi

    # get the storage type of the repository
    local storage_type=""
    storage_type=$(get_upstream_type $CVMFS_UPSTREAM_STORAGE)

    # print out repository information list
    echo "$name ($CVMFS_REPOSITORY_TYPE / $storage_type$transaction_info$whitelist_info$health_info$checkout_info) $stratum1_info $version_info"
    CVMFS_CREATOR_VERSION=""
  done
}


