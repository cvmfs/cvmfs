#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server


# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


# only called by check_repository_compatibility()!
# @param creator  the creator version of the (incompatible) repository
# @param nokill   (optional) see check_repository_compatibility()
_repo_is_incompatible() {
  local creator=$1
  # if 'nokill' is set, be silent and just return 1
  if [ $# -gt 1 ]; then
    return 1
  fi

  echo "\
This repository uses the previous layout revision $(mangle_version_string $creator).
This version of CernVM-FS requires layout revision $(cvmfs_layout_revision), which is
incompatible to $(mangle_version_string $creator).

Please run \`cvmfs_server migrate\` to update your repository before proceeding."
  exit 1
}


# checks if the sourced server.conf is compatible with the running version of
# this script.
# Note: this assumes that server.conf was already sourced!
# @param nokill  (optional) if not set -> `exit 1` on incompatibility
check_repository_compatibility() {
  local name="$1"
  local nokill=$2
  local creator=$(repository_creator_version $name)

  if compare_versions $(cvmfs_layout_revision) -lt "$creator"; then
    if [ $# -gt 1 ]; then
      return 1 # nokill
    fi
    echo "This repository uses layout revision $creator which is newer than the
layout used by the currently installed CernVM-FS ($(cvmfs_layout_revision)).
Please upgrade CernVM-FS to manipulate this repository."
    exit 1
  fi

  # Migration History:
  #   2.1.6 -> 2.1.7
  #     -> repository format changed
  #
  #   2.1.7+ -> 2.1.15
  #     -> config files changed (adding client.local)
  #     -> adjustments in /etc/fstab
  #     -> additional statistics counters in file catalogs
  #
  #   2.1.15+ -> 2.1.20
  #     -> replica (i.e. stratum 1) with local upstream storage has
  #        additional apache config for wsgi
  #
  #   2.1.20+ -> 2.2.0-1 (2.2.0-0 was a server pre-release and needs migration)
  #     -> new (mandatory) parameters in client.conf (Stratum 0)
  #     -> adjustments in /etc/fstab
  #     -> CVMFS_AUTO_REPAIR_MOUNTPOINT=true becomes the enforced default
  #     -> Apache configuration updated
  #
  #   2.2.0-1+ -> 2.3.0-1
  #     -> new scratch directory layout (which also effects /etc/fstab)
  #
  #   2.3.0-1+ --> 2.3.3-1
  #     -> update global JSON info if repo was migrated from 2.1.20 or before
  #        (CVM-1159)
  #
  #   2.3.3-1+ --> 137
  #     -> use an arbitrary server layout revision to decouple the creator
  #        version from the software version (CVM-1065)
  #
  #   137 --> 138
  #     -> update apache configs on relevant stratum 1s for better geo api
  #        implementation (CVM-1349)
  #
  # Note: I tried to make this code as verbose as possible
  #
  if [ "$creator" = "2.1.6" ] && version_greater_or_equal "2.1.7"; then
    _repo_is_incompatible "$creator" $nokill
    return $?
  fi

  if [ "$creator" = "2.1.7"  ] || [ "$creator" = "2.1.8"  ] || \
     [ "$creator" = "2.1.9"  ] || [ "$creator" = "2.1.10" ] || \
     [ "$creator" = "2.1.11" ] || [ "$creator" = "2.1.12" ] || \
     [ "$creator" = "2.1.13" ] || [ "$creator" = "2.1.14" ];
  then
    if version_greater_or_equal "2.1.15"; then
      _repo_is_incompatible "$creator" $nokill
      return $?
    fi
  fi

  if [ "$creator" = "2.1.15" ] || [ "$creator" = "2.1.16" ] || \
     [ "$creator" = "2.1.17" ] || [ "$creator" = "2.1.18" ] || \
     [ "$creator" = "2.1.19" ];
  then
    if version_greater_or_equal "2.1.20" && \
       is_stratum1 $name                 && \
       is_local_upstream $CVMFS_UPSTREAM_STORAGE; then
      _repo_is_incompatible "$creator" $nokill
      return $?
    fi
  fi

  if [ "$creator" = "2.1.15"  ] || [ "$creator" = "2.1.16"  ] || \
     [ "$creator" = "2.1.17"  ] || [ "$creator" = "2.1.18"  ] || \
     [ "$creator" = "2.1.19"  ] || [ "$creator" = "2.1.20"  ] || \
     [ "$creator" = "2.2.0-0" ];
  then
    if version_greater_or_equal "2.2.0"; then
      _repo_is_incompatible "$creator" $nokill
      return $?
    fi
  fi

  if [ "$creator" = "2.2.0-1" ] || [ "$creator" = "2.2.1-1" ] || \
     [ "$creator" = "2.2.2-1" ] || [ "$creator" = "2.2.3-1" ] && \
     is_stratum0 $name; then
    if version_greater_or_equal "2.3.0"; then
      _repo_is_incompatible "$creator" $nokill
      return $?
    fi
  fi

  if [ "$creator" = "2.2.0-1" ] || [ "$creator" = "2.2.1-1" ] || \
     [ "$creator" = "2.2.2-1" ] || [ "$creator" = "2.2.3-1" ] || \
     [ "$creator" = "2.3.0-1" ] || [ "$creator" = "2.3.1-1" ] || \
     [ "$creator" = "2.3.2-1" ]; then
    if version_greater_or_equal "2.3.3"; then
      _repo_is_incompatible "$creator" $nokill
      return $?
    fi
  fi

  if [ "$creator" = "2.3.3-1" ] || [ "$creator" = "2.3.4-1" ] || \
     [ "$creator" = "2.3.5-1" ] || [ "$creator" = "2.3.6-1" ] || \
     [ "$creator" = "2.4.0-1" ]; then
    _repo_is_incompatible "$creator" $nokill
    return $?
  fi

  # At the moment any numeric version other than the current one is
  # only incompatible on stratum 1s with upstream storage and an
  # automatically managed apache config file, but that criteria will
  # undoubtedly change in future versions.  Perhaps we will want two
  # variables indicating the minimum compatible stratum 0 and minimum
  # compatible stratum 1 versions with apache config, and maybe a
  # third indicating the minimum compatible version in general.
  # Either that or we forget about being so selective and just force
  # everybody to do a migrate even if it isn't relevant in their config.

  local apache_conf="$(get_apache_conf_path)/$(get_apache_conf_filename $name)"
  if [ "$creator" -lt "$(cvmfs_layout_revision)" ] && \
       is_stratum1 $name && \
       is_local_upstream $CVMFS_UPSTREAM_STORAGE && \
       cvmfs_sys_file_is_regular $apache_conf; then
    _repo_is_incompatible "$creator" $nokill
    return $?
  fi

  return 0
}
