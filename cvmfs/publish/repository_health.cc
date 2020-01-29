/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <cstdio>

#include "hash.h"
#include "manifest.h"
#include "publish/except.h"
#include "publish/repository_util.h"
#include "util/posix.h"

namespace publish {

int Publisher::CheckHealth(Publisher::ERepairMode mode, bool is_quiet) {
  int result = kFailOk;
  if (!IsMountPoint(settings_.transaction().spool_area().readonly_mnt())) {
    result |= kFailRdOnlyBroken;
  } else {
    const std::string root_hash_xattr = "user.root_hash";
    std::string root_hash_str;
    bool retval = platform_getxattr(
      settings_.transaction().spool_area().readonly_mnt(), root_hash_xattr,
      &root_hash_str);
    if (!retval)
      throw EPublish("cannot retrieve root hash from read-only mount point");
    shash::Any root_hash = shash::MkFromHexPtr(shash::HexPtr(root_hash_str),
                                               shash::kSuffixCatalog);
    if (root_hash != manifest()->catalog_hash()) {
      CheckoutMarker *marker = CheckoutMarker::CreateFrom(
        settings_.transaction().spool_area().checkout_marker());
      if (marker != NULL) {
        if (marker->hash() != root_hash)
          result |= kFailRdOnlyWrongRevision;
        delete marker;
      } else {
        result |= kFailRdOnlyOutdated;
      }
    }
  }


  return result;

  /* get_checked_out_tag() {
  local name=$1
  load_repo_config $name
  cat /var/spool/cvmfs/${name}/checkout | cut -d" " -f1
}


# parses the checkout file
#
# @param name  the repository name to be checked
# @return      0 if checked out
get_checked_out_hash() {
  local name=$1
  load_repo_config $name
  cat /var/spool/cvmfs/${name}/checkout | cut -d" " -f2
}


# parses the checkout file
#
# @param name  the repository name to be checked
# @return      0 if checked out
get_checked_out_branch() {
  local name=$1
  load_repo_config $name
  cat /var/spool/cvmfs/${name}/checkout | cut -d" " -f3
}
*/


  // [ -f /var/spool/cvmfs/${name}/checkout ]

  /*assert(!mount_point.empty());
    const std::string root_hash_xattr = "user.root_hash";
    std::string root_hash;
    const bool success =
        platform_getxattr(mount_point, root_hash_xattr, &root_hash);
    if (!success) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "failed to retrieve extended attribute "
               " '%s' from '%s' (errno: %d)",
               root_hash_xattr.c_str(), mount_point.c_str(), errno);
      return 1;
    }
    LogCvmfs(kLogCvmfs, kLogStdout, "%s%s",
             (human_readable) ? "Mounted Root Hash:               " : "",
             root_hash.c_str());*/

  return kFailOk;
}

}  // namespace publish
