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
#include "upload.h"
#include "upload_spooler_definition.h"
#include "util/pointer.h"
#include "util/posix.h"


namespace publish {

int Publisher::CheckHealth(Publisher::ERepairMode repair_mode, bool is_quiet) {
  const std::string rdonly_mnt =
    settings_.transaction().spool_area().readonly_mnt();
  const std::string union_mnt =
    settings_.transaction().spool_area().union_mnt();
  const std::string transaction_lock =
    settings_.transaction().spool_area().transaction_lock();
  const std::string publishing_lock =
    settings_.transaction().spool_area().publishing_lock();
  const std::string fqrn = settings_.fqrn();

  int result = kFailOk;

  if (!IsMountPoint(rdonly_mnt)) {
    result |= kFailRdOnlyBroken;
  } else {
    const std::string root_hash_xattr = "user.root_hash";
    std::string root_hash_str;
    bool retval = platform_getxattr(rdonly_mnt, root_hash_xattr,
                                    &root_hash_str);
    if (!retval)
      throw EPublish("cannot retrieve root hash from read-only mount point");
    shash::Any root_hash = shash::MkFromHexPtr(shash::HexPtr(root_hash_str),
                                               shash::kSuffixCatalog);

    UniquePtr<CheckoutMarker> marker(CheckoutMarker::CreateFrom(
      settings_.transaction().spool_area().checkout_marker()));
    if (marker.IsValid()) {
      if (marker->hash() != root_hash)
        result |= kFailRdOnlyWrongRevision;
    }

    if ((root_hash != manifest()->catalog_hash()) && !marker.IsValid()) {
      // In a gateway setup, it is expected that other publishers changed
      // the repository in the meantime
      if (spooler()->GetDriverType() != upload::SpoolerDefinition::Gateway)
        result |= kFailRdOnlyOutdated;
    }
  }

  bool union_should_be_rw = false;
  bool union_should_be_ro = false;
  // The process that opens the transaction does not stay alive for the life
  // time of the transaction
  bool is_in_transaction =
    ServerLockFile::IsLocked(transaction_lock, true /* ignore_stale */);
  if (!IsMountPoint(union_mnt)) {
    result |= kFailUnionBroken;
    if (is_in_transaction) {
      union_should_be_rw = true;
    } else {
      union_should_be_ro = true;
    }
  } else {
    FileSystemInfo fs_info = GetFileSystemInfo(union_mnt);
    if (is_in_transaction) {
      if (fs_info.is_rdonly) {
        union_should_be_rw = true;
        result |= kFailUnionLocked;
      }
    } else {
      if (!fs_info.is_rdonly) {
        union_should_be_ro = true;
        result |= kFailUnionWritable;
      }
    }
  }

  if (result == kFailOk)
    return result;

  // Report & Repair

  int logFlags = kLogStderr;
  if (is_quiet)
    logFlags |= kLogNone;
  if (result & kFailRdOnlyBroken) {
    LogCvmfs(kLogCvmfs, logFlags, "%s is not mounted properly",
             rdonly_mnt.c_str());
  }
  if (result & kFailRdOnlyOutdated) {
    LogCvmfs(kLogCvmfs, logFlags,
             "%s is not based on the newest published revision", fqrn.c_str());
  }
  if (result & kFailRdOnlyWrongRevision) {
    LogCvmfs(kLogCvmfs, logFlags,
             "%s is not based on the checked out revision", fqrn.c_str());
  }
  if (result & kFailUnionBroken) {
    LogCvmfs(kLogCvmfs, logFlags, "%s is not mounted properly",
             union_mnt.c_str());
  }
  if (union_should_be_ro) {
    LogCvmfs(kLogCvmfs, logFlags,
             "%s is not in a transaction but %s is mounted read/write",
             fqrn.c_str(), union_mnt.c_str());
  }
  if (union_should_be_rw) {
    LogCvmfs(kLogCvmfs, logFlags,
             "%s is in a transaction but %s is not mounted read/write",
             fqrn.c_str(), union_mnt.c_str());
  }

  // Check whether we can repair

  bool is_publishing =
    ServerLockFile::IsLocked(publishing_lock, false /* ignore_stale */);
  switch (repair_mode) {
    case kRepairNever:
      return result;
    case kRepairAlways:
      break;
    case kRepairSafe:
      if (is_publishing) {
        LogCvmfs(kLogCvmfs, logFlags,
          "WARNING: The repository %s is currently publishing and should not\n"
          "be touched. If you are absolutely sure, that this is _not_ the "
          "case,\nplease run the following command and retry:\n\n"
          "    rm -fR %s\n",
          fqrn.c_str(), publishing_lock.c_str());
        return result;
      }

      if (is_in_transaction) {
        LogCvmfs(kLogCvmfs, logFlags,
          "Repository %s is in a transaction and cannot be repaired.\n"
          "--> Run `cvmfs_server abort $name` to revert and repair.",
          fqrn.c_str());
        return result;
      }

      break;
    default:
      abort();
  }

  LogCvmfs(kLogCvmfs, kLogSyslog, "(%s) attempting mountpoint repair (%d)",
           fqrn.c_str());
  return result;

  // Repair


}

}  // namespace publish
