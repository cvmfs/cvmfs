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
#include "util/string.h"


namespace publish {

void Publisher::ManagedNode::Open() {
  AlterMountpoint(kAlterUnionOpen, kLogSyslog);
}


void Publisher::ManagedNode::Lock() {
  AlterMountpoint(kAlterUnionLock, kLogSyslog);
}


void Publisher::ManagedNode::Unmount() {
  try {
    AlterMountpoint(kAlterUnionUnmount, kLogSyslog);
  } catch (const EPublish &e) {
    AlterMountpoint(kAlterUnionLazyUnmount, kLogSyslog);
    AlterMountpoint(kAlterRdOnlyKillUnmount, kLogSyslog);
    AlterMountpoint(kAlterRdOnlyLazyUnmount, kLogSyslog);
    return;
  }

  try {
    AlterMountpoint(kAlterRdOnlyUnmount, kLogSyslog);
  } catch (const EPublish &e) {
    AlterMountpoint(kAlterRdOnlyKillUnmount, kLogSyslog);
    AlterMountpoint(kAlterRdOnlyLazyUnmount, kLogSyslog);
  }
}

void Publisher::ManagedNode::Mount() {
  AlterMountpoint(kAlterRdOnlyMount, kLogSyslog);
  AlterMountpoint(kAlterUnionMount, kLogSyslog);
}

void Publisher::ManagedNode::ClearScratch() {
  const std::string scratch_dir =
    publisher_->settings_.transaction().spool_area().scratch_dir();
  const std::string scratch_wastebin =
    publisher_->settings_.transaction().spool_area().scratch_wastebin();
  const std::string tmp_dir =
    publisher_->settings_.transaction().spool_area().tmp_dir();

  std::string waste_dir = CreateTempDir(scratch_wastebin + "/waste");
  if (waste_dir.empty()) throw EPublish("cannot create wastebin directory");
  int rvi = rename(scratch_dir.c_str(), (waste_dir + "/delete-me").c_str());
  if (rvi != 0) throw EPublish("cannot move scratch directory to wastebin");

  publisher_->CreateDirectoryAsOwner(scratch_dir, kPrivateDirMode);

  AlterMountpoint(kAlterScratchWipe, kLogSyslog);

  std::vector<mode_t> modes;
  std::vector<std::string> names;
  ListDirectory(tmp_dir, &names, &modes);
  for (unsigned i = 0; i < names.size(); ++i) {
    if (HasPrefix(names[i], "receiver.", false /* ignore_case */))
      continue;

    unlink((tmp_dir + "/" + names[i]).c_str());
  }
}


int Publisher::ManagedNode::Check(bool is_quiet) {
  const std::string rdonly_mnt =
    publisher_->settings_.transaction().spool_area().readonly_mnt();
  const std::string union_mnt =
    publisher_->settings_.transaction().spool_area().union_mnt();
  const std::string publishing_lock =
    publisher_->settings_.transaction().spool_area().publishing_lock();
  const std::string fqrn = publisher_->settings_.fqrn();
  EUnionMountRepairMode repair_mode =
    publisher_->settings_.transaction().spool_area().repair_mode();

  int result = kFailOk;

  shash::Any expected_hash = publisher_->manifest()->catalog_hash();
  UniquePtr<CheckoutMarker> marker(CheckoutMarker::CreateFrom(
    publisher_->settings_.transaction().spool_area().checkout_marker()));
  if (marker.IsValid())
    expected_hash = marker->hash();

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

    if (expected_hash != root_hash) {
      if (marker.IsValid()) {
        result |= kFailRdOnlyWrongRevision;
      } else {
        result |= kFailRdOnlyOutdated;
      }
    }
  }

  // The process that opens the transaction does not stay alive for the life
  // time of the transaction
  if (!IsMountPoint(union_mnt)) {
    result |= kFailUnionBroken;
  } else {
    FileSystemInfo fs_info = GetFileSystemInfo(union_mnt);
    if (publisher_->in_transaction_ && fs_info.is_rdonly)
      result |= kFailUnionLocked;
    if (!publisher_->in_transaction_ && !fs_info.is_rdonly)
      result |= kFailUnionWritable;
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
  if (result & kFailUnionWritable) {
    LogCvmfs(kLogCvmfs, logFlags,
             "%s is not in a transaction but %s is mounted read/write",
             fqrn.c_str(), union_mnt.c_str());
  }
  if (result & kFailUnionLocked) {
    LogCvmfs(kLogCvmfs, logFlags,
             "%s is in a transaction but %s is not mounted read/write",
             fqrn.c_str(), union_mnt.c_str());
  }

  // Check whether we can repair

  switch (repair_mode) {
    case kUnionMountRepairNever:
      return result;
    case kUnionMountRepairAlways:
      break;
    case kUnionMountRepairSafe:
      if (publisher_->is_publishing()) {
        LogCvmfs(kLogCvmfs, logFlags,
          "WARNING: The repository %s is currently publishing and should not\n"
          "be touched. If you are absolutely sure, that this is _not_ the "
          "case,\nplease run the following command and retry:\n\n"
          "    rm -fR %s\n",
          fqrn.c_str(), publishing_lock.c_str());
        return result;
      }

      if (publisher_->in_transaction_) {
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
           fqrn.c_str(), result);

  // consecutively bring the mountpoints into a sane state by working bottom up:
  //   1. solve problems with the rdonly mountpoint
  //      Note: this might require to 'break' the union mount
  //            (kFailUnionBroken -> 1)
  //      1.1. solve outdated rdonly mountpoint (kFailRdOnlyOutdated -> 0)
  //      1.2. remount rdonly mountpoint        (kFailRdOnlyBroken   -> 0)
  //   2. solve problems with the union mountpoint
  //      2.1. mount the union mountpoint read-only    (kFailUnionBroken   -> 0)
  //      2.2. remount the union mountpoint read-only  (kFailUnionWritable -> 0)
  //      2.2. remount the union mountpoint read-write (kFailUnionLocked   -> 0)

  int log_flags = kLogSyslog;
  if (!is_quiet)
    log_flags |= kLogStderr;

  if ((result & kFailRdOnlyOutdated) || (result & kFailRdOnlyWrongRevision)) {
    if ((result & kFailUnionBroken) == 0) {
      AlterMountpoint(kAlterUnionUnmount, log_flags);
      result |= kFailUnionBroken;
    }

    if ((result & kFailRdOnlyBroken) == 0) {
      AlterMountpoint(kAlterRdOnlyUnmount, log_flags);
      result |= kFailRdOnlyBroken;
    }

    SetRootHash(expected_hash);
    result &= ~kFailRdOnlyOutdated;
    result &= ~kFailRdOnlyWrongRevision;
  }

  if (result & kFailRdOnlyBroken) {
    if ((result & kFailUnionBroken) == 0) {
      AlterMountpoint(kAlterUnionUnmount, log_flags);
      result |= kFailUnionBroken;
    }
    AlterMountpoint(kAlterRdOnlyMount, log_flags);
    result &= ~kFailRdOnlyBroken;
  }

  if (result & kFailUnionBroken) {
    AlterMountpoint(kAlterUnionMount, log_flags);
    // read-only mount by default
    if (publisher_->in_transaction_)
      result |= kFailUnionLocked;

    result &= ~kFailUnionBroken;
    result &= ~kFailUnionWritable;
  }

  if (result & kFailUnionLocked) {
    AlterMountpoint(kAlterUnionOpen, log_flags);
    result &= ~kFailUnionLocked;
  }

  if (result & kFailUnionWritable) {
    AlterMountpoint(kAlterUnionLock, log_flags);
    result &= ~kFailUnionWritable;
  }

  LogCvmfs(kLogCvmfs, kLogSyslog, "finished mountpoint repair (%d)", result);

  return result;
}

void Publisher::ManagedNode::AlterMountpoint(
  EMountpointAlterations how, int log_level)
{
  std::string mountpoint;
  std::string info_msg;
  std::string suid_helper_verb;
  switch (how) {
    case kAlterUnionUnmount:
      mountpoint = publisher_->settings_.transaction().spool_area().union_mnt();
      info_msg = "Trying to unmount " + mountpoint;
      suid_helper_verb = "rw_umount";
      break;
    case kAlterUnionLazyUnmount:
      mountpoint = publisher_->settings_.transaction().spool_area().union_mnt();
      info_msg = "Trying to lazily unmount " + mountpoint;
      suid_helper_verb = "rw_lazy_umount";
      break;
    case kAlterRdOnlyUnmount:
      mountpoint =
        publisher_->settings_.transaction().spool_area().readonly_mnt();
      info_msg = "Trying to unmount " + mountpoint;
      suid_helper_verb = "rdonly_umount";
      break;
    case kAlterRdOnlyKillUnmount:
      mountpoint =
        publisher_->settings_.transaction().spool_area().readonly_mnt();
      info_msg = "Trying to forcefully stop " + mountpoint;
      suid_helper_verb = "kill_cvmfs";
      break;
    case kAlterRdOnlyLazyUnmount:
      mountpoint =
        publisher_->settings_.transaction().spool_area().readonly_mnt();
      info_msg = "Trying to lazily unmount " + mountpoint;
      suid_helper_verb = "rdonly_lazy_umount";
      break;
    case kAlterUnionMount:
      mountpoint = publisher_->settings_.transaction().spool_area().union_mnt();
      info_msg = "Trying to mount " + mountpoint;
      suid_helper_verb = "rw_mount";
      break;
    case kAlterRdOnlyMount:
      mountpoint =
        publisher_->settings_.transaction().spool_area().readonly_mnt();
      info_msg = "Trying to mount " + mountpoint;
      suid_helper_verb = "rdonly_mount";
      break;
    case kAlterUnionOpen:
      mountpoint = publisher_->settings_.transaction().spool_area().union_mnt();
      info_msg = "Trying to remount " + mountpoint + " read/write";
      suid_helper_verb = "open";
      break;
    case kAlterUnionLock:
      mountpoint =
        publisher_->settings_.transaction().spool_area().union_mnt();
      info_msg = "Trying to remount " + mountpoint + " read-only";
      suid_helper_verb = "lock";
      break;
    case kAlterScratchWipe:
      mountpoint =
        publisher_->settings_.transaction().spool_area().scratch_dir();
      info_msg = "Trying to wipe out " + mountpoint + " (async cleanup)";
      suid_helper_verb = "clear_scratch_async";
      break;
    default:
      throw EPublish("internal error: unknown mountpoint alteration");
  }

  if (log_level & kLogStdout) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogNoLinebreak, "Note: %s... ",
             info_msg.c_str());
  }

  try {
    RunSuidHelper(suid_helper_verb, publisher_->settings_.fqrn());
    LogCvmfs(kLogCvmfs, (log_level & ~kLogStdout), "%s... success",
             info_msg.c_str());
    if (log_level & kLogStdout)
      LogCvmfs(kLogCvmfs, kLogStdout, "success");
  } catch (const EPublish&) {
    LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogErr, "%s... fail",
             info_msg.c_str());
    throw EPublish(info_msg + "... fail");
  }
}


void Publisher::ManagedNode::SetRootHash(const shash::Any &hash) {
  const std::string config_path =
    publisher_->settings_.transaction().spool_area().client_lconfig();
  SetInConfig(config_path, "CVMFS_ROOT_HASH", hash.ToString());
}

}  // namespace publish
