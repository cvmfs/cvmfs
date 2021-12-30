/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <unistd.h>

#include <string>

#include "logging.h"
#include "publish/except.h"
#include "publish/repository_util.h"
#include "publish/settings.h"
#include "util/posix.h"

namespace publish {

void Publisher::WipeScratchArea() {
  // TODO(jblomer): implement for enter shell etc.
  if (!managed_node_.IsValid())
    return;

  managed_node_->ClearScratch();
}

void Publisher::Abort() {
  if (is_publishing_.IsLocked()) {
    throw EPublish(
      "Repository " + settings_.fqrn() + " is currently publishing "
      "(aborting abort)", EPublish::kFailTransactionState);
  }

  if (!in_transaction_.IsLocked()) {
    if (session_->has_lease()) {
      LogCvmfs(kLogCvmfs, kLogSyslogWarn, "removing stale session token for %s",
               settings_.fqrn().c_str());
      session_->Drop();
    }
    throw EPublish(
      "Repository " + settings_.fqrn() + " is not in a transaction",
      EPublish::kFailTransactionState);
  }

  session_->Drop();

  if (managed_node_.IsValid()) {
    // We already checked for is_publishing and in_transaction.  Normally, at
    // this point we do want to repair the mount points of a repository
    // in transaction
    EUnionMountRepairMode repair_mode =
      settings_.transaction().spool_area().repair_mode();
    if (repair_mode == kUnionMountRepairSafe) {
      settings_.GetTransaction()->GetSpoolArea()->SetRepairMode(
        kUnionMountRepairAlways);
    }
    int rvi = managed_node_->Check(false /* is_quiet */);
    settings_.GetTransaction()->GetSpoolArea()->SetRepairMode(repair_mode);
    if (rvi != 0) throw EPublish("publisher file system mount state is broken");

    managed_node_->Unmount();
    managed_node_->ClearScratch();
    managed_node_->Mount();
  }

  in_transaction_.Release();
}

}  // namespace publish
