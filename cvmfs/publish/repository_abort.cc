/**
 * This file is part of the CernVM File System.
 */


#include <unistd.h>

#include <string>

#include "publish/except.h"
#include "publish/repository.h"
#include "publish/repository_util.h"
#include "publish/settings.h"
#include "util/logging.h"
#include "util/posix.h"

namespace {

void TrySessionDrop(publish::Publisher::Session *session,
                    bool ignore_invalid_lease) {
  try {
    session->Drop();
  } catch (const publish::EPublish &e) {
    if (ignore_invalid_lease
        && ((e.failure() == e.kFailLeaseBody)
            || (e.failure() == e.kFailLeaseNoEntry))) {
      LogCvmfs(kLogCvmfs, kLogStderr | kLogSyslogWarn,
               "force abort, continue despite error while trying to drop lease,"
               " removing session token. Error: %s",
               e.msg().c_str());
      unlink(session->token_path().c_str());
      return;
    }
    throw e;
  }
}

}  // anonymous namespace

namespace publish {

void Publisher::WipeScratchArea() {
  // TODO(jblomer): implement for enter shell etc.
  if (!managed_node_.IsValid())
    return;

  managed_node_->ClearScratch();
}

void Publisher::Abort() {
  ServerLockFileGuard g(is_publishing_);

  if (!in_transaction_.IsSet()) {
    if (session_->has_lease()) {
      LogCvmfs(kLogCvmfs, kLogSyslogWarn, "removing stale session token for %s",
               settings_.fqrn().c_str());
      TrySessionDrop(session_.weak_ref(), settings_.ignore_invalid_lease());
    }
    throw EPublish(
        "Repository " + settings_.fqrn() + " is not in a transaction",
        EPublish::kFailTransactionState);
  }

  TrySessionDrop(session_.weak_ref(), settings_.ignore_invalid_lease());

  if (managed_node_.IsValid()) {
    // We already checked for is_publishing and in_transaction.  Normally, at
    // this point we do want to repair the mount points of a repository
    // in transaction
    EUnionMountRepairMode
        repair_mode = settings_.transaction().spool_area().repair_mode();
    if (repair_mode == kUnionMountRepairSafe) {
      settings_.GetTransaction()->GetSpoolArea()->SetRepairMode(
          kUnionMountRepairAlways);
    }
    int rvi = managed_node_->Check(false /* is_quiet */);
    settings_.GetTransaction()->GetSpoolArea()->SetRepairMode(repair_mode);
    if (rvi != 0)
      throw EPublish("publisher file system mount state is broken");

    managed_node_->Unmount();
    managed_node_->ClearScratch();
    managed_node_->Mount();
  }

  in_transaction_.Clear();
}

}  // namespace publish
