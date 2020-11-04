/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <string>

#include "backoff.h"
#include "catalog_mgr_ro.h"
#include "catalog_mgr_rw.h"
#include "directory_entry.h"
#include "logging.h"
#include "manifest.h"
#include "publish/except.h"
#include "publish/repository_util.h"
#include "publish/settings.h"
#include "util/pointer.h"
#include "util/posix.h"

namespace publish {

void Publisher::CheckTransactionStatus() {
  // The process that opens the transaction does not stay alive for the life
  // time of the transaction
  const std::string transaction_lock =
    settings_.transaction().spool_area().transaction_lock();
  in_transaction_ =
    ServerLockFile::IsLocked(transaction_lock, true /* ignore_stale */);
}


void Publisher::TransactionRetry() {
  if (settings_.transaction().GetTimeoutS() < 0) {
    TransactionImpl();
    return;
  }

  BackoffThrottle throttle(500, 5000, 10000);
  uint64_t deadline = platform_monotonic_time() +
                      settings_.transaction().GetTimeoutS();
  if (settings_.transaction().GetTimeoutS() == 0)
    deadline = uint64_t(-1);

  while (true) {
    try {
      TransactionImpl();
      break;
    } catch (const publish::EPublish& e) {
      if ((e.failure() == EPublish::kFailTransactionLocked) ||
          (e.failure() == EPublish::kFailLeaseBusy))
      {
        if (platform_monotonic_time() > deadline)
          throw;

        LogCvmfs(kLogCvmfs, kLogStdout, "repository busy, retrying");
        throttle.Throttle();
        CheckTransactionStatus();
        continue;
      }

      throw;
    }  // try-catch
  }  // while (true)
}


void Publisher::TransactionImpl() {
  if (in_transaction_) {
    throw EPublish("another transaction is already open",
                   EPublish::kFailTransactionLocked);
  }

  InitSpoolArea();

  // We might have a valid lease for a non-existing path. Nevertheless, we run
  // run into problems when merging catalogs later, so for the time being we
  // disallow transactions on non-existing paths.
  if (!settings_.transaction().lease_path().empty()) {
    std::string path = GetParentPath(
      "/" + settings_.transaction().lease_path());
    catalog::SimpleCatalogManager *catalog_mgr = GetSimpleCatalogManager();
    catalog::DirectoryEntry dirent;
    bool retval = catalog_mgr->LookupPath(path, catalog::kLookupSole, &dirent);
    if (!retval) {
      throw EPublish("cannot open transaction on non-existing path " + path,
                     EPublish::kFailLeaseNoEntry);
    }
    if (!dirent.IsDirectory()) {
      throw EPublish(
        "cannot open transaction on " + path + ", which is not a directory",
        EPublish::kFailLeaseNoDir);
    }
  }

  UniquePtr<Session> session(Session::Create(settings_));
  ConstructSpoolers();

  const std::string transaction_lock =
    settings_.transaction().spool_area().transaction_lock();
  ServerLockFile::Acquire(transaction_lock, true /* ignore_stale */);

  UniquePtr<CheckoutMarker> marker(CheckoutMarker::CreateFrom(
    settings_.transaction().spool_area().checkout_marker()));
  // TODO(jblomer): take root hash from r/o mountpoint?
  if (marker.IsValid())
    settings_.GetTransaction()->SetBaseHash(marker->hash());
  else
    settings_.GetTransaction()->SetBaseHash(manifest_->catalog_hash());

  if (settings_.transaction().HasTemplate()) {
    LogCvmfs(kLogCvmfs, llvl_ | kLogStdout | kLogNoLinebreak,
             "CernVM-FS: cloning template %s --> %s ... ",
             settings_.transaction().template_from().c_str(),
             settings_.transaction().template_to().c_str());
    ConstructSyncManagers();
    catalog_mgr_->CloneTree(settings_.transaction().template_from(),
                            settings_.transaction().template_to());
    Sync();
    SendTalkCommand(settings_.transaction().spool_area().readonly_talk_socket(),
      "chroot " + settings_.transaction().base_hash().ToString() + "\n");
    LogCvmfs(kLogCvmfs, llvl_ | kLogStdout, "[done]");
    // TODO(jblomer): fix-me
    // PushReflog();
  }

  in_transaction_ = true;
  LogCvmfs(kLogCvmfs, llvl_ | kLogDebug | kLogSyslog,
           "(%s) opened transaction", settings_.fqrn().c_str());
}

}  // namespace publish
