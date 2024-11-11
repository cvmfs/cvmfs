/**
 * This file is part of the CernVM File System.
 */


#include "publish/repository.h"

#include <string>

#include "backoff.h"
#include "catalog_mgr_ro.h"
#include "catalog_mgr_rw.h"
#include "directory_entry.h"
#include "manifest.h"
#include "publish/except.h"
#include "publish/repository_util.h"
#include "publish/settings.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/pointer.h"
#include "util/posix.h"

namespace publish {


void Publisher::TransactionRetry() {
  if (managed_node_.IsValid()) {
    int rvi = managed_node_->Check(false /* is_quiet */);
    if (rvi != 0) throw EPublish("cannot establish writable mountpoint");
  }

  BackoffThrottle throttle(500, 5000, 10000);
  // Negative timeouts (i.e.: no retry) will result in a deadline that has
  // already passed and thus has the correct effect
  uint64_t deadline = platform_monotonic_time() +
                      settings_.transaction().GetTimeoutS();
  if (settings_.transaction().GetTimeoutS() == 0)
    deadline = uint64_t(-1);

  while (true) {
    try {
      TransactionImpl();
      break;
    } catch (const publish::EPublish& e) {
      if (e.failure() != EPublish::kFailTransactionState) {
        session_->Drop();
        in_transaction_.Clear();
      }

      if ((e.failure() == EPublish::kFailTransactionState) ||
          (e.failure() == EPublish::kFailLeaseBusy))
      {
        if (platform_monotonic_time() > deadline)
          throw;

        LogCvmfs(kLogCvmfs, kLogStdout, "repository busy, retrying");
        throttle.Throttle();
        continue;
      }

      throw;
    }  // try-catch
  }  // while (true)

  if (managed_node_.IsValid())
    managed_node_->Open();
}


void Publisher::TransactionImpl() {
  if (in_transaction_.IsSet()) {
    throw EPublish("another transaction is already open",
                   EPublish::kFailTransactionState);
  }

  InitSpoolArea();

  // On error, Transaction() will release the transaction lock and drop
  // the session
  in_transaction_.Set();
  session_->Acquire();

  // We might have a valid lease for a non-existing path. Nevertheless, we run
  // run into problems when merging catalogs later, so for the time being we
  // disallow transactions on non-existing paths.
  if (!settings_.transaction().lease_path().empty()) {
    std::string path = GetParentPath(
      "/" + settings_.transaction().lease_path());
    catalog::SimpleCatalogManager *catalog_mgr = GetSimpleCatalogManager();
    catalog::DirectoryEntry dirent;
    bool retval = catalog_mgr->LookupPath(path, catalog::kLookupDefault,
                                          &dirent);
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

  ConstructSpoolers();

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

    try {
      catalog_mgr_->CloneTree(settings_.transaction().template_from(),
                              settings_.transaction().template_to());
    } catch (const ECvmfsException &e) {
      std::string panic_msg = e.what();
      in_transaction_.Clear();
      // TODO(aandvalenzuela): release session token (gateway publishing)
      throw publish::EPublish("cannot clone directory tree. " + panic_msg,
                              publish::EPublish::kFailInput);
    }

    Sync();
    SendTalkCommand(settings_.transaction().spool_area().readonly_talk_socket(),
      "chroot " + settings_.transaction().base_hash().ToString() + "\n");
    LogCvmfs(kLogCvmfs, llvl_ | kLogStdout, "[done]");
    // TODO(jblomer): fix-me
    // PushReflog();
  }

  LogCvmfs(kLogCvmfs, llvl_ | kLogDebug | kLogSyslog,
           "(%s) opened transaction", settings_.fqrn().c_str());
}

}  // namespace publish
