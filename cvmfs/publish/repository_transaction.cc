/**
 * This file is part of the CernVM File System.
 */


#include <memory>
#include <string>

#include "backoff.h"
#include "catalog_mgr_ro.h"
#include "catalog_mgr_rw.h"
#include "directory_entry.h"
#include "gateway_util.h"
#include "manifest.h"
#include "publish/except.h"
#include "publish/repository.h"
#include "publish/repository_util.h"
#include "publish/settings.h"
#include "util/exception.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/string.h"

namespace publish {


void Publisher::TransactionRetry() {
  if (managed_node_.get() != nullptr) {
    const int rvi = managed_node_->Check(false /* is_quiet */);
    if (rvi != 0)
      throw EPublish("cannot establish writable mountpoint");
  }

  BackoffThrottle throttle(500, 5000, 10000);
  // Negative timeouts (i.e.: no retry) will result in a deadline that has
  // already passed and thus has the correct effect
  uint64_t deadline = platform_monotonic_time()
                      + settings_.transaction().GetTimeoutS();
  if (settings_.transaction().GetTimeoutS() == 0)
    deadline = uint64_t(-1);

  while (true) {
    try {
      TransactionImpl();
      break;
    } catch (const publish::EPublish &e) {
      if (e.failure() != EPublish::kFailTransactionState) {
        session_->Drop();
        in_transaction_.Clear();
      }

      if ((e.failure() == EPublish::kFailTransactionState)
          || (e.failure() == EPublish::kFailLeaseBusy)) {
        if (platform_monotonic_time() > deadline)
          throw;

        LogCvmfs(kLogCvmfs, kLogStdout, "repository busy, retrying");
        throttle.Throttle();
        continue;
      }

      throw;
    }  // try-catch
  }  // while (true)

  if (managed_node_.get() != nullptr)
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
  session_->Acquire();

  // Now that the lease is held (the lease subtree is frozen) refresh to the
  // current HEAD before any HEAD-dependent step. The manifest fetched when this
  // process started can be stale: another release manager may have advanced
  // HEAD in the meantime. Refreshing here makes both the lease-path validation
  // below and the catalog diff at publish time see post-lease HEAD. Without it,
  // a parent path another publisher just created looks absent (spurious
  // kFailLeaseNoEntry), and concurrently-added content looks deleted and gets
  // dropped (#3867). Done on every gateway transaction, not only when waiting
  // on a busy lease -- staleness is independent of contention.
  // DownloadRootObjects also invalidates the cached read-only catalog manager;
  // Check() remounts the read-only layer only if outdated; managed_node_ is
  // absent for mount-less publishing.
  if (settings_.storage().type() == upload::SpoolerDefinition::Gateway) {
    DownloadRootObjects(settings_.url(), settings_.fqrn(),
                        settings_.transaction().spool_area().tmp_dir());
    if (managed_node_.get() != nullptr) {
      const int rvi = managed_node_->Check(true /* is_quiet */);
      if (rvi != 0)
        throw EPublish("cannot establish writable mountpoint");
    }
  }

  // Missing lease parents require --allow-nonexistent-path and receiver API 4.
  // The receiver materializes the ancestors during the catalog merge.
  if (!settings_.transaction().lease_path().empty()) {
    const std::string path = GetParentPath(
        "/" + settings_.transaction().lease_path());
    catalog::SimpleCatalogManager *catalog_mgr = GetSimpleCatalogManager();
    catalog::DirectoryEntry dirent;
    const bool retval = catalog_mgr->LookupPath(path, catalog::kLookupDefault,
                                                &dirent);
    if (!retval) {
      if (!settings_.transaction().allow_nonexistent_path()) {
        throw EPublish("cannot open transaction on non-existing path " + path
                           + " (use --allow-nonexistent-path to permit this)",
                       EPublish::kFailLeaseNoEntry);
      }
      // Refuse before upload when the receiver cannot create the ancestors.
      // Local publishing builds the complete catalog itself.
      if (settings_.storage().type() == upload::SpoolerDefinition::Gateway
          && session_->negotiated_api_version()
                 < gateway::kApiVersionNonexistentPath) {
        const int negotiated_version = session_->negotiated_api_version();
        if (negotiated_version < 0) {
          throw EPublish(
              "cannot verify gateway support for opening a transaction on the "
              "non-existing path "
                  + path
                  + ": the existing lease token has no recorded API "
                    "negotiation; "
                    "drop the lease and acquire it again",
              EPublish::kFailInput);
        }
        throw EPublish(
            "the gateway does not support opening a transaction on the "
            "non-existing path "
                + path + " (needs API version "
                + StringifyInt(gateway::kApiVersionNonexistentPath)
                + ", gateway negotiated " + StringifyInt(negotiated_version)
                + "); upgrade the gateway or create the parent path first",
            EPublish::kFailInput);
      }
      LogCvmfs(kLogCvmfs, llvl_ | kLogStdout | kLogSyslog,
               "opening transaction on non-existing path %s; missing parent "
               "directories will be created at commit time",
               path.c_str());
    } else if (!dirent.IsDirectory()) {
      throw EPublish(
          "cannot open transaction on " + path + ", which is not a directory",
          EPublish::kFailLeaseNoDir);
    }
  }

  const std::unique_ptr<CheckoutMarker> marker(CheckoutMarker::CreateFrom(
      settings_.transaction().spool_area().checkout_marker()));

  in_transaction_.Set();
  // Pre-create the publishing lock file so that Abort() can acquire it even
  // if the disk fills up before abort is called.
  is_publishing_.Touch();
  ConstructSpoolers();
  if (marker.get() != nullptr)
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
      const std::string panic_msg = e.what();
      in_transaction_.Clear();
      // TODO(aandvalenzuela): release session token (gateway publishing)
      throw publish::EPublish("cannot clone directory tree. " + panic_msg,
                              publish::EPublish::kFailInput);
    }

    Sync();
    SendTalkCommand(
        settings_.transaction().spool_area().readonly_talk_socket(),
        "chroot " + settings_.transaction().base_hash().ToString() + "\n");
    LogCvmfs(kLogCvmfs, llvl_ | kLogStdout, "[done]");
    // TODO(jblomer): fix-me
    // PushReflog();
  }

  LogCvmfs(kLogCvmfs, llvl_ | kLogDebug | kLogSyslog, "(%s) opened transaction",
           settings_.fqrn().c_str());
}

}  // namespace publish
