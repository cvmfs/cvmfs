/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <string>

#include "logging.h"
#include "publish/except.h"
#include "publish/repository_util.h"
#include "publish/settings.h"

namespace publish {

void Publisher::Transaction(const std::string &path) {
  if (in_transaction_) {
    throw EPublish("another transaction is already open",
                   EPublish::kIdTransactionLocked);
  }

  InitSpoolArea();

  // No-op for all but the gateway spooler
  AcquireLease(path);

  const std::string transaction_lock =
    settings_.transaction().spool_area().transaction_lock();
  ServerLockFile::Acquire(transaction_lock, true /* ignore_stale */);

  in_transaction_ = true;
  LogCvmfs(kLogCvmfs, llvl_ | kLogDebug | kLogSyslog,
           "(%s) opened transaction", settings_.fqrn().c_str());
}

}  // namespace publish
