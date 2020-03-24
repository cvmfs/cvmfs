/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "publish/repository.h"

#include <string>

#include "catalog_mgr_ro.h"
#include "directory_entry.h"
#include "logging.h"
#include "publish/except.h"
#include "publish/repository_util.h"
#include "publish/settings.h"
#include "upload.h"

namespace publish {

void Publisher::Transaction(const std::string &path) {
  if (in_transaction_) {
    throw EPublish("another transaction is already open",
                   EPublish::kIdTransactionLocked);
  }

  InitSpoolArea();

  // TODO(jblomer): create Spooler::AcquireLease method
  if (spooler()->GetDriverType() == upload::SpoolerDefinition::Gateway) {
    catalog::SimpleCatalogManager *catalog_mgr = GetSimpleCatalogManager();
    catalog::DirectoryEntry dirent;
    bool retval = catalog_mgr->LookupPath(path, catalog::kLookupSole, &dirent);
    if (!retval) {
      throw EPublish("cannot open transaction on non-existing path " + path);
    }
    if (!dirent.IsDirectory()) {
      throw EPublish("cannot open transaction on " + path + ", which is not "
                     "a directory");
    }


  }

  const std::string transaction_lock =
    settings_.transaction().spool_area().transaction_lock();
  ServerLockFile::Acquire(transaction_lock, true /* ignore_stale */);

  in_transaction_ = true;
  LogCvmfs(kLogCvmfs, llvl_ | kLogDebug | kLogSyslog,
           "(%s) opened transaction", settings_.fqrn().c_str());
}

}  // namespace publish