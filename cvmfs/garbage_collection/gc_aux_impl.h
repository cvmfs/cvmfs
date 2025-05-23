/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_GARBAGE_COLLECTION_GC_AUX_IMPL_H_
#define CVMFS_GARBAGE_COLLECTION_GC_AUX_IMPL_H_

#include <cstdio>
#include <string>
#include <vector>

#include "util/logging.h"
#include "util/string.h"

template<class CatalogTraversalT, class HashFilterT>
GarbageCollectorAux<CatalogTraversalT, HashFilterT>::GarbageCollectorAux(
    const ConfigurationTN &config)
    : config_(config) {
  assert(config_.uploader != NULL);
}


template<class CatalogTraversalT, class HashFilterT>
bool GarbageCollectorAux<CatalogTraversalT, HashFilterT>::CollectOlderThan(
    uint64_t timestamp, const HashFilterT &preserved_objects) {
  if (config_.verbose) {
    LogCvmfs(kLogGc, kLogStdout | kLogDebug,
             "Sweeping auxiliary objects older than %s",
             StringifyTime(timestamp, true).c_str());
  }
  std::vector<SqlReflog::ReferenceType> aux_types;
  aux_types.push_back(SqlReflog::kRefCertificate);
  aux_types.push_back(SqlReflog::kRefHistory);
  aux_types.push_back(SqlReflog::kRefMetainfo);
  for (unsigned i = 0; i < aux_types.size(); ++i) {
    std::vector<shash::Any> hashes;
    bool retval = config_.reflog->ListOlderThan(aux_types[i], timestamp,
                                                &hashes);
    if (!retval) {
      LogCvmfs(kLogGc, kLogStderr, "failed to enumerate %s objects",
               PrintAuxType(aux_types[i]).c_str());
      return 1;
    }
    if (config_.verbose) {
      LogCvmfs(kLogGc, kLogStdout | kLogDebug, "Scanning %lu %s objects",
               hashes.size(), PrintAuxType(aux_types[i]).c_str());
    }

    for (unsigned iter = 0; iter < hashes.size(); ++iter) {
      if (preserved_objects.Contains(hashes[iter])) {
        if (config_.verbose) {
          LogCvmfs(kLogGc, kLogStdout | kLogDebug, "  preserving: %s",
                   hashes[iter].ToStringWithSuffix().c_str());
        }
        continue;
      }

      if (!Sweep(hashes[iter]))
        return false;
    }
  }

  config_.uploader->WaitForUpload();
  return config_.uploader->GetNumberOfErrors() == 0;
}


template<class CatalogTraversalT, class HashFilterT>
std::string GarbageCollectorAux<CatalogTraversalT, HashFilterT>::PrintAuxType(
    SqlReflog::ReferenceType type) {
  switch (type) {
    case SqlReflog::kRefCatalog:
      return "file catalog";
    case SqlReflog::kRefCertificate:
      return "certificate";
    case SqlReflog::kRefHistory:
      return "tag database";
    case SqlReflog::kRefMetainfo:
      return "repository meta information";
  }
  // Never here
  return "UNKNOWN";
}


template<class CatalogTraversalT, class HashFilterT>
bool GarbageCollectorAux<CatalogTraversalT, HashFilterT>::Sweep(
    const shash::Any &hash) {
  if (config_.verbose) {
    LogCvmfs(kLogGc, kLogStdout | kLogDebug, "  sweep: %s",
             hash.ToStringWithSuffix().c_str());
  }

  if (!config_.dry_run) {
    config_.uploader->RemoveAsync(hash);
    bool retval = config_.reflog->Remove(hash);
    if (!retval) {
      LogCvmfs(kLogGc, kLogStderr, "failed to remove %s from reference log",
               hash.ToStringWithSuffix().c_str());
      return false;
    }
  }

  if (config_.has_deletion_log()) {
    const int written = fprintf(config_.deleted_objects_logfile, "%s\n",
                                hash.ToStringWithSuffix().c_str());
    if (written < 0) {
      LogCvmfs(kLogGc, kLogStderr, "failed to write to deleted objects log");
      return false;
    }
  }

  return true;
}

#endif  // CVMFS_GARBAGE_COLLECTION_GC_AUX_IMPL_H_
