/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_IMPL_H_
#define CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_IMPL_H_

#include <algorithm>
#include <limits>
#include <string>
#include <vector>

#include "garbage_collection/garbage_collector.h"
#include "util/logging.h"
#include "util/string.h"

template<class CatalogTraversalT, class HashFilterT>
const uint64_t GarbageCollector<CatalogTraversalT, HashFilterT>::Configuration::
    kFullHistory = std::numeric_limits<uint64_t>::max();

template<class CatalogTraversalT, class HashFilterT>
const uint64_t GarbageCollector<CatalogTraversalT,
                                HashFilterT>::Configuration::kNoHistory = 0;

template<class CatalogTraversalT, class HashFilterT>
const time_t GarbageCollector<CatalogTraversalT,
                              HashFilterT>::Configuration::kNoTimestamp = 0;


template<class CatalogTraversalT, class HashFilterT>
GarbageCollector<CatalogTraversalT, HashFilterT>::GarbageCollector(
    const Configuration &configuration)
    : configuration_(configuration)
    , catalog_info_shim_(configuration.reflog)
    , traversal_(
          GarbageCollector<CatalogTraversalT, HashFilterT>::GetTraversalParams(
              configuration))
    , hash_filter_()
    , hash_map_delete_requests_()
    , use_reflog_timestamps_(false)
    , oldest_trunk_catalog_(static_cast<uint64_t>(-1))
    , oldest_trunk_catalog_found_(false)
    , preserved_catalogs_(0)
    , unreferenced_trees_(0)
    , condemned_trees_(0)
    , condemned_catalogs_(0)
    , last_reported_status_(0.0)
    , condemned_objects_(0)
    , condemned_bytes_(0)
    , duplicate_delete_requests_(0) {
  assert(configuration_.uploader != NULL);
}


template<class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::UseReflogTimestamps() {
  traversal_.SetCatalogInfoShim(&catalog_info_shim_);
  use_reflog_timestamps_ = true;
}


template<class CatalogTraversalT, class HashFilterT>
typename GarbageCollector<CatalogTraversalT, HashFilterT>::TraversalParameters
GarbageCollector<CatalogTraversalT, HashFilterT>::GetTraversalParams(
    const GarbageCollector<CatalogTraversalT, HashFilterT>::Configuration
        &config) {
  TraversalParameters params;
  params.object_fetcher = config.object_fetcher;
  params.history = config.keep_history_depth;
  params.timestamp = config.keep_history_timestamp;
  params.no_repeat_history = true;
  params.ignore_load_failure = true;
  params.quiet = !config.verbose;
  params.num_threads = config.num_threads;
  return params;
}


template<class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::PreserveDataObjects(
    const GarbageCollector<CatalogTraversalT,
                           HashFilterT>::TraversalCallbackDataTN
        &data  // NOLINT(runtime/references)
) {
  ++preserved_catalogs_;

  if (data.catalog->IsRoot()) {
    const uint64_t mtime = use_reflog_timestamps_
                               ? catalog_info_shim_.GetLastModified(
                                     data.catalog)
                               : data.catalog->GetLastModified();
    if (!oldest_trunk_catalog_found_)
      oldest_trunk_catalog_ = std::min(oldest_trunk_catalog_, mtime);
    if (configuration_.verbose) {
      const uint64_t rev = data.catalog->revision();
      LogCvmfs(
          kLogGc, kLogStdout | kLogDebug,
          "Preserving Revision %" PRIu64 " (%s / added @ %s)", rev,
          StringifyTime(data.catalog->GetLastModified(), true).c_str(),
          StringifyTime(catalog_info_shim_.GetLastModified(data.catalog), true)
              .c_str());
      PrintCatalogTreeEntry(data.tree_level, data.catalog);
    }
    if (data.catalog->schema() < 0.99) {
      LogCvmfs(
          kLogGc, kLogStdout | kLogDebug,
          "Warning: "
          "legacy catalog does not provide access to nested catalog "
          "hierarchy.\n"
          "         Some unreferenced objects may remain in the repository.");
    }
  }

  // the hash of the actual catalog needs to preserved
  hash_filter_.Fill(data.catalog->hash());

  // all the objects referenced from this catalog need to be preserved
  const HashVector &referenced_hashes = data.catalog->GetReferencedObjects();
  typename HashVector::const_iterator i = referenced_hashes.begin();
  const typename HashVector::const_iterator iend = referenced_hashes.end();
  for (; i != iend; ++i) {
    hash_filter_.Fill(*i);
  }
}


template<class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::SweepDataObjects(
    const GarbageCollector<CatalogTraversalT,
                           HashFilterT>::TraversalCallbackDataTN
        &data  // NOLINT(runtime/references)
) {
  ++condemned_catalogs_;
  if (data.catalog->IsRoot())
    ++condemned_trees_;

  if (configuration_.verbose) {
    if (data.catalog->IsRoot()) {
      const uint64_t rev = data.catalog->revision();
      const time_t mtime = static_cast<time_t>(data.catalog->GetLastModified());
      LogCvmfs(kLogGc, kLogStdout | kLogDebug,
               "Sweeping Revision %" PRIu64 " (%s)", rev,
               StringifyTime(mtime, true).c_str());
    }
    PrintCatalogTreeEntry(data.tree_level, data.catalog);
  }

  // all the objects referenced from this catalog need to be checked against the
  // the preserved hashes in the hash_filter_ and possibly deleted
  const HashVector &referenced_hashes = data.catalog->GetReferencedObjects();
  typename HashVector::const_iterator i = referenced_hashes.begin();
  const typename HashVector::const_iterator iend = referenced_hashes.end();
  for (; i != iend; ++i) {
    CheckAndSweep(*i);
  }

  // the catalog itself is also condemned and needs to be removed
  CheckAndSweep(data.catalog->hash());

  float threshold = static_cast<float>(condemned_trees_)
                    / static_cast<float>(unreferenced_trees_);
  if (threshold > last_reported_status_ + 0.1) {
    LogCvmfs(kLogGc, kLogStdout | kLogDebug,
             "      - %02.0f%%    %" PRIu64 " / %" PRIu64
             " unreferenced revisions removed [%s]",
             100.0 * threshold, condemned_trees_, unreferenced_trees_,
             RfcTimestamp().c_str());
    last_reported_status_ = threshold;
  }
}


template<class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::CheckAndSweep(
    const shash::Any &hash) {
  if (!hash_filter_.Contains(hash)) {
    if (!hash_map_delete_requests_.Contains(hash)) {
      hash_map_delete_requests_.Fill(hash);
      Sweep(hash);
    } else {
      ++duplicate_delete_requests_;
      LogCvmfs(kLogGc, kLogDebug, "Hash %s already marked as to delete",
               hash.ToString().c_str());
    }
  }
}


template<class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::Sweep(
    const shash::Any &hash) {
  ++condemned_objects_;
  if (configuration_.extended_stats) {
    if (!hash.HasSuffix() || hash.suffix == shash::kSuffixPartial) {
      int64_t condemned_bytes = configuration_.uploader->GetObjectSize(hash);
      if (condemned_bytes > 0) {
        condemned_bytes_ += condemned_bytes;
      }
    }
  }

  LogDeletion(hash);
  if (configuration_.dry_run) {
    return;
  }

  configuration_.uploader->RemoveAsync(hash);
}


template<class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::RemoveCatalogFromReflog(
    const shash::Any &catalog) {
  assert(catalog.suffix == shash::kSuffixCatalog);
  return (configuration_.dry_run) ? true
                                  : configuration_.reflog->Remove(catalog);
}


template<class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::Collect() {
  return AnalyzePreservedCatalogTree() && CheckPreservedRevisions()
         && SweepReflog();
}


template<class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT,
                      HashFilterT>::AnalyzePreservedCatalogTree() {
  LogCvmfs(kLogGc, kLogStdout, "  --> marking unreferenced objects [%s]",
           RfcTimestamp().c_str());
  if (configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStdout | kLogDebug,
             "Preserving data objects in latest revision");
  }

  typename CatalogTraversalT::CallbackTN
      *callback = traversal_.RegisterListener(
          &GarbageCollector<CatalogTraversalT,
                            HashFilterT>::PreserveDataObjects,
          this);

  bool success = traversal_.Traverse();
  oldest_trunk_catalog_found_ = true;
  success = success && traversal_.TraverseNamedSnapshots();
  traversal_.UnregisterListener(callback);

  return success;
}


template<class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT,
                      HashFilterT>::CheckPreservedRevisions() {
  const bool keeps_revisions = (preserved_catalog_count() > 0);
  if (!keeps_revisions && configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStderr | kLogDebug,
             "This would delete everything! Abort.");
  }

  return keeps_revisions;
}


template<class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::SweepReflog() {
  LogCvmfs(kLogGc, kLogStdout, "  --> sweeping unreferenced objects [%s]",
           RfcTimestamp().c_str());

  const ReflogTN *reflog = configuration_.reflog;
  std::vector<shash::Any> catalogs;
  if (NULL == reflog || !reflog->List(SqlReflog::kRefCatalog, &catalogs)) {
    LogCvmfs(kLogGc, kLogStderr, "Failed to list catalog reference log");
    return false;
  }

  typename CatalogTraversalT::CallbackTN
      *callback = traversal_.RegisterListener(
          &GarbageCollector<CatalogTraversalT, HashFilterT>::SweepDataObjects,
          this);

  std::vector<shash::Any> to_sweep;
  std::vector<shash::Any>::const_iterator i = catalogs.begin();
  std::vector<shash::Any>::const_iterator iend = catalogs.end();
  for (; i != iend; ++i) {
    if (!hash_filter_.Contains(*i)) {
      to_sweep.push_back(*i);
    }
  }
  unreferenced_trees_ = to_sweep.size();
  bool success = traversal_.TraverseList(to_sweep,
                                         CatalogTraversalT::kDepthFirst);
  traversal_.UnregisterListener(callback);

  i = to_sweep.begin();
  iend = to_sweep.end();
  for (; i != iend; ++i) {
    success = success && RemoveCatalogFromReflog(*i);
  }

  // TODO(jblomer): turn current counters into perf::Counters
  if (configuration_.statistics) {
    perf::Counter *ctr_preserved_catalogs = configuration_.statistics->Register(
        "gc.n_preserved_catalogs", "number of live catalogs");
    perf::Counter *ctr_condemned_catalogs = configuration_.statistics->Register(
        "gc.n_condemned_catalogs", "number of dead catalogs");
    perf::Counter *ctr_condemned_objects = configuration_.statistics->Register(
        "gc.n_condemned_objects", "number of deleted objects");
    perf::Counter *ctr_condemned_bytes = configuration_.statistics->Register(
        "gc.sz_condemned_bytes", "number of deleted bytes");
    perf::Counter
        *ctr_duplicate_delete_requests = configuration_.statistics->Register(
            "gc.n_duplicate_delete_requests",
            "number of duplicated delete requests");
    ctr_preserved_catalogs->Set(preserved_catalog_count());
    ctr_condemned_catalogs->Set(condemned_catalog_count());
    ctr_condemned_objects->Set(condemned_objects_count());
    ctr_condemned_bytes->Set(condemned_bytes_count());
    ctr_duplicate_delete_requests->Set(duplicate_delete_requests());
  }

  configuration_.uploader->WaitForUpload();
  LogCvmfs(kLogGc, kLogStdout, "  --> done garbage collecting [%s]",
           RfcTimestamp().c_str());
  return success && (configuration_.uploader->GetNumberOfErrors() == 0);
}


template<class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::PrintCatalogTreeEntry(
    const unsigned int tree_level, const CatalogTN *catalog) const {
  std::string tree_indent;
  for (unsigned int i = 0; i < tree_level; ++i) {
    tree_indent += "\u2502  ";
  }
  tree_indent += "\u251C\u2500 ";

  const std::string hash_string = catalog->hash().ToString();
  const std::string path = (catalog->mountpoint().IsEmpty())
                               ? "/"
                               : catalog->mountpoint().ToString();

  LogCvmfs(kLogGc, kLogStdout, "%s%s %s", tree_indent.c_str(),
           hash_string.c_str(), path.c_str());
  LogCvmfs(kLogGc, kLogDebug, "catalog tree entry: %s %s", hash_string.c_str(),
           path.c_str());
}


template<class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::LogDeletion(
    const shash::Any &hash) const {
  if (configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStdout | kLogDebug, "Sweep: %s",
             hash.ToStringWithSuffix().c_str());
  }

  if (configuration_.has_deletion_log()) {
    const int written = fprintf(configuration_.deleted_objects_logfile, "%s\n",
                                hash.ToStringWithSuffix().c_str());
    if (written < 0) {
      LogCvmfs(kLogGc, kLogStderr, "failed to write to deleted objects log");
    }
  }
}

#endif  // CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_IMPL_H_
