/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_IMPL_H_
#define CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_IMPL_H_

#include <algorithm>
#include <limits>
#include <string>
#include <vector>

#include "logging.h"

template<class CatalogTraversalT, class HashFilterT>
const unsigned int GarbageCollector<CatalogTraversalT,
                                    HashFilterT>::Configuration::kFullHistory =
  std::numeric_limits<unsigned int>::max();

template<class CatalogTraversalT, class HashFilterT>
const unsigned int GarbageCollector<CatalogTraversalT,
                                    HashFilterT>::Configuration::kNoHistory = 0;

template<class CatalogTraversalT, class HashFilterT>
const time_t GarbageCollector<CatalogTraversalT,
                              HashFilterT>::Configuration::kNoTimestamp = 0;


template <class CatalogTraversalT, class HashFilterT>
GarbageCollector<CatalogTraversalT, HashFilterT>::GarbageCollector(
                                             const Configuration &configuration)
  : configuration_(configuration)
  , catalog_info_shim_(configuration.reflog)
  , traversal_(
      GarbageCollector<CatalogTraversalT, HashFilterT>::GetTraversalParams(
                                                                configuration))
  , hash_filter_()
  , use_reflog_timestamps_(false)
  , oldest_trunk_catalog_(static_cast<uint64_t>(-1))
  , oldest_trunk_catalog_found_(false)
  , preserved_catalogs_(0)
  , condemned_catalogs_(0)
  , condemned_objects_(0)
{
  assert(configuration_.uploader != NULL);
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::UseReflogTimestamps() {
  traversal_.SetCatalogInfoShim(&catalog_info_shim_);
  use_reflog_timestamps_ = true;
}


template <class CatalogTraversalT, class HashFilterT>
typename GarbageCollector<CatalogTraversalT, HashFilterT>::TraversalParameters
  GarbageCollector<CatalogTraversalT, HashFilterT>::GetTraversalParams(
  const GarbageCollector<CatalogTraversalT, HashFilterT>::Configuration &config)
{
  TraversalParameters params;
  params.object_fetcher      = config.object_fetcher;
  params.history             = config.keep_history_depth;
  params.timestamp           = config.keep_history_timestamp;
  params.no_repeat_history   = true;
  params.ignore_load_failure = true;
  params.quiet               = !config.verbose;
  return params;
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::PreserveDataObjects(
  const GarbageCollector<CatalogTraversalT, HashFilterT>::
    TraversalCallbackDataTN &data  // NOLINT(runtime/references)
) {
  ++preserved_catalogs_;

  if (data.catalog->IsRoot()) {
    const uint64_t mtime = use_reflog_timestamps_
      ? catalog_info_shim_.GetLastModified(data.catalog)
      : data.catalog->GetLastModified();
    if (!oldest_trunk_catalog_found_)
      oldest_trunk_catalog_ = std::min(oldest_trunk_catalog_, mtime);
    if (configuration_.verbose) {
      const int    rev   = data.catalog->revision();
      LogCvmfs(kLogGc, kLogStdout, "Preserving Revision %d (%s / added @ %s)",
               rev,
               StringifyTime(data.catalog->GetLastModified(), true).c_str(),
               StringifyTime(catalog_info_shim_.GetLastModified(data.catalog),
                             true).c_str());
      PrintCatalogTreeEntry(data.tree_level, data.catalog);
    }
  }

  // the hash of the actual catalog needs to preserved
  hash_filter_.Fill(data.catalog->hash());

  // all the objects referenced from this catalog need to be preserved
  const HashVector &referenced_hashes = data.catalog->GetReferencedObjects();
        typename HashVector::const_iterator i    = referenced_hashes.begin();
  const typename HashVector::const_iterator iend = referenced_hashes.end();
  for (; i != iend; ++i) {
    hash_filter_.Fill(*i);
  }
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::SweepDataObjects(
  const GarbageCollector<CatalogTraversalT, HashFilterT>::
    TraversalCallbackDataTN &data  // NOLINT(runtime/references)
) {
  ++condemned_catalogs_;

  if (configuration_.verbose) {
    if (data.catalog->IsRoot()) {
      const int    rev   = data.catalog->revision();
      const time_t mtime = static_cast<time_t>(data.catalog->GetLastModified());
      LogCvmfs(kLogGc, kLogStdout, "Sweeping Revision %d (%s)",
                                   rev, StringifyTime(mtime, true).c_str());
    }
    PrintCatalogTreeEntry(data.tree_level, data.catalog);
  }

  // all the objects referenced from this catalog need to be checked against the
  // the preserved hashes in the hash_filter_ and possibly deleted
  const HashVector &referenced_hashes = data.catalog->GetReferencedObjects();
        typename HashVector::const_iterator i    = referenced_hashes.begin();
  const typename HashVector::const_iterator iend = referenced_hashes.end();
  for (; i != iend; ++i) {
    CheckAndSweep(*i);
  }

  // the catalog itself is also condemned and needs to be removed
  CheckAndSweep(data.catalog->hash());
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::CheckAndSweep(
  const shash::Any &hash)
{
  if (!hash_filter_.Contains(hash))
    Sweep(hash);
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::Sweep(
                                                       const shash::Any &hash) {
  ++condemned_objects_;

  LogDeletion(hash);
  if (configuration_.dry_run) {
    return;
  }

  configuration_.uploader->Remove(hash);
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::
  RemoveCatalogFromReflog(const shash::Any &catalog)
{
  assert(catalog.suffix == shash::kSuffixCatalog);
  return (configuration_.dry_run)
    ? true
    : configuration_.reflog->Remove(catalog);
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::Collect() {
  return AnalyzePreservedCatalogTree() &&
         CheckPreservedRevisions()     &&
         SweepReflog();
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::
  AnalyzePreservedCatalogTree()
{
  if (configuration_.verbose)
    LogCvmfs(kLogGc, kLogStdout, "Preserving data objects in latest revision");

  typename CatalogTraversalT::CallbackTN *callback =
    traversal_.RegisterListener(
       &GarbageCollector<CatalogTraversalT, HashFilterT>::PreserveDataObjects,
        this);

  bool success = traversal_.Traverse();
  oldest_trunk_catalog_found_ = true;
  success = success && traversal_.TraverseNamedSnapshots();
  traversal_.UnregisterListener(callback);

  return success;
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::CheckPreservedRevisions()
{
  const bool keeps_revisions = (preserved_catalog_count() > 0);
  if (!keeps_revisions && configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStderr, "This would delete everything! Abort.");
  }

  return keeps_revisions;
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::SweepReflog() {
  if (configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStdout, "Sweeping reference logs");
  }

  const ReflogTN *reflog = configuration_.reflog;
  std::vector<shash::Any> catalogs;
  if (NULL == reflog || !reflog->List(SqlReflog::kRefCatalog, &catalogs)) {
    LogCvmfs(kLogGc, kLogStderr, "Failed to list catalog reference log");
    return false;
  }

  typename CatalogTraversalT::CallbackTN *callback =
    traversal_.RegisterListener(
       &GarbageCollector<CatalogTraversalT, HashFilterT>::SweepDataObjects,
        this);

  bool success = true;
  const typename CatalogTraversalT::TraversalType traversal_type =
                                        CatalogTraversalT::kDepthFirstTraversal;
        std::vector<shash::Any>::const_iterator i    = catalogs.begin();
  const std::vector<shash::Any>::const_iterator iend = catalogs.end();
  for (; i != iend && success; ++i) {
    if (!hash_filter_.Contains(*i)) {
      success =
        success                                         &&
        traversal_.TraverseRevision(*i, traversal_type) &&
        RemoveCatalogFromReflog(*i);
    }
  }

  traversal_.UnregisterListener(callback);

  // TODO(jblomer): turn current counters into perf::Counters
  if (configuration_.statistics) {
    perf::Counter *ctr_preserved_catalogs =
      configuration_.statistics->Register(
        "gc.n_preserved_catalogs", "number of live catalogs");
    perf::Counter *ctr_condemned_catalogs =
      configuration_.statistics->Register(
        "gc.n_condemned_catalogs", "number of dead catalogs");
    perf::Counter *ctr_condemned_objects =
      configuration_.statistics->Register(
        "gc.n_condemned_objects", "number of deleted objects");
    ctr_preserved_catalogs->Set(preserved_catalog_count());
    ctr_condemned_catalogs->Set(condemned_catalog_count());
    ctr_condemned_objects->Set(condemned_objects_count());
  }

  return success;
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::PrintCatalogTreeEntry(
                                              const unsigned int  tree_level,
                                              const CatalogTN    *catalog) const
{
  std::string tree_indent;
  for (unsigned int i = 0; i < tree_level; ++i) {
    tree_indent += "\u2502  ";
  }
  tree_indent += "\u251C\u2500 ";

  const std::string hash_string = catalog->hash().ToString();
  const std::string path =
    (catalog->mountpoint().IsEmpty()) ? "/" : catalog->mountpoint().ToString();

  LogCvmfs(kLogGc, kLogStdout, "%s%s %s",
    tree_indent.c_str(),
    hash_string.c_str(),
    path.c_str());
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::LogDeletion(
                                                 const shash::Any &hash) const {
  if (configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStdout, "Sweep: %s",
                                 hash.ToStringWithSuffix().c_str());
  }

  if (configuration_.has_deletion_log()) {
    const int written = fprintf(configuration_.deleted_objects_logfile,
                                "%s\n", hash.ToStringWithSuffix().c_str());
    if (written < 0) {
      LogCvmfs(kLogGc, kLogStderr, "failed to write to deleted objects log");
    }
  }
}

#endif  // CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_IMPL_H_
