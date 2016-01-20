/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_IMPL_H_
#define CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_IMPL_H_

#include <limits>
#include <string>
#include <vector>

#include "../logging.h"

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
  , traversal_(
      GarbageCollector<CatalogTraversalT, HashFilterT>::GetTraversalParams(
                                                                configuration))
  , hash_filter_()
  , preserved_catalogs_(0)
  , condemned_catalogs_(0)
  , condemned_objects_(0)
{
  assert(configuration_.uploader != NULL);
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

  if (configuration_.verbose) {
    if (data.catalog->IsRoot()) {
      const int    rev   = data.catalog->revision();
      const time_t mtime = static_cast<time_t>(data.catalog->GetLastModified());
      LogCvmfs(kLogGc, kLogStdout, "Preserving Revision %d (%s)",
                                   rev, StringifyTime(mtime, true).c_str());
    }
    PrintCatalogTreeEntry(data.tree_level, data.catalog);
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
bool GarbageCollector<CatalogTraversalT, HashFilterT>::Collect() {
  return AnalyzePreservedCatalogTree()   &&
         CheckPreservedRevisions()       &&
         SweepCondemnedCatalogTree()     &&
         SweepHistoricRevisions();
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

  const bool success = traversal_.Traverse() &&
                       traversal_.TraverseNamedSnapshots();
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
bool GarbageCollector<CatalogTraversalT, HashFilterT>::
  SweepCondemnedCatalogTree()
{
  if (configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStdout, "Sweeping Condemned Catalog Graphs");
  }

  // check if we have anything to sweep in this stage
  const bool no_condemned_revisions = (traversal_.pruned_revision_count() == 0);
  if (no_condemned_revisions) {
    if (configuration_.verbose) {
      LogCvmfs(kLogGc, kLogStdout, "Nothing to be swept.");
    }
    return true;
  }

  // sweep all previously pruned revisions including their history
  typename CatalogTraversalT::CallbackTN *callback =
    traversal_.RegisterListener(
       &GarbageCollector<CatalogTraversalT, HashFilterT>::SweepDataObjects,
        this);

  const bool success = traversal_.TraversePruned(
                                       CatalogTraversalT::kDepthFirstTraversal);
  traversal_.UnregisterListener(callback);
  return success;
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::SweepHistoricRevisions()
{
  if (configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStdout, "Sweeping Historic Snapshots");
  }

  ObjectFetcherTN *fetcher = configuration_.object_fetcher;

  // find the content hash for the current HEAD history database
  UniquePtr<HistoryTN> history;
  const typename ObjectFetcherTN::Failures retval =
    fetcher->FetchHistory(&history);
  switch (retval) {
    case ObjectFetcherTN::kFailOk:
      break;

    case ObjectFetcherTN::kFailNotFound:
      if (configuration_.verbose)
        LogCvmfs(kLogGc, kLogStdout, "No history found");
      return true;

    default:
      return false;
  }

  typename CatalogTraversalT::CallbackTN *callback =
    traversal_.RegisterListener(
       &GarbageCollector<CatalogTraversalT, HashFilterT>::SweepDataObjects,
        this);

  // List the recycle bin of the current HEAD history database for sweeping
  typedef std::vector<shash::Any> Hashes;
  Hashes recycled_snapshots;
  if (!history->ListRecycleBin(&recycled_snapshots)) {
    return false;
  }

  // sweep all revisions that were marked as deleted in the recycle bin
        Hashes::const_iterator i    = recycled_snapshots.begin();
  const Hashes::const_iterator iend = recycled_snapshots.end();
  for (; i != iend; ++i) {
    if (!traversal_.Traverse(*i, CatalogTraversalT::kDepthFirstTraversal)) {
      return false;
    }
  }

  traversal_.UnregisterListener(callback);
  return true;
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
    (catalog->path().IsEmpty()) ? "/" : catalog->path().ToString();

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
