/**
 * This file is part of the CernVM File System.
 */

#include <limits>

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

template<class CatalogTraversalT, class HashFilterT>
const shash::Any GarbageCollector<
                   CatalogTraversalT,
                   HashFilterT>::Configuration::kLatestHistoryDatabase =
  shash::Any();


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
  assert (configuration_.uploader != NULL);
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
  params.quiet               = ! config.verbose;
  return params;
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::PreserveDataObjects(
 const GarbageCollector<CatalogTraversalT,
                        HashFilterT>::TraversalCallbackDataTN &data) {
  ++preserved_catalogs_;

  if (configuration_.verbose) {
    if (data.catalog->IsRoot()) {
      LogCvmfs(kLogGc, kLogStdout, "Preserving Revision %d",
                                   data.catalog->revision());
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
 const GarbageCollector<CatalogTraversalT,
                        HashFilterT>::TraversalCallbackDataTN &data) {
  ++condemned_catalogs_;

  if (configuration_.verbose) {
    if (data.catalog->IsRoot()) {
      LogCvmfs(kLogGc, kLogStdout, "Sweeping Revision %d",
                                   data.catalog->revision());
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
                                                       const shash::Any &hash) {
  if (! hash_filter_.Contains(hash)) {
    Sweep(hash);
  }
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::Sweep(
                                                       const shash::Any &hash) {
  ++condemned_objects_;

  if (configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStdout, "Sweep: %s", hash.ToString().c_str());
  }

  if (configuration_.dry_run) {
    return;
  }

  configuration_.uploader->Remove(hash);
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::Collect() {
  return AnalyzePreservedCatalogTree()   &&
         PreserveLatestHistoryDatabase() &&
         CheckPreservedRevisions()       &&
         SweepCondemnedCatalogTree()     &&
         SweepHistoricRevisions();
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::AnalyzePreservedCatalogTree() {
  if (configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStdout, "Preserving data objects in latest revision");
  }

  typename CatalogTraversalT::CallbackTN *callback =
    traversal_.RegisterListener(
       &GarbageCollector<CatalogTraversalT, HashFilterT>::PreserveDataObjects,
        this);

  const bool success = traversal_.Traverse();
  traversal_.UnregisterListener(callback);

  return success;
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::PreserveLatestHistoryDatabase() {
  if (configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStdout, "Preserving data objects in historic revisions");
  }

  // traverse the latest history database
  typename CatalogTraversalT::CallbackTN *callback =
  traversal_.RegisterListener(
     &GarbageCollector<CatalogTraversalT, HashFilterT>::PreserveDataObjects,
      this);

  const bool success = traversal_.TraverseNamedSnapshots();
  traversal_.UnregisterListener(callback);

  // preserve the latest history database file itself
  ObjectFetcherTN *fetcher = configuration_.object_fetcher;
  UniquePtr<manifest::Manifest> manifest(fetcher->FetchManifest());
  assert (manifest.IsValid());
  hash_filter_.Fill(manifest->history());

  return success;
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::CheckPreservedRevisions() {
  const bool keeps_revisions = (preserved_catalog_count() > 0);
  if (! keeps_revisions && configuration_.verbose) {
    LogCvmfs(kLogGc, kLogStderr, "This would delete everything! Abort.");
  }

  return keeps_revisions;
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::SweepCondemnedCatalogTree() {
  typedef std::set<shash::Any> RecycledCatalogs;
  RecycledCatalogs snapshots_to_recycle;
  if (! GetHistoryRecycleBinContents(&snapshots_to_recycle)) {
    return false;
  }

  const bool has_condemned_revisions = (traversal_.pruned_revision_count() > 0 ||
                                        snapshots_to_recycle.size()        > 0);
  if (configuration_.verbose) {
    if (! has_condemned_revisions) {
      LogCvmfs(kLogGc, kLogStdout, "Nothing to be sweeped.");

    } else {
      LogCvmfs(kLogGc, kLogStdout, "Sweeping unreferenced data objects in "
                                   "remaining catalogs");
    }
  }

  if (! has_condemned_revisions) {
    return true;
  }

  typename CatalogTraversalT::CallbackTN *callback =
    traversal_.RegisterListener(
       &GarbageCollector<CatalogTraversalT, HashFilterT>::SweepDataObjects,
        this);

  bool success = false;

  success = traversal_.TraversePruned(CatalogTraversalT::kDepthFirstTraversal);
        RecycledCatalogs::const_iterator i    = snapshots_to_recycle.begin();
  const RecycledCatalogs::const_iterator iend = snapshots_to_recycle.end();
  for (; success && i != iend; ++i) {
    success = traversal_.Traverse(*i, CatalogTraversalT::kDepthFirstTraversal);
  }

  traversal_.UnregisterListener(callback);

  return success;
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::SweepHistoricRevisions() {

  return true;
}


template <class CatalogTraversalT, class HashFilterT>
bool
GarbageCollector<CatalogTraversalT, HashFilterT>::GetHistoryRecycleBinContents(
                                       std::set<shash::Any> *result_set) const {
  assert (result_set != NULL);
  result_set->clear();

  ObjectFetcherTN *fetcher = configuration_.object_fetcher;
  const shash::Any &base_hash = configuration_.base_history_database;

  // fetch the latest history database
  unsigned int history_db_depth = 0;
  UniquePtr<history::History> history(fetcher->FetchHistory());
  if (! history) {
    if (configuration_.verbose) {
      LogCvmfs(kLogGc, kLogStdout, "No recycle bin and/or history found");
    }
    return true;
  }

  while (true) {
    if (configuration_.verbose) {
      LogCvmfs(kLogGc, kLogStdout, "Looking for deleted named snapshots (%d)",
               history_db_depth);
    }

    std::vector<shash::Any> recycle_hashes;
    if (! history->ListRecycleBin(&recycle_hashes)) {
      return false;
    }

    result_set->insert(recycle_hashes.begin(), recycle_hashes.end());
    const shash::Any previous_hash = history->previous_revision();

    if (base_hash.IsNull()     ||
        previous_hash.IsNull() ||
        previous_hash == base_hash) {
      break;
    }

    history = fetcher->FetchHistory(previous_hash);
    if (! history) {
      LogCvmfs(kLogGc, kLogStderr, "Failed to fetch previous history (%s)",
               previous_hash.ToString().c_str());
      return false;
    }
    ++history_db_depth;
  }

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
  const std::string path        = (catalog->path().IsEmpty())
                                          ? "/"
                                          : catalog->path().ToString();

  LogCvmfs(kLogGc, kLogStdout, "%s%s %s",
    tree_indent.c_str(),
    hash_string.c_str(),
    path.c_str());
}
