/**
 * This file is part of the CernVM File System.
 */

#include "../logging.h"

#include <iostream> // TODO: remove me!

template <class CatalogTraversalT, class HashFilterT>
swissknife::CatalogTraversalParams
  GarbageCollector<CatalogTraversalT, HashFilterT>::GetTraversalParams(
  const GarbageCollector<CatalogTraversalT, HashFilterT>::Configuration &config)
{
  swissknife::CatalogTraversalParams params;
  params.history           = config.keep_history_depth;
  params.no_repeat_history = true;
  params.repo_url          = config.repo_url;
  params.repo_name         = config.repo_name;
  params.repo_keys         = config.repo_keys;
  params.tmp_dir           = config.tmp_dir;
  return params;
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::PreserveDataObjects(
 const GarbageCollector<CatalogTraversalT, HashFilterT>::MyCallbackData &data) {
  ++preserved_catalogs_;

  if (configuration_.verbose) {
    if (data.catalog->IsRoot()) {
      LogCvmfs(kLogGC, kLogStdout, "Preserving Revision %d",
                                   data.catalog->revision());
    }
    PrintCatalogTreeEntry(data.tree_level, data.catalog);
  }

  // the hash of the actual catalog needs to preserved
  hash_filter_.Fill(data.catalog->catalog_hash());

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
 const GarbageCollector<CatalogTraversalT, HashFilterT>::MyCallbackData &data) {
  ++condemned_catalogs_;

  if (configuration_.verbose) {
    if (data.catalog->IsRoot()) {
      LogCvmfs(kLogGC, kLogStdout, "Sweeping Revision %d",
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
  CheckAndSweep(data.catalog->catalog_hash());
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
    LogCvmfs(kLogGC, kLogStdout, "Sweep: %s", hash.ToString().c_str());
  }

  if (configuration_.dry_run) {
    return;
  }

  configuration_.uploader->Remove(hash);
}


template <class CatalogTraversalT, class HashFilterT>
bool GarbageCollector<CatalogTraversalT, HashFilterT>::VerifyConfiguration() const {
  const Configuration &cfg = configuration_;

  if (cfg.uploader == NULL) {
    return false;
  }

  if (cfg.repo_url.empty()) {
    LogCvmfs(kLogGC, kLogDebug, "No repository url provided");
    return false;
  }

  if (cfg.repo_name.empty()) {
    LogCvmfs(kLogGC, kLogDebug, "No repository name provided");
    return false;
  }

  if (cfg.tmp_dir.empty() || ! DirectoryExists(cfg.tmp_dir)) {
    LogCvmfs(kLogGC, kLogDebug, "Temporary directory '%s' doesn't exist",
                                cfg.tmp_dir.c_str());
    return false;
  }

  return true;
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::Collect() {
  const bool config_okay = VerifyConfiguration();
  if (! config_okay) {
    LogCvmfs(kLogGC, kLogStderr, "Malformed GarbageCollector configuration.");
    return;
  }

  AnalyzePreservedCatalogTree();
  SweepCondemnedCatalogTree();
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::AnalyzePreservedCatalogTree() {
  assert (state_ == kPreserve);

  if (configuration_.verbose) {
    LogCvmfs(kLogGC, kLogStdout, "Preserving data objects in catalog tree");
  }

  typename CatalogTraversalT::callback_t *callback =
    traversal_.RegisterListener(
       &GarbageCollector<CatalogTraversalT, HashFilterT>::PreserveDataObjects,
        this);
  traversal_.Traverse();
  traversal_.UnregisterListener(callback);
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::SweepCondemnedCatalogTree() {
  state_ = kSweep;

  if (configuration_.verbose) {
    LogCvmfs(kLogGC, kLogStdout, "Sweeping unreferenced data objects in "
                                 "remaining catalogs");
  }

  typename CatalogTraversalT::callback_t *callback =
    traversal_.RegisterListener(
       &GarbageCollector<CatalogTraversalT, HashFilterT>::SweepDataObjects,
        this);
  traversal_.TraversePruned();
  traversal_.UnregisterListener(callback);
}


template <class CatalogTraversalT, class HashFilterT>
void GarbageCollector<CatalogTraversalT, HashFilterT>::PrintCatalogTreeEntry(
                                              const unsigned int  tree_level,
                                              const MyCatalog    *catalog) const
{
  std::string tree_indent;
  for (unsigned int i = 0; i < tree_level; ++i) {
    tree_indent += "\u2502  ";
  }
  tree_indent += "\u251C\u2500 ";

  const std::string hash_string = catalog->catalog_hash().ToString();
  const std::string path        = (catalog->path().IsEmpty())
                                          ? "/"
                                          : catalog->path().ToString();

  LogCvmfs(kLogGC, kLogStdout, "%s%s %s",
    tree_indent.c_str(),
    hash_string.c_str(),
    path.c_str());
}
