/**
 * This file is part of the CernVM File System.
 *
 * The GarbageCollector class is figuring out which data objects (represented by
 * their content hashes) can be deleted as outdated garbage.
 * Garbage collection is performed on the granularity of catalog revisions, thus
 * a complete repository revision is either considered to be outdated or active.
 * This way, a mountable repository revision stays completely usable (no nested
 * catalogs or data objects become unavailable). A revision is defined by it's
 * root catalog; the revision numbers of nested catalogs are irrelevant, since
 * they might be referenced by newer (preserved) repository revisions.
 * Thus, garbage objects are those that are _not_ referenced by any of the pre-
 * served root catalogs or their direct subordinate nested catalog try.
 *
 * We use a two-stage approach:
 *
 *   1st Stage - Initialization
 *               The GarbageCollector is reading all the catalogs that are meant
 *               to be preserved. It builds up a filter (HashFilterT) containing
 *               all content hashes that are _not_ to be deleted
 *
 *   2nd Stage - Sweeping
 *               The initialized HashFilterT is presented with all content
 *               hashes found in condemned catalogs and decides if they are
 *               referenced by the preserved catalog revisions or not.
 *
 * The GarbageCollector is templated with CatalogTraversalT mainly for
 * testability and with HashFilterT as an instance of the Strategy Pattern to
 * abstract from the actual hash filtering method to be used.
 */

#ifndef CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_H
#define CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_H

#include "hash_filter.h"

#include "../upload_facility.h"

template<class CatalogTraversalT, class HashFilterT>
class GarbageCollector {
 public:
  struct Configuration {
    Configuration() : uploader(NULL), keep_history_depth(1),
                      keep_named_snapshots(true), dry_run(false),
                      verbose(false) {}

    upload::AbstractUploader  *uploader;
    unsigned int               keep_history_depth;
    bool                       keep_named_snapshots;
    bool                       dry_run;
    bool                       verbose;
    std::string                repo_url;
    std::string                repo_name;
    std::string                repo_keys;
    std::string                tmp_dir;
  };

 protected:
  typedef typename CatalogTraversalT::Catalog          MyCatalog;
  typedef typename CatalogTraversalT::CallbackData     MyCallbackData;
  typedef std::vector<shash::Any>                      HashVector;

 public:
  GarbageCollector(const Configuration &configuration) :
    configuration_(configuration),
    traversal_(
      GarbageCollector<CatalogTraversalT, HashFilterT>::GetTraversalParams(
                                                                configuration)),
    hash_filter_(),
    preserved_catalogs_(0), condemned_catalogs_(0), condemned_objects_(0) {}

  bool Collect();

  unsigned int preserved_catalog_count() const { return preserved_catalogs_; }
  unsigned int condemned_catalog_count() const { return condemned_catalogs_; }
  unsigned int condemned_objects_count() const { return condemned_objects_;  }

 protected:
  static swissknife::CatalogTraversalParams GetTraversalParams(
                                            const Configuration &configuration);

  void PreserveDataObjects(const MyCallbackData &data);
  void SweepDataObjects   (const MyCallbackData &data);

  bool VerifyConfiguration() const;
  bool AnalyzePreservedCatalogTree();
  bool SweepCondemnedCatalogTree();

  void CheckAndSweep(const shash::Any &hash);
  void Sweep(const shash::Any &hash);

  void PrintCatalogTreeEntry(const unsigned int  tree_level,
                             const MyCatalog    *catalog) const;

 private:
  const Configuration   configuration_;
  CatalogTraversalT     traversal_;
  HashFilterT           hash_filter_;

  unsigned int          preserved_catalogs_;
  unsigned int          condemned_catalogs_;

  unsigned int          condemned_objects_;
};

#include "garbage_collector_impl.h"

#endif /* CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_H */
