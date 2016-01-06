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

#ifndef CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_H_
#define CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_H_

#include <vector>

#include "../upload_facility.h"
#include "hash_filter.h"

template<class CatalogTraversalT, class HashFilterT>
class GarbageCollector {
 protected:
  typedef typename CatalogTraversalT::ObjectFetcherTN ObjectFetcherTN;
  typedef typename ObjectFetcherTN::HistoryTN         HistoryTN;
  typedef typename CatalogTraversalT::CatalogTN       CatalogTN;
  typedef typename CatalogTraversalT::CallbackDataTN  TraversalCallbackDataTN;
  typedef typename CatalogTraversalT::Parameters      TraversalParameters;
  typedef std::vector<shash::Any>                     HashVector;

 public:
  struct Configuration {
    static const unsigned int kFullHistory;
    static const unsigned int kNoHistory;
    static const time_t       kNoTimestamp;
    static const shash::Any   kLatestHistoryDatabase;

    Configuration()
      : uploader(NULL)
      , object_fetcher(NULL)
      , keep_history_depth(kFullHistory)
      , keep_history_timestamp(kNoTimestamp)
      , dry_run(false)
      , verbose(false)
      , deleted_objects_logfile(NULL) {}

    bool has_deletion_log() const { return deleted_objects_logfile != NULL; }

    upload::AbstractUploader  *uploader;
    ObjectFetcherTN           *object_fetcher;
    unsigned int               keep_history_depth;
    time_t                     keep_history_timestamp;
    bool                       dry_run;
    bool                       verbose;
    FILE                      *deleted_objects_logfile;
  };

 public:
  explicit GarbageCollector(const Configuration &configuration);

  bool Collect();

  unsigned int preserved_catalog_count() const { return preserved_catalogs_; }
  unsigned int condemned_catalog_count() const { return condemned_catalogs_; }
  unsigned int condemned_objects_count() const { return condemned_objects_;  }

 protected:
  static TraversalParameters GetTraversalParams(
                                            const Configuration &configuration);

  void PreserveDataObjects(const TraversalCallbackDataTN &data);
  void SweepDataObjects(const TraversalCallbackDataTN &data);

  bool AnalyzePreservedCatalogTree();
  bool CheckPreservedRevisions();
  bool SweepCondemnedCatalogTree();
  bool SweepHistoricRevisions();

  void CheckAndSweep(const shash::Any &hash);
  void Sweep(const shash::Any &hash);

  void PrintCatalogTreeEntry(const unsigned int  tree_level,
                             const CatalogTN    *catalog) const;
  void LogDeletion(const shash::Any &hash) const;

 private:
  const Configuration   configuration_;
  CatalogTraversalT     traversal_;
  HashFilterT           hash_filter_;

  unsigned int          preserved_catalogs_;
  unsigned int          condemned_catalogs_;

  unsigned int          condemned_objects_;
};

#include "garbage_collector_impl.h"

#endif  // CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_H_
