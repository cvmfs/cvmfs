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

#include <inttypes.h>

#include <vector>

#include "catalog_traversal_parallel.h"
#include "garbage_collection/hash_filter.h"
#include "statistics.h"
#include "upload_facility.h"

template<class CatalogTraversalT, class HashFilterT>
class GarbageCollector {
 protected:
  typedef typename CatalogTraversalT::ObjectFetcherTN ObjectFetcherTN;
  typedef typename ObjectFetcherTN::HistoryTN HistoryTN;
  typedef typename ObjectFetcherTN::ReflogTN ReflogTN;
  typedef typename CatalogTraversalT::CatalogTN CatalogTN;
  typedef typename CatalogTraversalT::CallbackDataTN TraversalCallbackDataTN;
  typedef typename CatalogTraversalT::Parameters TraversalParameters;
  typedef std::vector<shash::Any> HashVector;

 public:
  struct Configuration {
    static const uint64_t kFullHistory;
    static const uint64_t kNoHistory;
    static const time_t kNoTimestamp;
    static const shash::Any kLatestHistoryDatabase;

    Configuration()
        : uploader(NULL)
        , object_fetcher(NULL)
        , reflog(NULL)
        , keep_history_depth(kFullHistory)
        , keep_history_timestamp(kNoTimestamp)
        , dry_run(false)
        , verbose(false)
        , deleted_objects_logfile(NULL)
        , statistics(NULL)
        , extended_stats(false)
        , num_threads(8) { }

    bool has_deletion_log() const { return deleted_objects_logfile != NULL; }

    upload::AbstractUploader *uploader;
    ObjectFetcherTN *object_fetcher;
    ReflogTN *reflog;
    uint64_t keep_history_depth;
    time_t keep_history_timestamp;
    bool dry_run;
    bool verbose;
    FILE *deleted_objects_logfile;
    perf::Statistics *statistics;
    bool extended_stats;
    unsigned int num_threads;
  };

 public:
  explicit GarbageCollector(const Configuration &configuration);

  void UseReflogTimestamps();
  bool Collect();

  uint64_t preserved_catalog_count() const { return preserved_catalogs_; }
  uint64_t condemned_catalog_count() const { return condemned_catalogs_; }
  uint64_t condemned_objects_count() const { return condemned_objects_; }
  uint64_t duplicate_delete_requests() const {
    return duplicate_delete_requests_;
  }
  uint64_t condemned_bytes_count() const { return condemned_bytes_; }
  uint64_t oldest_trunk_catalog() const { return oldest_trunk_catalog_; }

 protected:
  TraversalParameters GetTraversalParams(const Configuration &configuration);

  void PreserveDataObjects(const TraversalCallbackDataTN &data);
  void SweepDataObjects(const TraversalCallbackDataTN &data);

  bool AnalyzePreservedCatalogTree();
  bool CheckPreservedRevisions();
  bool SweepReflog();

  void CheckAndSweep(const shash::Any &hash);
  void Sweep(const shash::Any &hash);
  bool RemoveCatalogFromReflog(const shash::Any &catalog);

  void PrintCatalogTreeEntry(const unsigned int tree_level,
                             const CatalogTN *catalog) const;
  void LogDeletion(const shash::Any &hash) const;

 private:
  class ReflogBasedInfoShim
      : public swissknife::CatalogTraversalInfoShim<CatalogTN> {
   public:
    explicit ReflogBasedInfoShim(ReflogTN *reflog) : reflog_(reflog) {
      pthread_mutex_init(&reflog_mutex_, NULL);
    }
    virtual ~ReflogBasedInfoShim() { pthread_mutex_destroy(&reflog_mutex_); }
    virtual uint64_t GetLastModified(const CatalogTN *catalog) {
      uint64_t timestamp;
      MutexLockGuard m(&reflog_mutex_);
      bool retval = reflog_->GetCatalogTimestamp(catalog->hash(), &timestamp);
      return retval ? timestamp : catalog->GetLastModified();
    }

   private:
    ReflogTN *reflog_;
    pthread_mutex_t reflog_mutex_;
  };

  const Configuration configuration_;
  ReflogBasedInfoShim catalog_info_shim_;
  CatalogTraversalT traversal_;
  HashFilterT hash_filter_;
  HashFilterT hash_map_delete_requests_;


  bool use_reflog_timestamps_;
  /**
   * A marker for the garbage collection grace period, the time span that is
   * walked back from the current head catalog.  There can be named snapshots
   * older than this snapshot.  The oldest_trunk_catalog_ is used as a marker
   * for when to remove auxiliary files (meta info, history, ...).
   */
  uint64_t oldest_trunk_catalog_;
  bool oldest_trunk_catalog_found_;
  uint64_t preserved_catalogs_;
  /**
   * Number of catalogs in the reflog that are to be deleted (in fact, some of
   * them might not exist anymore).
   */
  uint64_t unreferenced_trees_;
  /**
   * Number of root catalogs garbage collected, count grows as GC progresses
   */
  uint64_t condemned_trees_;
  /**
   * Number of catalogs garbage collected, count grows as GC progresses
   */
  uint64_t condemned_catalogs_;
  /**
   * Keeps track if the last status report issued, between 0 and 1
   */
  float last_reported_status_;

  uint64_t condemned_objects_;
  uint64_t condemned_bytes_;
  uint64_t duplicate_delete_requests_;
};

#include "garbage_collector_impl.h"

#endif  // CVMFS_GARBAGE_COLLECTION_GARBAGE_COLLECTOR_H_
