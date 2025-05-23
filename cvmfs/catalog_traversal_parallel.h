/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_TRAVERSAL_PARALLEL_H_
#define CVMFS_CATALOG_TRAVERSAL_PARALLEL_H_

#include <stack>
#include <string>
#include <vector>

#include "catalog_traversal.h"
#include "util/atomic.h"
#include "util/exception.h"
#include "util/tube.h"

namespace swissknife {

/**
 * This class implements the same functionality as CatalogTraversal, but in
 * parallel.  For common functionality, see the documentation of
 * CatalogTraversal. Differences:
 *  - can choose number of threads
 *  - traversal types change meaning:
 *    - depth-first -> parallelized post-order traversal (parents are processed
 *                     after all children are finished)
 *    - breadth-first -> same as original, but parallelized
 */
template<class ObjectFetcherT>
class CatalogTraversalParallel : public CatalogTraversalBase<ObjectFetcherT> {
 public:
  typedef CatalogTraversalBase<ObjectFetcherT> Base;
  typedef ObjectFetcherT ObjectFetcherTN;
  typedef typename ObjectFetcherT::CatalogTN CatalogTN;
  typedef typename ObjectFetcherT::HistoryTN HistoryTN;
  typedef CatalogTraversalData<CatalogTN> CallbackDataTN;
  typedef typename CatalogTN::NestedCatalogList NestedCatalogList;
  typedef typename Base::Parameters Parameters;
  typedef typename Base::TraversalType TraversalType;
  typedef std::vector<shash::Any> HashList;

  explicit CatalogTraversalParallel(const Parameters &params)
      : CatalogTraversalBase<ObjectFetcherT>(params)
      , num_threads_(params.num_threads)
      , serialize_callbacks_(params.serialize_callbacks) {
    atomic_init32(&num_errors_);
    shash::Any null_hash;
    null_hash.SetNull();
    catalogs_processing_.Init(1024, null_hash, hasher);
    catalogs_done_.Init(1024, null_hash, hasher);
    pthread_mutex_init(&catalog_callback_lock_, NULL);
    pthread_mutex_init(&catalogs_lock_, NULL);
    effective_history_depth_ = this->default_history_depth_;
    effective_timestamp_threshold_ = this->default_timestamp_threshold_;
  }

 protected:
  struct CatalogJob : public CatalogTraversal<ObjectFetcherT>::CatalogJob,
                      public Observable<int> {
    explicit CatalogJob(const std::string &path,
                        const shash::Any &hash,
                        const unsigned tree_level,
                        const uint64_t history_depth,
                        CatalogTN *parent = NULL)
        : CatalogTraversal<ObjectFetcherT>::CatalogJob(path, hash, tree_level,
                                                       history_depth, parent) {
      atomic_init32(&children_unprocessed);
    }

    void WakeParents() { this->NotifyListeners(0); }

    atomic_int32 children_unprocessed;
  };

 public:
  /**
   * Starts the traversal process.
   * After calling this methods CatalogTraversal will go through all catalogs
   * and call the registered callback methods for each found catalog.
   * If something goes wrong in the process, the traversal will be cancelled.
   *
   * @return       true, when all catalogs were successfully processed. On
   *               failure the traversal is cancelled and false is returned.
   */
  bool Traverse(const TraversalType type = Base::kBreadthFirst) {
    const shash::Any root_catalog_hash = this->GetRepositoryRootCatalogHash();
    if (root_catalog_hash.IsNull()) {
      return false;
    }
    return Traverse(root_catalog_hash, type);
  }

  /**
   * Starts the traversal process at the catalog pointed to by the given hash
   *
   * @param root_catalog_hash  the entry point into the catalog traversal
   * @return                   true when catalogs were successfully traversed
   */
  bool Traverse(const shash::Any &root_catalog_hash,
                const TraversalType type = Base::kBreadthFirst) {
    // add the root catalog of the repository as the first element on the job
    // stack
    if (this->no_repeat_history_
        && catalogs_done_.Contains(root_catalog_hash)) {
      return true;
    }
    effective_traversal_type_ = type;
    CatalogJob *root_job = new CatalogJob("", root_catalog_hash, 0, 0);
    PushJob(root_job);
    return DoTraverse();
  }

  /**
   * Start the traversal process from a list of root catalogs. Same as
   * TraverseRevision function, TraverseList does not traverse into predecessor
   * catalog revisions and ignores TraversalParameter settings.
   */
  bool TraverseList(const HashList &root_catalog_list,
                    const TraversalType type = Base::kBreadthFirst) {
    // Push in reverse order for CatalogTraversal-like behavior
    HashList::const_reverse_iterator i = root_catalog_list.rbegin();
    const HashList::const_reverse_iterator iend = root_catalog_list.rend();
    bool has_pushed = false;
    {
      MutexLockGuard m(&catalogs_lock_);
      for (; i != iend; ++i) {
        if (this->no_repeat_history_ && catalogs_done_.Contains(*i)) {
          continue;
        }

        CatalogJob *root_job = new CatalogJob("", *i, 0, 0);
        PushJobUnlocked(root_job);
        has_pushed = true;
      }
    }
    // noop: no catalogs to traverse
    if (!has_pushed) {
      return true;
    }
    effective_traversal_type_ = type;
    effective_history_depth_ = Parameters::kNoHistory;
    effective_timestamp_threshold_ = Parameters::kNoTimestampThreshold;
    bool result = DoTraverse();
    effective_history_depth_ = this->default_history_depth_;
    effective_timestamp_threshold_ = this->default_timestamp_threshold_;
    return result;
  }

  /**
   * Starts the traversal process at the catalog pointed to by the given hash
   * but doesn't traverse into predecessor catalog revisions. This overrides the
   * TraversalParameter settings provided at construction.
   *
   * @param root_catalog_hash  the entry point into the catalog traversal
   * @return                   true when catalogs were successfully traversed
   */
  bool TraverseRevision(const shash::Any &root_catalog_hash,
                        const TraversalType type = Base::kBreadthFirst) {
    effective_history_depth_ = Parameters::kNoHistory;
    effective_timestamp_threshold_ = Parameters::kNoTimestampThreshold;
    bool result = Traverse(root_catalog_hash, type);
    effective_history_depth_ = this->default_history_depth_;
    effective_timestamp_threshold_ = this->default_timestamp_threshold_;
    return result;
  }

 protected:
  static uint32_t hasher(const shash::Any &key) {
    // Don't start with the first bytes, because == is using them as well
    return (uint32_t) * (reinterpret_cast<const uint32_t *>(key.digest) + 1);
  }

  bool DoTraverse() {
    // Optimal number of threads is yet to be determined. The main event loop
    // contains a spin-lock, so it should not be more than number of cores.
    threads_process_ = reinterpret_cast<pthread_t *>(
        smalloc(sizeof(pthread_t) * num_threads_));
    for (unsigned int i = 0; i < num_threads_; ++i) {
      int retval = pthread_create(&threads_process_[i], NULL, MainProcessQueue,
                                  this);
      if (retval != 0)
        PANIC(kLogStderr, "failed to create thread");
    }

    for (unsigned int i = 0; i < num_threads_; ++i) {
      int retval = pthread_join(threads_process_[i], NULL);
      assert(retval == 0);
    }
    free(threads_process_);

    if (atomic_read32(&num_errors_))
      return false;

    assert(catalogs_processing_.size() == 0);
    assert(pre_job_queue_.IsEmpty());
    assert(post_job_queue_.IsEmpty());
    return true;
  }

  static void *MainProcessQueue(void *data) {
    CatalogTraversalParallel<ObjectFetcherT> *traversal = reinterpret_cast<
        CatalogTraversalParallel<ObjectFetcherT> *>(data);
    CatalogJob *current_job;
    while (true) {
      current_job = traversal->post_job_queue_.TryPopFront();
      if (current_job != NULL) {
        traversal->ProcessJobPost(current_job);
      } else {
        current_job = traversal->pre_job_queue_.PopFront();
        // NULL means the master thread tells us to finish
        if (current_job->hash.IsNull()) {
          delete current_job;
          break;
        }
        traversal->ProcessJobPre(current_job);
      }
    }
    return NULL;
  }

  void NotifyFinished() {
    shash::Any null_hash;
    null_hash.SetNull();
    for (unsigned i = 0; i < num_threads_; ++i) {
      CatalogJob *job = new CatalogJob("", null_hash, 0, 0);
      pre_job_queue_.EnqueueFront(job);
    }
  }

  void PushJob(CatalogJob *job) {
    MutexLockGuard m(&catalogs_lock_);
    PushJobUnlocked(job);
  }

  void PushJobUnlocked(CatalogJob *job) {
    catalogs_processing_.Insert(job->hash, job);
    pre_job_queue_.EnqueueFront(job);
  }

  void ProcessJobPre(CatalogJob *job) {
    if (!this->PrepareCatalog(job)) {
      atomic_inc32(&num_errors_);
      NotifyFinished();
      return;
    }
    if (job->ignore) {
      FinalizeJob(job);
      return;
    }
    NestedCatalogList catalog_list = job->catalog->ListOwnNestedCatalogs();
    unsigned int num_children;
    // Ensure that pushed children won't call ProcessJobPost on this job
    // before this function finishes
    {
      MutexLockGuard m(&catalogs_lock_);
      if (effective_traversal_type_ == Base::kBreadthFirst) {
        num_children = PushPreviousRevision(job)
                       + PushNestedCatalogs(job, catalog_list);
      } else {
        num_children = PushNestedCatalogs(job, catalog_list)
                       + PushPreviousRevision(job);
        atomic_write32(&job->children_unprocessed, num_children);
      }
      if (!this->CloseCatalog(false, job)) {
        atomic_inc32(&num_errors_);
        NotifyFinished();
      }
    }

    // breadth-first: can post-process immediately
    // depth-first: no children -> can post-process immediately
    if (effective_traversal_type_ == Base::kBreadthFirst || num_children == 0) {
      ProcessJobPost(job);
      return;
    }
  }

  unsigned int PushNestedCatalogs(CatalogJob *job,
                                  const NestedCatalogList &catalog_list) {
    typename NestedCatalogList::const_iterator i = catalog_list.begin();
    typename NestedCatalogList::const_iterator iend = catalog_list.end();
    unsigned int num_children = 0;
    for (; i != iend; ++i) {
      if (this->no_repeat_history_ && catalogs_done_.Contains(i->hash)) {
        continue;
      }

      CatalogJob *child;
      if (!this->no_repeat_history_
          || !catalogs_processing_.Lookup(i->hash, &child)) {
        CatalogTN *parent = (this->no_close_) ? job->catalog : NULL;
        child = new CatalogJob(i->mountpoint.ToString(),
                               i->hash,
                               job->tree_level + 1,
                               job->history_depth,
                               parent);
        PushJobUnlocked(child);
      }

      if (effective_traversal_type_ == Base::kDepthFirst) {
        child->RegisterListener(&CatalogTraversalParallel::OnChildFinished,
                                this, job);
      }
      ++num_children;
    }
    return num_children;
  }

  /**
   * Pushes the previous revision of a root catalog.
   * @return  the number of catalogs pushed on the processing stack
   */
  unsigned int PushPreviousRevision(CatalogJob *job) {
    // only root catalogs are used for entering a previous revision (graph)
    if (!job->catalog->IsRoot()) {
      return 0;
    }

    const shash::Any previous_revision = job->catalog->GetPreviousRevision();
    if (previous_revision.IsNull()) {
      return 0;
    }

    // check if the next deeper history level is actually requested
    if (this->IsBelowPruningThresholds(*job, effective_history_depth_,
                                       effective_timestamp_threshold_)) {
      return 0;
    }

    if (this->no_repeat_history_
        && catalogs_done_.Contains(previous_revision)) {
      return 0;
    }

    CatalogJob *prev_job;
    if (!this->no_repeat_history_
        || !catalogs_processing_.Lookup(previous_revision, &prev_job)) {
      prev_job = new CatalogJob("", previous_revision, 0,
                                job->history_depth + 1);
      PushJobUnlocked(prev_job);
    }

    if (effective_traversal_type_ == Base::kDepthFirst) {
      prev_job->RegisterListener(&CatalogTraversalParallel::OnChildFinished,
                                 this, job);
    }
    return 1;
  }

  void ProcessJobPost(CatalogJob *job) {
    // Save time by keeping catalog open when suitable
    if (job->catalog == NULL) {
      if (!this->ReopenCatalog(job)) {
        atomic_inc32(&num_errors_);
        NotifyFinished();
        return;
      }
    }
    if (serialize_callbacks_) {
      MutexLockGuard m(&catalog_callback_lock_);
      this->NotifyListeners(job->GetCallbackData());
    } else {
      this->NotifyListeners(job->GetCallbackData());
    }
    if (!this->no_close_) {
      if (!this->CloseCatalog(true, job)) {
        atomic_inc32(&num_errors_);
        NotifyFinished();
        return;
      }
    }
    FinalizeJob(job);
  }

  void FinalizeJob(CatalogJob *job) {
    {
      MutexLockGuard m(&catalogs_lock_);
      catalogs_processing_.Erase(job->hash);
      catalogs_done_.Insert(job->hash, true);
      // No more catalogs to process -> finish
      if (catalogs_processing_.size() == 0 && pre_job_queue_.IsEmpty()
          && post_job_queue_.IsEmpty()) {
        NotifyFinished();
      }
    }
    if (effective_traversal_type_ == Base::kDepthFirst) {
      job->WakeParents();
    }
    delete job;
  }

  void OnChildFinished(const int &a, CatalogJob *job) {
    // atomic_xadd32 returns value before subtraction -> needs to equal 1
    if (atomic_xadd32(&job->children_unprocessed, -1) == 1) {
      post_job_queue_.EnqueueFront(job);
    }
  }

  unsigned int num_threads_;
  bool serialize_callbacks_;

  uint64_t effective_history_depth_;
  time_t effective_timestamp_threshold_;
  TraversalType effective_traversal_type_;

  pthread_t *threads_process_;
  atomic_int32 num_errors_;

  Tube<CatalogJob> pre_job_queue_;
  Tube<CatalogJob> post_job_queue_;
  SmallHashDynamic<shash::Any, CatalogJob *> catalogs_processing_;
  SmallHashDynamic<shash::Any, bool> catalogs_done_;
  pthread_mutex_t catalogs_lock_;

  pthread_mutex_t catalog_callback_lock_;
};

}  // namespace swissknife

#endif  // CVMFS_CATALOG_TRAVERSAL_PARALLEL_H_
