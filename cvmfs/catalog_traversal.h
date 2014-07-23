/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_TRAVERSAL_H_
#define CVMFS_CATALOG_TRAVERSAL_H_

#include <string>
#include <stack>

#include "catalog.h"
#include "util.h"
#include "util_concurrency.h"
#include "download.h"
#include "logging.h"
#include "compression.h"

#include "manifest.h"
#include "manifest_fetch.h"
#include "signature.h"
#include "history.h"


namespace catalog {
  class Catalog;
  class WritableCatalog;
}

namespace swissknife {

/**
 * Callback data which has to be implemented by the registered callback
 * functions/methods (see Observable<> for further details)
 * @param catalog             the catalog object which needs to be processed
 * @param catalog_hash        the content hash of the catalog
 * @param tree_level          the depth in the nested catalog tree
 *                            (starting at zero)
 * @param history_depth       the distance from the current HEAD revision
 *                            (current HEAD has history_depth 0)
 */
template <class CatalogT>
struct CatalogTraversalData {
  CatalogTraversalData(const CatalogT     *catalog,
                       const shash::Any   &catalog_hash,
                       const unsigned      tree_level,
                       const size_t        file_size,
                       const unsigned int  history_depth) :
    catalog(catalog), catalog_hash(catalog_hash), tree_level(tree_level),
    file_size(file_size),
    history_depth(history_depth) {}

  const CatalogT     *catalog;
  const shash::Any    catalog_hash;
  const unsigned int  tree_level;
  const size_t        file_size;
  const unsigned int  history_depth;
};


/**
 * @param repo_url             the path to the repository to be traversed:
 *                             -> either absolute path to the local catalogs
 *                             -> or an URL to a remote repository
 * @param repo_name            fully qualified repository name (used for remote
 *                             repository signature check) (optional)
 * @param repo_keys            a comma separated list of public key file
 *                             locations to verify the repository manifest file
 * @param history              depth of the desired catalog history traversal
 *                             (default: 0 - only HEAD catalogs are traversed)
 * @param no_repeat_history    keep track of visited catalogs and don't re-visit
 *                             them in previous revisions
 * @param no_close             do not close catalogs after they were attached
 *                             (catalogs retain their parent/child pointers)
 * @param ignore_load_failure  suppressed an error message if a revision's root
 *                             catalog could not be loaded (i.e. was sweeped
 *                             before by a garbage collection run)
 * @param quiet                silence messages that would go to stderr
 * @param tmp_dir              path to the temporary directory to be used
 *                             (default: /tmp)
 */
struct CatalogTraversalParams {
  CatalogTraversalParams() : history(0), no_repeat_history(false),
  no_close(false), ignore_load_failure(false), quiet(false), tmp_dir("/tmp") {}

  static const unsigned int kFullHistory;

  std::string   repo_url;
  std::string   repo_name;
  std::string   repo_keys;
  unsigned int  history;
  bool          no_repeat_history;
  bool          no_close;
  bool          ignore_load_failure;
  bool          quiet;
  std::string   tmp_dir;
};


class ObjectFetcher;

/**
 * This class traverses the catalog hierarchy of a CVMFS repository recursively.
 * Also historic catalog trees can be traversed. The user needs to specify a
 * callback which is called for each catalog on the way.
 *
 * CatalogTraversal<> can be configured and used in various ways:
 *   -> Historic catalog traversal
 *   -> Never traverse a certain catalog twice
 *   -> Breadth First Traversal or Depth First Traversal
 *   -> Optional catalog memory management (no_close)
 *   -> Use all Named Snapshots of a repository as traversal entry point
 *   -> Traverse starting from a provided catalog
 *   -> Traverse catalogs that were previously skipped
 *
 * Breadth First Traversal Strategy
 *   Catalogs are handed out to the user identical as they are traversed.
 *   Say: From top to buttom. When you would simply print each received catalog
 *        the result would be a nice representation of the catalog tree.
 *   This method is more efficient, because catalogs are opened, processed and
 *   thrown away directly afterwards.
 *
 * Depth First Traversal Strategy
 *   The user gets the catalog tree starting from the leaf nodes.
 *   Say: From bottom to top. A user can assume that he got all children or
 *        historical ancestors of a catalog before.
 *   This method climbs down the full catalog tree and hands it out 'in reverse
 *   order'. Thus, catalogs on the way are opened, checked for their descendants
 *   and closed. Once all children and historical ancestors are processed, it is
 *   re-opened and handed out to the user.
 *   Note: This method needs more disk space to temporarily storage downloaded
 *         but not yet processed catalogs.
 *
 * Note: Since all CVMFS catalog files together can grow to several gigabytes in
 *       file size, each catalog is loaded, processed and removed immediately
 *       afterwards. Except if no_close is specified, which allows the user to
 *       choose when a catalog should be closed.
 *
 * @param CatalogT        the catalog class that should be used for traversal
 *                        usually this will be either catalog::Catalog or
 *                        catalog::WritableCatalog
 *                        (please consider to use the typedef'ed versions of
 *                        this template i.e. [Readable|Writable]CatalogTraversal)
 *
 * @param ObjectFetcherT  Strategy Pattern implementation that is supposed to be
 *                        used mainly for unit-testability. Normal usage should
 *                        not bring up the need to actually set this to anything
 *                        different than the default value (see below)
 *
 * CAUTION: the Catalog* pointer passed into the callback becomes invalid
 *          directly after the callback method returns, unless you create the
 *          CatalogTraversal object with no_close = true.
 */
template<class CatalogT, class ObjectFetcherT = ObjectFetcher>
class CatalogTraversal : public Observable<CatalogTraversalData<CatalogT> > {
 public:
  typedef CatalogTraversalData<CatalogT> CallbackData;
  typedef CatalogT                       Catalog;

 public:
  enum TraversalType {
    kBreadthFirstTraversal,
    kDepthFirstTraversal
  };

 protected:
  typedef std::set<shash::Any>           HashSet;

 protected:
  /**
   * This struct keeps information about a catalog that still needs to be
   * traversed by a currently running catalog traversal process.
   */
  struct CatalogJob {
    CatalogJob(const std::string  &path,
               const shash::Any   &hash,
               const unsigned      tree_level,
               const unsigned      history_depth,
                     CatalogT     *parent = NULL) :
      path(path),
      hash(hash),
      tree_level(tree_level),
      history_depth(history_depth),
      parent(parent),
      catalog_file_size(0),
      ignore(false),
      catalog(NULL),
      referenced_catalogs(0),
      postponed(false) {}

    bool IsRootCatalog() const { return tree_level == 0; }

    CallbackData GetCallbackData() const {
      return CallbackData(catalog, hash, tree_level,
                          catalog_file_size, history_depth);
    }

    // initial state description
    const std::string   path;
    const shash::Any    hash;
    const unsigned      tree_level;
    const unsigned      history_depth;
          CatalogT     *parent;

    // dynamic processing state (used internally)
    std::string   catalog_file_path;
    size_t        catalog_file_size;
    bool          ignore;
    CatalogT     *catalog;
    unsigned int  referenced_catalogs;
    bool          postponed;
  };

  typedef std::stack<CatalogJob> CatalogJobStack;

  /**
   * This struct represents a catalog traversal context. It needs to be re-
   * created for each catalog traversal process and contains information to this
   * specific catalog traversal run.
   *
   * @param history_depth   the history traversal threshold
   * @param traversal_type  either breadth or depth first traversal strategy
   * @param catalog_stack   the call stack for catalogs to be traversed
   * @param callback_stack  used in depth first traversal for deferred yielding
   */
  struct TraversalContext {
    TraversalContext(const unsigned       history_depth,
                     const TraversalType  traversal_type) :
      history_depth(history_depth),
      traversal_type(traversal_type) {}

    const unsigned       history_depth;
    const TraversalType  traversal_type;
    CatalogJobStack      catalog_stack;
    CatalogJobStack      callback_stack;
  };

 public:
  /**
   * Constructs a new catalog traversal engine based on the construction
   * parameters described in struct ConstructionParams.
   */
	CatalogTraversal(const CatalogTraversalParams &params) :
    object_fetcher_(params),
    repo_name_(params.repo_name),
    no_close_(params.no_close),
    ignore_load_failure_(params.ignore_load_failure),
    no_repeat_history_(params.no_repeat_history),
    default_history_depth_(params.history),
    error_sink_((params.quiet) ? kLogDebug : kLogStderr)
  {}


  /**
   * Starts the traversal process.
   * After calling this methods CatalogTraversal will go through all catalogs
   * and call the registered callback methods for each found catalog.
   * If something goes wrong in the process, the traversal will be cancelled.
   *
   * @param type   breadths or depth first traversal
   * @return       true, when all catalogs were successfully processed. On
   *               failure the traversal is cancelled and false is returned.
   */
  bool Traverse(const TraversalType type = kBreadthFirstTraversal) {
    TraversalContext ctx(default_history_depth_, type);
    const shash::Any root_catalog_hash = GetRepositoryRootCatalogHash();
    if (root_catalog_hash.IsNull()) {
      return false;
    }
    Push(ctx, root_catalog_hash);
    return DoTraverse(ctx);
  }

  /**
   * Starts the traversal process at the catalog pointed to by the given hash
   *
   * @param root_catalog_hash  the entry point into the catalog traversal
   * @param type               breadths or depth first traversal
   * @return                   true when catalogs were successfully traversed
   */
  bool Traverse(const shash::Any     &root_catalog_hash,
                const TraversalType   type = kBreadthFirstTraversal) {
    // add the root catalog of the repository as the first element on the job
    // stack
    TraversalContext ctx(default_history_depth_, type);
    Push(ctx, root_catalog_hash);
    return DoTraverse(ctx);
  }

  /**
   * Figures out all named tags in a repository and uses all of them as entry
   * points into the traversal process. Traversing is done as configured from
   * each of the entry points.
   *
   * @param type  breadths or depth first traversal
   * @return      true when catalog traversal successfully finished
   */
  bool TraverseNamedSnapshots(const TraversalType type = kBreadthFirstTraversal) {
    typedef std::vector<shash::Any> HashList;

    TraversalContext ctx(default_history_depth_, type);
    const UniquePtr<history::History> tag_db(GetHistory());
    HashList root_hashes;
    const bool success = tag_db->GetHashes(&root_hashes);
    assert (success);

    // traversing referenced named root hashes in reverse chronological order
    // to make sure that overlapping history traversals don't leave out catalog
    // revisions accidentially
          HashList::const_reverse_iterator i    = root_hashes.rbegin();
    const HashList::const_reverse_iterator iend = root_hashes.rend();
    for (; i != iend; ++i) {
      Push(ctx, *i);
    }

    return DoTraverse(ctx);
  }

  /**
   * This traverses all catalogs that were left out by previous traversal runs.
   *
   * Note: This method asserts that previous traversal runs left out certain
   *       catalogs due to history_depth restrictions. CatalogTraversal keeps
   *       track of the root catalog hashes of catalog revisions that have been
   *       pruned before. TraversePruned() will use those as entry points.
   *
   * @param type  breadths or depth first traversal
   * @return      true on successful traversal of all necessary catalogs or
   *              false in case of failure or no_repeat_history == false
   */
  bool TraversePruned(const TraversalType type = kBreadthFirstTraversal) {
    TraversalContext ctx(CatalogTraversalParams::kFullHistory, type);
    if (pruned_revisions_.empty()) {
      return false;
    }

          HashSet::const_iterator i    = pruned_revisions_.begin();
    const HashSet::const_iterator iend = pruned_revisions_.end();
    for (; i != iend; ++i) {
      Push(ctx, *i);
    }
    pruned_revisions_.clear();
    return DoTraverse(ctx);
  }

  size_t pruned_revision_count() const { return pruned_revisions_.size(); }

 protected:
  /**
   * This controls the actual traversal. Using a stack to traverse down the
   * catalog hierarchy. This method implements the traversal itself, but not
   * in which way catalogs are handed out to the user code.
   *
   * Each catalog is processed in these steps:
   *  1.) Check if the catalog was processed before
   *        Duplicated traversal can (optionally) be avoided. Note: skipped jobs
   *        can still trigger postponed yields
   *  2.) Pop the next catalog from the stack
   *        Catalogs are always traversed from latest to oldest revision and
   *        from root to leaf nested catalogs
   *  3.) Fetch the catalog from the repository
   *        Depending on where the catalog comes from, this might do different
   *        things (see ObjectFetcherT).
   *  4.) Open the fetched catalog
   *        Depending on the catalog this implementation might differ
   *        (see CatalogT::AttachFreely)
   *  5.) Find and push referencing catalogs
   *        This pushes all descendants of the current catalog onto the stack.
   *        Note that this is dependant on the strategy (depth or breadth first)
   *        and on the history threshold (see history_depth). Furthermore,
   *        catalogs might not be pushed again, when seen before (see
   *        no_repeat_history).
   *  6.) Hand the catalog out to the user code
   *        Depending on the traversal strategy (depth of breadths first) this
   *        might immediately yield zero to N catalogs to the user code.
   *
   * Note: If anything unexpected goes wrong during the traversal process, it
   *       is aborted immediately.
   *
   * @param ctx   the traversal context that steers the whole traversal process
   * @return      true on successful traversal and false on abort
   */
  bool DoTraverse(TraversalContext &ctx) {
    assert (ctx.callback_stack.empty());

    while (! ctx.catalog_stack.empty()) {
      // Get the top most catalog for the next processing step
      CatalogJob job = Pop(ctx);

      // download and open the catalog for processing
      if (! PrepareCatalog(job)) {
        return false;
      }

      // ignored catalogs don't need to be processed anymore but they might
      // release postponed yields
      if (job.ignore) {
        if (! HandlePostponedYields(ctx, job)) {
          return false;
        }
        continue;
      }

      // push catalogs referenced by the current catalog (onto stack)
      PushReferencedCatalogs(ctx, job);

      // notify listeners
      if (! YieldToListeners(ctx, job)) {
        return false;
      }
    }

    // invariant: after the traversal finshed, there should be no more catalogs
    //            to traverse or to yield!
    assert (ctx.catalog_stack.empty());
    assert (ctx.callback_stack.empty());
    return true;
  }


  bool PrepareCatalog(CatalogJob &job) {
    // skipping duplicate catalogs might also yield postponed catalogs
    if (ShouldBeSkipped(job)) {
      job.ignore = true;
      return true;
    }

    // download the catalog file from the backend storage
    // Note: Due to garbage collection, catalogs might not be fetchable anymore.
    //       However, this only counts for root catalogs, since the garbage
    //       collection works on repository revision granularity.
    if (! FetchCatalog(job)) {
      if (ignore_load_failure_ && job.IsRootCatalog()) {
        LogCvmfs(kLogCatalogTraversal, kLogDebug, "ignore missing root catalog "
                                                  "%s (possibly sweeped before)",
                 job.hash.ToString().c_str());
        job.ignore = true;
        return true;
      } else {
        LogCvmfs(kLogCatalogTraversal, error_sink_, "failed to load catalog %s",
                 job.hash.ToString().c_str());
        return false;
      }
    }

    // open the catalog file
    return OpenCatalog(job);
  }


  bool FetchCatalog(CatalogJob &job) {
    if (! object_fetcher_.Fetch(job.hash, &job.catalog_file_path)) {
      return false;
    }

    job.catalog_file_size = GetFileSize(job.catalog_file_path);
    return true;
  }


  bool OpenCatalog(CatalogJob &job) {
    assert (! job.ignore);
    assert (job.catalog == NULL);

    job.catalog = CatalogT::AttachFreely(job.path,
                                         job.catalog_file_path,
                                         job.hash,
                                         job.parent);

    if (job.catalog == NULL) {
      LogCvmfs(kLogCatalogTraversal, error_sink_, "failed to open catalog %s",
               job.hash.ToString().c_str());
      return false;
    }

    return true;
  }


  void PushReferencedCatalogs(TraversalContext &ctx, CatalogJob &job) {
    assert (! job.ignore);
    assert (job.catalog != NULL);
    assert (ctx.traversal_type == kBreadthFirstTraversal ||
            ctx.traversal_type == kDepthFirstTraversal);

    // this differs, depending on the traversal strategy.
    //
    // Breadths First Traversal
    //   Catalogs are traversed from top (root catalog) to bottom (leaf catalogs)
    //   and from more recent (HEAD revision) to older (historic revisions)
    //
    // Depth First Traversal
    //   Catalogs are traversed from oldest revision (depends on the configured
    //   maximal history depth) to the HEAD revision and from bottom (leafs) to
    //   top (root catalogs)
    job.referenced_catalogs = (ctx.traversal_type == kBreadthFirstTraversal)
      ?   PushPreviousRevision(ctx, job)
        + PushNestedCatalogs  (ctx, job)

      :   PushNestedCatalogs  (ctx, job)
        + PushPreviousRevision(ctx, job);
  }

  /**
   * Pushes the previous revision of a (root) catalog.
   * @return  the number of catalogs pushed on the processing stack
   */
  unsigned int PushPreviousRevision(      TraversalContext  &ctx,
                                    const CatalogJob        &job)
  {
    // only root catalogs are used for entering a previous revision (graph)
    if (! job.catalog->IsRoot()) {
      return 0;
    }

    const shash::Any previous_revision = job.catalog->GetPreviousRevision();
    if (previous_revision.IsNull()) {
      return 0;
    }

    // check if the next deeper history level is actually requested
    // Note: otherwise it is marked to be 'pruned' for possible later traversal
    //       (see: TraversePruned())
    if (job.history_depth >= ctx.history_depth) {
      pruned_revisions_.insert(previous_revision);
      return 0;
    }

    Push(ctx, CatalogJob("",
                         previous_revision,
                         0,
                         job.history_depth + 1,
                         NULL));
    return 1;
  }

  /**
   * Pushes all the referenced nested catalogs.
   * @return  the number of catalogs pushed on the processing stack
   */
  unsigned int PushNestedCatalogs(      TraversalContext  &ctx,
                                  const CatalogJob        &job)
  {
    const typename CatalogT::NestedCatalogList &nested = // TODO: C++11 auto ;-)
                                              job.catalog->ListNestedCatalogs();
    typename CatalogT::NestedCatalogList::const_iterator i    = nested.begin();
    typename CatalogT::NestedCatalogList::const_iterator iend = nested.end();
    for (; i != iend; ++i) {
      CatalogT* parent = (no_close_) ? job.catalog : NULL;
      const CatalogJob new_job(i->path.ToString(),
                               i->hash,
                               job.tree_level + 1,
                               job.history_depth,
                               parent);
      Push(ctx, new_job);
    }

    return nested.size();
  }

  void Push(TraversalContext &ctx, const shash::Any &root_catalog_hash) {
    Push(ctx, CatalogJob("", root_catalog_hash, 0, 0));
  }


  bool YieldToListeners(TraversalContext &ctx, CatalogJob &job) {
    assert (! job.ignore);
    assert (job.catalog != NULL);
    assert (ctx.traversal_type == kBreadthFirstTraversal ||
            ctx.traversal_type == kDepthFirstTraversal);

    // in breadth first search mode, every catalog is simply handed out once
    // it is visited. No extra magic required...
    if (ctx.traversal_type == kBreadthFirstTraversal) {
      return Yield(job);
    }

    // in depth first search mode, catalogs might need to wait until all of
    // their referenced catalogs are yielded (ctx.callback_stack)...
    assert (ctx.traversal_type == kDepthFirstTraversal);
    if (job.referenced_catalogs > 0) {
      PostponeYield(ctx, job);
      return true;
    }

    // this catalog can be yielded
    return Yield(job) && HandlePostponedYields(ctx, job);
  }


 private:
  /**
   * This actually hands out a catalog to the user code
   * It is not called by DoTraversa() directly but by wrapper functions in order
   * to provide higher level yielding behaviour.
   */
  bool Yield(CatalogJob &job) {
    assert (! job.ignore);
    assert (job.catalog != NULL || job.postponed);

    // catalog was pushed on ctx.callback_stack before, it might need to be re-
    // opened. If CatalogTraversal<> is configured with no_close, it was not
    // closed before, hence does not need a re-open.
    if (job.postponed && ! no_close_ && ! OpenCatalog(job)) {
      return false;
    }

    // hand the catalog out to the user code (see Observable<>)
    assert (job.catalog != NULL);
    this->NotifyListeners(job.GetCallbackData());

    // close the catalog again after it was processed
    if (! no_close_) {
      delete job.catalog; job.catalog = NULL;
    }

    // we can delete the temporary catalog file here
    if (! job.catalog_file_path.empty()) {
      const int retval = unlink(job.catalog_file_path.c_str());
      if (retval != 0) {
        LogCvmfs(kLogCatalogTraversal, error_sink_, "Failed to unlink %s - %d",
                 job.catalog_file_path.c_str(), errno);
      }
    }

    // all went well...
    return true;
  }

  /**
   * Pushes a catalog to the callback_stack for later yielding
   * Note: this is only used for the Depth First Traversal strategy!
   */
  void PostponeYield(TraversalContext &ctx, CatalogJob &job) {
    assert (job.referenced_catalogs > 0);

    job.postponed = true;
    if (! no_close_) {
      delete job.catalog; job.catalog = NULL;
    }
    ctx.callback_stack.push(job);
  }

  /**
   * Determines if there are postponed yields that can be set free based on
   * the catalog currently being yielded
   *
   * Note: the CatalogJob being handed into this method does not necessarily
   *       have an open Catalog attached to it.
   *
   * @param ctx   the TraversalContext
   * @param job   the catalog job that was just yielded
   * @return      true on successful execution
   */
  bool HandlePostponedYields(TraversalContext &ctx, CatalogJob &job) {
    if (ctx.traversal_type == kBreadthFirstTraversal) {
      return true;
    }

    assert (ctx.traversal_type == kDepthFirstTraversal);
    assert (job.referenced_catalogs == 0);

    // walk through the callback_stack and yield all catalogs that have no un-
    // yielded referenced_catalogs anymore. Every time a CatalogJob in the
    // callback_stack gets yielded it decrements the referenced_catalogs of the
    // next top of the stack (it's parent CatalogJob waiting for yielding)
    CatalogJobStack &clbs = ctx.callback_stack;
    while (! clbs.empty()) {
      CatalogJob &postponed_job = clbs.top();
      if (--postponed_job.referenced_catalogs > 0) {
        break;
      }

      if (! Yield(postponed_job)) {
        return false;
      }
      clbs.pop();
    }

    return true;
  }

  void Push(TraversalContext &ctx, const CatalogJob &job) {
    ctx.catalog_stack.push(job);
  }

  CatalogJob Pop(TraversalContext &ctx) {
    CatalogJob job = ctx.catalog_stack.top(); ctx.catalog_stack.pop();
    return job;
  }

  /**
   * Checks the traversal history if the given catalog was traversed or at least
   * seen before. If 'no_repeat_history' is not set this is always 'false'.
   *
   * @param job   the job to be checked against the traversal history
   * @return      true if the specified catalog was hit before
   */
  bool ShouldBeSkipped(const CatalogJob &job) {
    if (! no_repeat_history_) {
      return false;
    }

    if (visited_catalogs_.count(job.hash) > 0) {
      return true;
    }

    visited_catalogs_.insert(job.hash);
    return false;
  }

  shash::Any GetRepositoryRootCatalogHash() {
    // get the manifest of the repository to learn about the entry point or the
    // root catalog of the repository to be traversed
    manifest::Manifest *manifest = object_fetcher_.FetchManifest();
    if (!manifest) {
      LogCvmfs(kLogCatalogTraversal, error_sink_,
        "Failed to load manifest for repository %s", repo_name_.c_str());
      return shash::Any();
    }

    const shash::Any root_catalog_hash = manifest->catalog_hash();
    delete manifest;

    return root_catalog_hash;
  }

  history::History* GetHistory() {
    return object_fetcher_.FetchHistory();
  }

 private:
  ObjectFetcherT        object_fetcher_;
  const std::string     repo_name_;
  const bool            no_close_;
  const bool            ignore_load_failure_;
  const bool            no_repeat_history_;
  const unsigned int    default_history_depth_;
  HashSet               visited_catalogs_;
  HashSet               pruned_revisions_;
  LogFacilities         error_sink_;
};

typedef CatalogTraversal<catalog::Catalog>         ReadonlyCatalogTraversal;
typedef CatalogTraversal<catalog::WritableCatalog> WritableCatalogTraversal;



/**
 * This is the default class implementing the data object fetching strategy of
 * the CatalogTraversal<> template. It abstracts all accesses to external file
 * or HTTP resources. There is no need to change the default ObjectFetcher of
 * CatalogTraversal<> except for unit-testing.
 */
class ObjectFetcher {
 public:
  ObjectFetcher(const CatalogTraversalParams &params) :
    repo_url_(MakeCanonicalPath(params.repo_url)),
    repo_name_(params.repo_name),
    repo_keys_(params.repo_keys),
    is_remote_(params.repo_url.substr(0, 7) == "http://"),
    ignore_load_failure_(params.ignore_load_failure),
    temporary_directory_(params.tmp_dir),
    error_sink_((params.quiet) ? kLogDebug : kLogStderr)
  {
    if (is_remote_) {
      download_manager_.Init(1, true);
    }
  }

  virtual ~ObjectFetcher() {
    if (is_remote_) {
      download_manager_.Fini();
    }
  }


 public:
  manifest::Manifest* FetchManifest() {
    manifest::Manifest *manifest = NULL;
    // Grab manifest file
    if (!is_remote_) {
      // Locally
      manifest = manifest::Manifest::LoadFile(repo_url_ + "/.cvmfspublished");
    } else {
      // Remote
      const std::string url = repo_url_ + "/.cvmfspublished";

      // Initialize signature module
      signature::SignatureManager signature_manager;
      signature_manager.Init();
      const bool success = signature_manager.LoadPublicRsaKeys(repo_keys_);
      if (!success) {
        LogCvmfs(kLogCatalogTraversal, error_sink_,
          "cvmfs public key(s) could not be loaded.");
        signature_manager.Fini();
        return NULL;
      }

      // Download manifest file
      struct manifest::ManifestEnsemble manifest_ensemble;
      manifest::Failures retval = manifest::Fetch(
                                    repo_url_,
                                    repo_name_,
                                    0,
                                    NULL,
                                    &signature_manager,
                                    &download_manager_,
                                    &manifest_ensemble);

      // We don't need the signature module from now on
      signature_manager.Fini();

      // Check if manifest was loaded correctly
      if (retval == manifest::kFailOk) {
        manifest = new manifest::Manifest(*manifest_ensemble.manifest);
      } else if (retval == manifest::kFailNameMismatch) {
        LogCvmfs(kLogCatalogTraversal, error_sink_,
                 "repository name mismatch. No name provided?");
      } else if (retval == manifest::kFailBadSignature   ||
                 retval == manifest::kFailBadCertificate ||
                 retval == manifest::kFailBadWhitelist)
      {
        LogCvmfs(kLogCatalogTraversal, error_sink_,
                 "repository signature mismatch. No key(s) provided?");
      } else {
        LogCvmfs(kLogCatalogTraversal, error_sink_,
                 "failed to load manifest (%d - %s)",
                 retval, Code2Ascii(retval));
      }
    }

    return manifest;
  }

  inline history::History* FetchHistory() {
    return NULL;
  }

  inline bool Fetch(const shash::Any  &object_hash,
                    std::string       *object_file,
                    const char         hash_suffix = 'C') {
    return (is_remote_) ? Download  (object_hash, hash_suffix, object_file)
                        : Decompress(object_hash, hash_suffix, object_file);
  }


  /**
   * Checks if a file exists, both remotely or locally, depending on the type
   * of repository currently traversed
   * @param file   the file to be checked for existence
   * @return       true if the file exists, false otherwise
   */
  inline bool Exists(const std::string &file) {
    if (is_remote_) {
      download::JobInfo head(&file, false);
      return download_manager_.Fetch(&head) == download::kFailOk;
    } else {
      return FileExists(file);
    }
  }


 protected:
  /**
   * Downloads an object from a remote repository and extracts it in one shot
   * @param object_hash    the SHA-1 hash of the object to be downloaded
   * @param hash_suffix    hash suffix for the object to be downloaded
   * @param file_path      output parameter for the loaded object file
   * @return               true, if object was successfully downloaded
   */
  bool Download(const shash::Any  &object_hash,
                const char         hash_suffix,
                std::string       *file_path) {
    file_path->clear();

    const std::string source =
      "data" + object_hash.MakePathExplicit(1, 2) + hash_suffix;
    const std::string dest = temporary_directory_ + "/" + object_hash.ToString();
    const std::string url = repo_url_ + "/" + source;

    download::JobInfo download_catalog(&url, true, false, &dest, &object_hash);
    download::Failures retval = download_manager_.Fetch(&download_catalog);

    if (! ignore_load_failure_ && retval != download::kFailOk) {
      LogCvmfs(kLogCatalogTraversal, error_sink_, "failed to download object "
                                                  "%s (%d - %s)",
               object_hash.ToString().c_str(), retval, Code2Ascii(retval));
    }

    *file_path = dest;
    return retval == download::kFailOk;
  }


  /**
   * Decompresses an object that resides on local storage.
   * @param object_hash    the SHA-1 hash of the object to be extracted
   * @return               the path to the extracted object file
   */
  bool Decompress(const shash::Any  &object_hash,
                  const char         hash_suffix,
                  std::string       *file_path) {
    file_path->clear();

    const std::string source =
      repo_url_ + "/data" + object_hash.MakePathExplicit(1, 2) + hash_suffix;
    const std::string dest = temporary_directory_ + "/" + object_hash.ToString();
    const bool file_exists = FileExists(source);

    if (! ignore_load_failure_ && ! file_exists) {
      LogCvmfs(kLogCatalogTraversal, error_sink_, "failed to locate object %s "
                                                  "at '%s'",
               object_hash.ToString().c_str(), dest.c_str());
    }

    if (! file_exists || ! zlib::DecompressPath2Path(source, dest)) {
      LogCvmfs(kLogCatalogTraversal, error_sink_, "failed to extract object %s "
                                                  "from '%s' to '%s' (errno: %d)",
               object_hash.ToString().c_str(), source.c_str(), dest.c_str(),
               errno);
      return false;
    }

    *file_path = dest;
    return true;
  }

 private:
  const std::string          repo_url_;
  const std::string          repo_name_;
  const std::string          repo_keys_;
  const bool                 is_remote_;
  const bool                 ignore_load_failure_;
  const std::string          temporary_directory_;
  download::DownloadManager  download_manager_;
  LogFacilities              error_sink_;
};

}

#endif /* CVMFS_CATALOG_TRAVERSAL_H_*/
