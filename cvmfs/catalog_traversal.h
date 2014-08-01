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
 * @param catalog_hash        the SHA-1 content hash of the catalog
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
 * This class traverses the complete catalog hierarchy of a CVMFS repository
 * recursively. The user can specify a callback which is called for each catalog
 * on the way.
 * We are doing a BFS, pre-order traversal here. (I.e. if you simply print
 * the catalogs in the provided order you obtain a nice catalog tree.)
 * Note: Since all CVMFS catalog files together can grow to several gigabytes in
 *       file size, each catalog is loaded, processed and removed immediately
 *       afterwards.
 *
 * @param CatalogT        the catalog class that should be used for traversal
 *                        usually this will be either catalog::Catalog or
 *                        catalog::WritableCatalog
 *                        (please consider to use the typedef'ed versions of
 *                        this template i.e. [Readable|Writable]CatalogTraversal)
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

    // dynamic processing state
    std::string   catalog_file_path;
    size_t        catalog_file_size;
    bool          ignore;
    CatalogT     *catalog;
    unsigned int  referenced_catalogs;
    bool          postponed;
  };

  typedef std::stack<CatalogJob>   CatalogJobStack;

  /**
   * This struct represents a catalog traversal context. It needs to be re-
   * created for each catalog traversal process and contains information to this
   * specific catalog traversal run.
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
    MightPush(ctx, root_catalog_hash);
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
    MightPush(ctx, root_catalog_hash);
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
      MightPush(ctx, *i);
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
      MightPush(ctx, *i);
    }
    pruned_revisions_.clear();
    return DoTraverse(ctx);
  }

  size_t pruned_revision_count() const { return pruned_revisions_.size(); }

 protected:
  bool DoTraverse(TraversalContext &ctx) {

    // The CatalogTraversal works with a stack, where new nested catalogs are
    // pushed onto while processing their parent (breadth first traversal).
    // When all catalogs are processed, this stack will naturally be empty and
    // the traversal can terminate
    while (! ctx.catalog_stack.empty()) {
      // Get the top most catalog for the next processing step
      CatalogJob job = ctx.catalog_stack.top(); ctx.catalog_stack.pop();

      // download the catalog file
      const bool successful_download = FetchCatalog(job);
      if (job.ignore)            continue;
      if (! successful_download) return false;

      // open the catalog file
      const bool successful_open = OpenCatalog(job);
      if (! successful_open) return false;

      // push catalogs referenced by the current catalog (onto stack)
      PushReferencedCatalogs(ctx, job);

      // notify listeners
      const bool successful_yield = YieldToListeners(ctx, job);
      if (! successful_yield) return false;
    }

    assert (ctx.catalog_stack.empty());
    assert (ctx.callback_stack.empty());
    return true;
  }

  bool FetchCatalog(CatalogJob &job) {
    const bool successful_fetch = object_fetcher_.Fetch( job.hash,
                                                        &job.catalog_file_path);

    if (! successful_fetch) {
      if (ignore_load_failure_ && job.IsRootCatalog()) {
        LogCvmfs(kLogCatalogTraversal, kLogDebug, "ignore missing root catalog "
                                                  "%s (possibly sweeped before)",
                 job.hash.ToString().c_str());
        job.ignore = true;
      } else {
        LogCvmfs(kLogCatalogTraversal, error_sink_, "failed to load catalog %s",
                 job.hash.ToString().c_str());
      }

      return false;
    }

    job.catalog_file_size = GetFileSize(job.catalog_file_path);
    return true;
  }

  bool OpenCatalog(CatalogJob &job) {
    assert (! job.ignore);

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

    job.referenced_catalogs = (ctx.traversal_type == kBreadthFirstTraversal)
      ?   MightPushPreviousRevision(ctx, job)
        + MightPushNestedCatalogs  (ctx, job)

      :   MightPushNestedCatalogs  (ctx, job)
        + MightPushPreviousRevision(ctx, job);
  }

  unsigned int MightPushPreviousRevision(      TraversalContext  &ctx,
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
    if (job.history_depth >= ctx.history_depth) {
      pruned_revisions_.insert(previous_revision);
      return 0;
    }

    const CatalogJob new_job("",
                             previous_revision,
                             0,
                             job.history_depth + 1,
                             NULL);
    return MightPush(ctx, new_job) ? 1 : 0;
  }

  unsigned int MightPushNestedCatalogs(      TraversalContext  &ctx,
                                       const CatalogJob        &job)
  {
    unsigned int pushed_catalogs = 0;
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
      if (MightPush(ctx, new_job)) {
        ++pushed_catalogs;
      }
    }

    return pushed_catalogs;
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
    // their referenced catalogs are yielded (callback_stack)...
    assert (ctx.traversal_type == kDepthFirstTraversal);
    PostponeYield(ctx, job);

    // walk through the callback_stack and yield all catalogs that have no un-
    // yielded referenced_catalogs anymore. Every time a CatalogJob in the
    // callback_stack gets yielded it decrements the referenced_catalogs of the
    // next top of the stack (it's parent CatalogJob waiting for yielding)
    CatalogJobStack &clbs = ctx.callback_stack;
    while (! clbs.empty() && clbs.top().referenced_catalogs == 0) {
      if (! Yield(clbs.top())) {
        return false;
      }

      clbs.pop();
      if (! clbs.empty()) {
        CatalogJob &parent_job = ctx.callback_stack.top();
        assert (parent_job.referenced_catalogs > 0);
        parent_job.referenced_catalogs--;
      }
    }

    return true;
  }

  bool Yield(CatalogJob &job) {
    assert (! job.ignore);
    assert (job.catalog != NULL || job.postponed);

    if (job.postponed && ! OpenCatalog(job)) {
      return false;
    }

    assert (job.catalog != NULL);
    this->NotifyListeners(job.GetCallbackData());

    if (! no_close_) {
      delete job.catalog; job.catalog = NULL;
      unlink(job.catalog_file_path.c_str());
    }

    return true;
  }

  void PostponeYield(TraversalContext &ctx, CatalogJob &job) {
    if (! no_close_ && job.referenced_catalogs > 0) {
      delete job.catalog; job.catalog = NULL;
      job.postponed = true;
    }

    ctx.callback_stack.push(job);
  }

  bool MightPush(TraversalContext &ctx, const CatalogJob &job) {
    if (WasHitBefore(job)) {
      return false;
    }

    ctx.catalog_stack.push(job);
    return true;
  }

  bool MightPush(      TraversalContext  &ctx,
                 const shash::Any        &root_catalog_hash) {
    return MightPush(ctx, CatalogJob("", root_catalog_hash, 0, 0));
  }

  /**
   * Checks the traversal history if the given catalog was traversed or at least
   * seen before. If 'no_repeat_history' is not set this is always 'false'.
   *
   * @param job   the job to be checked against the traversal history
   * @return      true if the specified catalog was hit before
   */
  bool WasHitBefore(const CatalogJob &job) {
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

  inline history::TagList FetchTagList() {
    return history::TagList();
  }

  inline bool Fetch(const shash::Any  &catalog_hash,
                    std::string       *catalog_file) {
    return (is_remote_) ? DownloadCatalog  (catalog_hash, catalog_file)
                        : DecompressCatalog(catalog_hash, catalog_file);
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
   * Downloads a catalog from a remote repository and extracts it in one shot
   * @param catalog_hash   the SHA-1 hash of the catalog to be downloaded
   * @param catalog_file   output parameter for the loaded catalog file
   * @return               true, if catalog was successfully downloaded
   */
  bool DownloadCatalog(const shash::Any  &catalog_hash,
                       std::string       *catalog_file) {
    catalog_file->clear();

    const std::string source = "data" +
                               catalog_hash.MakePathExplicit(1, 2) + "C";
    const std::string dest = temporary_directory_ + "/" + catalog_hash.ToString();
    const std::string url = repo_url_ + "/" + source;

    download::JobInfo download_catalog(&url, true, false, &dest, &catalog_hash);
    download::Failures retval = download_manager_.Fetch(&download_catalog);

    if (! ignore_load_failure_ && retval != download::kFailOk) {
      LogCvmfs(kLogCatalogTraversal, error_sink_, "failed to download catalog "
                                                  "%s (%d - %s)",
               catalog_hash.ToString().c_str(), retval, Code2Ascii(retval));
    }

    *catalog_file = dest;
    return retval == download::kFailOk;
  }


  /**
   * Decompresses a catalog that resides on local storage.
   * @param catalog_hash   the SHA-1 hash of the catalog to be extracted
   * @return               the path to the extracted catalog file
   */
  bool DecompressCatalog(const shash::Any  &catalog_hash,
                         std::string       *catalog_file) {
    catalog_file->clear();

    const std::string source =
      repo_url_ + "/data" + catalog_hash.MakePathExplicit(1, 2) + "C";
    const std::string dest = temporary_directory_ + "/" + catalog_hash.ToString();
    const bool file_exists = FileExists(source);

    if (! ignore_load_failure_ && ! file_exists) {
      LogCvmfs(kLogCatalogTraversal, error_sink_, "failed to locate catalog %s "
                                                  "at '%s'",
               catalog_hash.ToString().c_str(), dest.c_str());
    }

    if (! file_exists || ! zlib::DecompressPath2Path(source, dest)) {
      LogCvmfs(kLogCatalogTraversal, error_sink_, "failed to extract catalog %s "
                                                  "from '%s' to '%s' (errno: %d)",
               catalog_hash.ToString().c_str(), source.c_str(), dest.c_str(),
               errno);
      return false;
    }

    *catalog_file = dest;
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
