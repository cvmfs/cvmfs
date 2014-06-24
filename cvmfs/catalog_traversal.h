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
 * @param history_depty       the distance from the current HEAD revision
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
 * @param repo_url    the path to the repository to be traversed:
 *                    -> either absolute path to the local catalogs
 *                    -> or an URL to a remote repository
 * @param repo_name   fully qualified repository name (used for remote
 *                    repository signature check) (optional)
 * @param repo_keys   a comma separated list of public key file
 *                    locations to verify the repository manifest file
 * @param history     depth of the desired catalog history traversal
 *                    (default: 0 - only HEAD catalogs are traversed)
 * @param no_close    do not close catalogs after they were attached
 *                    (catalogs retain their parent/child pointers)
 * @param tmp_dir     path to the temporary directory to be used
 *                    (default: /tmp)
 */
struct CatalogTraversalParams {
  CatalogTraversalParams() : history(0), no_close(false), tmp_dir("/tmp") {}

  static const unsigned int kFullHistory;

  std::string   repo_url;
  std::string   repo_name;
  std::string   repo_keys;
  unsigned int  history;
  bool          no_close;
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

 protected:
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
      parent(parent) {}

    const std::string   path;
    const shash::Any    hash;
    const unsigned      tree_level;
    const unsigned      history_depth;
          CatalogT     *parent;
  };
  typedef std::stack<CatalogJob> CatalogJobStack;

 public:
  /**
   * Constructs a new catalog traversal engine based on the construction
   * parameters described in struct ConstructionParams.
   */
	CatalogTraversal(const CatalogTraversalParams &params) :
    object_fetcher_(params),
    repo_name_(params.repo_name),
    no_close_(params.no_close),
    history_depth_(params.history)
  {}


  /**
   * Starts the traversal process.
   * After calling this methods CatalogTraversal will go through all catalogs
   * and call the registered callback methods for each found catalog.
   * If something goes wrong in the process, the traversal will be cancelled.
   * @return  true, when all catalogs were successfully processed. On failure
   *          the traversal is cancelled and false is returned
   */
  bool Traverse() {
    // get the manifest of the repository to learn about the entry point or the
    // root catalog of the repository to be traversed
    manifest::Manifest *manifest = object_fetcher_.FetchManifest();
    if (!manifest) {
      LogCvmfs(kLogCatalogTraversal, kLogStderr,
        "Failed to load manifest for repository %s", repo_name_.c_str());
      return false;
    }

    // add the root catalog of the repository as the first element on the job
    // stack
    Push(CatalogJob("", manifest->catalog_hash(), 0, 0));

    delete manifest;

    return DoTraverse();
  }


 protected:
  bool DoTraverse() {
    // The CatalogTraversal works with a stack, where new nested catalogs are
    // pushed onto while processing their parent (breadth first traversal).
    // When all catalogs are processed, this stack will naturally be empty and
    // the traversal can terminate
    while(!catalog_stack_.empty())
    {
      // Get the top most catalog for the next processing step
      CatalogJob job = catalog_stack_.top();
      catalog_stack_.pop();

      // Process it (potentially generating new catalog jobs on the stack)
      const bool success = ProcessCatalogJob(job);
      if (!success)
        return false;
    }
    return true;
  }


  bool ProcessCatalogJob(const CatalogJob &job) {
    // Load a catalog
    std::string tmp_file;
    if (!object_fetcher_.Fetch(job.hash, &tmp_file)) {
      LogCvmfs(kLogCatalogTraversal, kLogStderr, "failed to load catalog %s",
               job.hash.ToString().c_str());
      return false;
    }

    // Get the size of the decompressed catalog file
    const size_t file_size = GetFileSize(tmp_file);

    // Open the catalog
    CatalogT *catalog = CatalogT::AttachFreely(job.path,
                                               tmp_file,
                                               job.hash,
                                               job.parent);
    if (!no_close_) {
      unlink(tmp_file.c_str());
    }
    if (catalog == NULL) {
      LogCvmfs(kLogCatalogTraversal, kLogStderr, "failed to open catalog %s",
               job.hash.ToString().c_str());
      return false;
    }

    // Provide the user with the catalog
    NotifyListeners(CallbackData(catalog,
                                 job.hash,
                                 job.tree_level,
                                 file_size,
                                 job.history_depth));

    // Inception! Go deeper into the catalog tree
    PushReferencedCatalogs(catalog, job);

    // We are done with this catalog
    if (!no_close_) {
      delete catalog;
    }

    return true;
  }


  unsigned int PushReferencedCatalogs(      CatalogT    *catalog,
                                      const CatalogJob  &job) {
    unsigned int pushed_catalogs = 0;

    // Only Root catalogs may be used as entry points into previous revisions
    if (job.history_depth < history_depth_ && catalog->IsRoot()) {
      shash::Any previous_revision = catalog->GetPreviousRevision();
      if (! previous_revision.IsNull()) {
        const bool pushed = Push(CatalogJob("",
                                            previous_revision,
                                            0,
                                            job.history_depth + 1,
                                            NULL));
        if (pushed) {
          ++pushed_catalogs;
        }
      }
    }

    // Inception! Go to the next catalog level
    // Note: taking a copy of the result of ListNestedCatalogs() here for
    //       data corruption prevention
    const typename CatalogT::NestedCatalogList &nested =
                                                  catalog->ListNestedCatalogs();
    typename CatalogT::NestedCatalogList::const_iterator i    = nested.begin();
    typename CatalogT::NestedCatalogList::const_iterator iend = nested.end();
    for (; i != iend; ++i) {
      CatalogT* parent = (no_close_) ? catalog : NULL;
      const bool pushed = Push(CatalogJob(i->path.ToString(),
                                          i->hash,
                                          job.tree_level + 1,
                                          job.history_depth,
                                          parent));
      if (pushed) {
        ++pushed_catalogs;
      }
    }

    return pushed_catalogs;
  }


  bool Push(const CatalogJob &job) {
    catalog_stack_.push(job);
    return true;
  }

 private:
  ObjectFetcherT      object_fetcher_;
  const std::string   repo_name_;
  const bool          no_close_;
  const unsigned int  history_depth_;
  CatalogJobStack     catalog_stack_;
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
    temporary_directory_(params.tmp_dir)
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
        LogCvmfs(kLogCatalogTraversal, kLogStderr,
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
        LogCvmfs(kLogCatalogTraversal, kLogStderr,
                 "repository name mismatch. No name provided?");
      } else if (retval == manifest::kFailBadSignature   ||
                 retval == manifest::kFailBadCertificate ||
                 retval == manifest::kFailBadWhitelist)
      {
        LogCvmfs(kLogCatalogTraversal, kLogStderr,
                 "repository signature mismatch. No key(s) provided?");
      } else {
        LogCvmfs(kLogCatalogTraversal, kLogStderr,
                 "failed to load manifest (%d - %s)",
                 retval, Code2Ascii(retval));
      }
    }

    return manifest;
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
  bool DownloadCatalog(const shash::Any& catalog_hash,
                       std::string *catalog_file) {
    catalog_file->clear();

    const std::string source = "data" + catalog_hash.MakePath(1,2) + "C";
    const std::string dest = temporary_directory_ + "/" + catalog_hash.ToString();
    const std::string url = repo_url_ + "/" + source;

    download::JobInfo download_catalog(&url, true, false, &dest, &catalog_hash);
    download::Failures retval = download_manager_.Fetch(&download_catalog);

    if (retval != download::kFailOk) {
      LogCvmfs(kLogCatalogTraversal, kLogStderr, "failed to download catalog %s"
                                                 " (%d - %s)",
             catalog_hash.ToString().c_str(), retval, Code2Ascii(retval));
      return false;
    }

    *catalog_file = dest;
    return true;
  }


  /**
   * Decompresses a catalog that resides on local storage.
   * @param catalog_hash   the SHA-1 hash of the catalog to be extracted
   * @return               the path to the extracted catalog file
   */
  bool DecompressCatalog(const shash::Any& catalog_hash,
                         std::string *catalog_file) {
    catalog_file->clear();

    const std::string source =
      repo_url_ + "/data" + catalog_hash.MakePath(1,2) + "C";
    const std::string dest = temporary_directory_ + "/" + catalog_hash.ToString();

    if (!zlib::DecompressPath2Path(source, dest))
      return false;

    *catalog_file = dest;
    return true;
  }

 private:
  const std::string          repo_url_;
  const std::string          repo_name_;
  const std::string          repo_keys_;
  const bool                 is_remote_;
  const std::string          temporary_directory_;
  download::DownloadManager  download_manager_;
};

}

#endif /* CVMFS_CATALOG_TRAVERSAL_H_*/
