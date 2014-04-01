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

namespace swissknife {

/**
 * Callback data which has to be implemented by the registered callback
 * functions/methods (see Observable<> for further details)
 * @param catalog             the catalog object which needs to be processed
 * @param catalog_hash        the SHA-1 content hash of the catalog
 * @param tree_level          the depth in the nested catalog tree
 *                            (starting at zero)
 */
struct CatalogTraversalData {
  CatalogTraversalData(const catalog::Catalog* catalog,
                       const shash::Any&       catalog_hash,
                       const unsigned          tree_level) :
    catalog(catalog), catalog_hash(catalog_hash), tree_level(tree_level) {}

  const catalog::Catalog*  catalog;
  const shash::Any         catalog_hash;
  const unsigned int       tree_level;
};

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
 * CAUTION: the Catalog* pointer passed into the callback becomes invalid
 *          directly after the callback method returns, unless you create the
 *          CatalogTraversal object with no_close = true.
 */
template<class CatalogT>
class CatalogTraversal : public Observable<CatalogTraversalData> {
 protected:
  struct CatalogJob {
    CatalogJob(const std::string      &path,
               const shash::Any       &hash,
               const unsigned          tree_level,
                     catalog::Catalog *parent = NULL) :
      path(path),
      hash(hash),
      tree_level(tree_level),
      parent(parent) {}
    CatalogJob(const catalog::Catalog::NestedCatalog &nested_catalog,
               const unsigned                         tree_level,
                     catalog::Catalog                *parent = NULL) :
      path(nested_catalog.path.ToString()),
      hash(nested_catalog.hash),
      tree_level(tree_level),
      parent(parent) {}

    const std::string       path;
    const shash::Any        hash;
    const unsigned          tree_level;
          catalog::Catalog *parent;
  };
  typedef std::stack<CatalogJob> CatalogJobStack;

 public:
  /**
   * Constructs a new catalog traversal engine.
   * @param repo_url           the path to the repository to be traversed:
   *                           -> either absolute path to the local catalogs
   *                           -> or an URL to a remote repository
   * @param repo_name          fully qualified repository name (used for remote
   *                           repository signature check) (optional)
   * @param repo_keys          a comma separated list of public key file
   *                           locations to verify the repository manifest file
   * @param no_close           do not close catalogs after they were attached
   *                           (catalogs retain their parent/child pointers)
   */
	CatalogTraversal(const std::string& repo_url,
                   const std::string& repo_name = "",
                   const std::string& repo_keys = "",
                   const bool         no_close = false,
                   const std::string& tmp_dir  = "/tmp") :
    repo_url_(MakeCanonicalPath(repo_url)),
    repo_name_(repo_name),
    repo_keys_(repo_keys),
    is_remote_(repo_url.substr(0, 7) == "http://"),
    no_close_(no_close),
    temporary_directory_(tmp_dir)
  {
    if (is_remote_)
      download_manager_.Init(1, true);
  }

  virtual ~CatalogTraversal() {
    if (is_remote_)
      download_manager_.Fini();
  }


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
    manifest::Manifest *manifest = LoadManifest();
    if (!manifest) {
      LogCvmfs(kLogCatalogTraversal, kLogStderr,
        "Failed to load manifest for repository %s", repo_name_.c_str());
      return false;
    }

    // add the root catalog of the repository as the first element on the job
    // stack
    CatalogJob job("", manifest->catalog_hash(), 0);
    catalog_stack_.push(job);

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
    if (!FetchCatalog(job.hash, &tmp_file)) {
      LogCvmfs(kLogCatalogTraversal, kLogStderr, "failed to load catalog %s",
               job.hash.ToString().c_str());
      return false;
    }

    // Open the catalog
    catalog::Catalog *catalog = CatalogT::AttachFreely(job.path,
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
    NotifyListeners(CatalogTraversalData(catalog, job.hash, job.tree_level));

    // Inception! Go to the next catalog level
    // Note: taking a copy of the result of ListNestedCatalogs() here for
    //       data corruption prevention
    const catalog::Catalog::NestedCatalogList nested_catalogs =
      catalog->ListNestedCatalogs();
    for (catalog::Catalog::NestedCatalogList::const_iterator i =
         nested_catalogs.begin(), iEnd = nested_catalogs.end();
         i != iEnd; ++i)
    {
      catalog::Catalog* parent = (no_close_) ? catalog : NULL;
      catalog_stack_.push(CatalogJob(*i, job.tree_level + 1, parent));
    }

    // We are done with this catalog
    if (!no_close_) {
      delete catalog;
    }
    return true;
  }


  manifest::Manifest* LoadManifest() {
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


  inline bool FetchCatalog(const shash::Any& catalog_hash,
                           std::string *catalog_file)
  {
    return (is_remote_) ? DownloadCatalog  (catalog_hash, catalog_file)
                        : DecompressCatalog(catalog_hash, catalog_file);
  }


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


  /**
   * Checks if a file exists, both remotely or locally, depending on the type
   * of repository currently traversed
   * @param file   the file to be checked for existence
   * @return       true if the file exists, false otherwise
   */
  bool Exists(const std::string &file) {
    if (is_remote_) {
      download::JobInfo head(&file, false);
      return download_manager_.Fetch(&head) == download::kFailOk;
    } else {
      return FileExists(file);
    }
  }

 private:
  const std::string repo_url_;
  const std::string repo_name_;
  const std::string repo_keys_;
  const bool        is_remote_;
  const bool        no_close_;
  const std::string temporary_directory_;
  CatalogJobStack   catalog_stack_;
  download::DownloadManager download_manager_;
};

typedef CatalogTraversal<catalog::Catalog> SimpleCatalogTraversal;

}

#endif /* CVMFS_CATALOG_TRAVERSAL_H_*/
