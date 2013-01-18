/**
 * This file is part of the CernVM File System.
 */

#include <string>
#include <stack>

#include "catalog.h"
#include "util.h"
#include "download.h"
#include "logging.h"
#include "compression.h"

#include "manifest.h"
#include "manifest_fetch.h"
#include "signature.h"

namespace swissknife {

/**
 * This class traverses the complete catalog hierarchy of a CVMFS repository
 * recursively. The user can specify a callback which is called for each catalog
 * on the way.
 * We are doing a depth first, pre-order traversal here. (I.e. if you simply print
 * the catalogs in the provided order you obtain a nice catalog tree.)
 * Note: Since all CVMFS catalog files together can grow to several gigabytes in
 *       file size, each catalog is loaded, processed and removed immediately
 *       afterwards.
 *
 * CAUTION: currently the Catalog* pointer passed into the callback becomes in-
 *          valid directly after the callback method returns. Therefore you
 *          MUST NOT store it for later use.
 *
 * TODO: Use the Observable template buried in Pull Request 46 instead of imple-
 *       menting your own callback infrastructure here.
 */
template<class T>
class CatalogTraversal
{
 public:
  /**
   * Callback signature which has to be implemented by the delegate object
   * @param catalog             the catalog object which needs to be processed
   * @param catalog_hash        the SHA-1 content hash of the catalog
   * @param tree_level          the depth in the nested catalog tree
   *                            (starting at zero)
   */
  typedef void (T::*Callback)(const catalog::Catalog* catalog,
                              const hash::Any&        catalog_hash,
                              const unsigned          tree_level);


 protected:
  typedef struct CatalogJob_ {
    CatalogJob_(const std::string& path,
                const hash::Any&   hash,
                const unsigned     tree_level) :
      path(path),
      hash(hash),
      tree_level(tree_level) {}
    CatalogJob_(const catalog::Catalog::NestedCatalog& nested_catalog,
                const unsigned                         tree_level) :
      path(nested_catalog.path.ToString()),
      hash(nested_catalog.hash),
      tree_level(tree_level) {}

    const std::string path;
    const hash::Any   hash;
    const unsigned    tree_level;
  } CatalogJob;
  typedef std::stack<CatalogJob> CatalogJobStack;


 public:

  /**
   * Constructs a new catalog traversal engine.
   * @param delegate           the object to be notified when a catalog needs
   *                           to be processed
   * @param catalog_callback   a function pointer to the callback to be called
   *                           on the delegate object
   * @param repo_url           the path to the repository to be traversed:
   *                           -> either absolute path to the local catalogs
   *                           -> or an URL to a remote repository
   * @param repo_name          fully qualified repository name (used for remote
   *                           repository signature check) (optional)
   * @param repo_keys          a comma separated list of public key file
   *                           locations to verify the repository manifest file
   */
	CatalogTraversal(T*                 delegate,
                   Callback           catalog_callback,
                   const std::string& repo_url,
                   const std::string& repo_name = "",
                   const std::string& repo_keys = "") :
    delegate_(delegate),
    catalog_callback_(catalog_callback),
    repo_url_(MakeCanonicalPath(repo_url)),
    repo_name_(repo_name),
    repo_keys_(repo_keys),
    is_remote_(repo_url.substr(0, 7) == "http://")
  {
    if (is_remote_)
      download::Init(1);
  }

  virtual ~CatalogTraversal() {
    if (is_remote_)
      download::Fini();
  }


  /**
   * Starts the traversal process.
   * After calling this methods CatalogTraversal will go through all catalogs
   * and call the given callback method on the provided delegate object for each
   * found catalog.
   * If something goes wrong in the process, the traversal will be cancelled.
   * @return  true, when all catalogs were successfully processed. On failure
   *          the traversal is cancelled and false is returned
   */
  bool Traverse() {
    assert(catalog_callback_ != NULL);

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

    // start the magic
    return TraverseRecursively();
  }


 protected:
  bool TraverseRecursively() {

    // the CatalogTraversal works with a stack, where new nested catalogs are
    // pushed onto while processing their parent (depth first traversal).
    // When all catalogs are processed, this stack will naturally be empty and
    // the traversal can terminate
    while(!catalog_stack_.empty())
    {
      // get the top most catalog for the next processing step
      CatalogJob job = catalog_stack_.top();
      catalog_stack_.pop();

      // process it (potentially generating new catalog jobs on the stack)
      const bool success = ProcessCatalogJob(job);
      if (!success)
        return false;
    }

    // well done
    return true;
  }


  bool ProcessCatalogJob(const CatalogJob &job) {
    // load a catalog
    std::string tmp_file;
    if (!FetchCatalog(job.hash, &tmp_file)) {
      LogCvmfs(kLogCatalogTraversal, kLogStdout, "failed to load catalog %s",
               job.hash.ToString().c_str());
      return false;
    }

    // open the catalog
    catalog::Catalog *catalog = catalog::AttachFreely(job.path, tmp_file);
    unlink(tmp_file.c_str());
    if (catalog == NULL) {
      LogCvmfs(kLogCatalogTraversal, kLogStdout, "failed to open catalog %s",
               job.hash.ToString().c_str());
      return false;
    }

    // provide the user with the catalog
    (delegate_->*catalog_callback_)(catalog, job.hash, job.tree_level);

    // Inception! Go to the next recursion level
    catalog::Catalog::NestedCatalogList *nested_catalogs =
      catalog->ListNestedCatalogs();
    for (catalog::Catalog::NestedCatalogList::const_iterator i =
         nested_catalogs->begin(), iEnd = nested_catalogs->end();
         i != iEnd; ++i)
    {
      catalog_stack_.push(CatalogJob(*i, job.tree_level + 1));
    }

    // we are done with this catalog
    delete catalog;

    // sucessfully traversed
    return true;
  }


  manifest::Manifest* LoadManifest() {
    manifest::Manifest *manifest = NULL;

    // grab manifest file
    if (!is_remote_) {
      // locally
      manifest = manifest::Manifest::LoadFile(repo_url_ + "/.cvmfspublished");

    } else {
      // remote
      const std::string url = repo_url_ + "/.cvmfspublished";

      // initialize signature module
      signature::Init();
      const bool success = signature::LoadPublicRsaKeys(repo_keys_);
      if (!success) {
        LogCvmfs(kLogCatalogTraversal, kLogStderr,
          "cvmfs public key(s) could not be loaded.");
        signature::Fini();
        return NULL;
      }

      // download manifest file
      struct manifest::ManifestEnsemble manifest_ensemble;
      manifest::Failures retval = manifest::Fetch(
                                    repo_url_,
                                    repo_name_,
                                    0,
                                    NULL,
                                    &manifest_ensemble);

      // we don't need the signature module from now on
      signature::Fini();

      // check if manifest was loaded correctly
      if (retval == manifest::kFailNameMismatch)
        LogCvmfs(kLogCatalogTraversal, kLogStderr,
          "repository name mismatch. No name provided?");
      if (retval == manifest::kFailBadSignature   ||
          retval == manifest::kFailBadCertificate ||
          retval == manifest::kFailBadWhitelist)
        LogCvmfs(kLogCatalogTraversal, kLogStderr,
          "repository signature mismatch. No key(s) provided?");

      if (retval == manifest::kFailOk)
        manifest = new manifest::Manifest(*manifest_ensemble.manifest);
    }

    return manifest;
  }


  inline bool FetchCatalog(const hash::Any& catalog_hash,
                                  std::string *catalog_file) {
    return (is_remote_) ? DownloadCatalog  (catalog_hash, catalog_file)
                        : DecompressCatalog(catalog_hash, catalog_file);
  }


  /**
   * Downloads a catalog from a remote repository and extracts it in one shot
   * @param catalog_hash   the SHA-1 hash of the catalog to be downloaded
   * @param catalog_file   output parameter for the loaded catalog file
   * @return               true, if catalog was successfully downloaded
   */
  bool DownloadCatalog(const hash::Any& catalog_hash,
                       std::string *catalog_file) {
    catalog_file->clear();

    const std::string source = "data" + catalog_hash.MakePath(1,2) + "C";
    const std::string dest = "/tmp/" + catalog_hash.ToString();
    const std::string url = repo_url_ + "/" + source;

    download::JobInfo download_catalog(&url, true, false, &dest, &catalog_hash);
    download::Failures retval = download::Fetch(&download_catalog);

    if (retval != download::kFailOk) {
      LogCvmfs(kLogCatalogTraversal, kLogStdout, "failed to download catalog %s (%d)",
             catalog_hash.ToString().c_str(), retval);
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
  bool DecompressCatalog(const hash::Any& catalog_hash,
                         std::string *catalog_file) {
    catalog_file->clear();

    const std::string source =
      repo_url_ + "/data" + catalog_hash.MakePath(1,2) + "C";
    const std::string dest = "/tmp/" + catalog_hash.ToString();

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
      return download::Fetch(&head) == download::kFailOk;
    } else {
      return FileExists(file);
    }
  }

 private:
  T                *delegate_;
  Callback          catalog_callback_;
  const std::string repo_url_;
  const std::string repo_name_;
  const std::string repo_keys_;
  const bool        is_remote_;
  CatalogJobStack   catalog_stack_;
};

}
