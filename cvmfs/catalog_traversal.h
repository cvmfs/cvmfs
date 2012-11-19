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

namespace swissknife {

/**
 * This class traverses the complete catalog hierarchy of a CVMFS repository
 * recursively. The user can specify a callback which is called for each catalog
 * that was hit on the way.
 * Note: Since all CVMFS catalog files together can grow to several gigabytes in
 *       file size, each catalog is loaded, processed and removed immediately
 *       afterwards.
 */
template<class T>
class CatalogTraversal
{
 public:
  /**
   * Callback signature which has to be implemented by the delegate object
   * @param catalog             the catalog object which needs to be processed
   * @param recursion_depth     the depth in the nested catalog tree 
   *                            (starting at zero)
   */
  typedef void (T::*Callback)(const catalog::Catalog* catalog,
                              const unsigned recursion_depth);


 protected:
  typedef struct CatalogJob_ {
    CatalogJob_(const std::string& path,
                const hash::Any&   hash,
                const unsigned     recursion_depth) :
      path(path),
      hash(hash),
      recursion_depth(recursion_depth) {}
    CatalogJob_(const catalog::Catalog::NestedCatalog& nested_catalog,
                const unsigned                         recursion_depth) :
      path(nested_catalog.path.ToString()),
      hash(nested_catalog.hash),
      recursion_depth(recursion_depth) {}

    const std::string path;
    const hash::Any   hash;
    const unsigned    recursion_depth;
  } CatalogJob;
  typedef std::stack<CatalogJob> CatalogJobStack;


 public:

  /**
   * Constructs a new catalog traversal engine.
   * @param delegate           the object to be notified when a catalog needs 
   *                           to be processed
   * @param catalog_callback   a function pointer to the callback to be called
   *                           on the delegate object
   * @param repository         the path to the repository to be traversed:
   *                           -> either absolute path to the local catalogs
   *                           -> or an URL to a remote repository
   */
	CatalogTraversal(T*                 delegate,
                   Callback           catalog_callback,
                   const std::string& repository) :
    delegate_(delegate),
    catalog_callback_(catalog_callback),
    repository_(MakeCanonicalPath(repository)),
    is_remote_(repository.substr(0, 7) == "http://")
  {
    if (is_remote_)
      download::Init(1);
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
    if (!manifest)
      return false;

    // add the root catalog of the repository as the first element on the job
    // stack
    CatalogJob job("", manifest->catalog_hash(), 0);
    catalog_stack_.push(job);

    // start the magic
    return TraverseRecursively();
  }


 protected:
  bool TraverseRecursively() {

    // the CatalogTraversal works with a stack, where new nested catalogs are
    // pushed onto while processing their parent.
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


  bool ProcessCatalogJob(const CatalogJob &job)
  {
    // load a catalog
    const std::string tmp_file = LoadAndExtractCatalog(job.hash);
    if (tmp_file.empty()) {
      LogCvmfs(kLogCvmfs, kLogStdout, "failed to load catalog %s",
               job.hash.ToString().c_str());
      return false;
    }

    // open the catalog
    catalog::Catalog *catalog = catalog::AttachFreely(job.path, tmp_file);
    unlink(tmp_file.c_str());
    if (catalog == NULL) {
      LogCvmfs(kLogCvmfs, kLogStdout, "failed to open catalog %s",
               job.hash.ToString().c_str());
      return false;
    }

    // check if we got the correct catalog here
    if (catalog->root_prefix() != PathString(job.path.data(), job.path.length())) {
      LogCvmfs(kLogCvmfs, kLogStderr, "root prefix mismatch; "
               "expected %s, got %s",
               job.path.c_str(), catalog->root_prefix().c_str());
      return false;
    }

    // provide the user with the catalog
    (delegate_->*catalog_callback_)(catalog, job.recursion_depth);

    // Inception! Go to the next recursion level
    catalog::Catalog::NestedCatalogList *nested_catalogs =
      catalog->ListNestedCatalogs();
    for (catalog::Catalog::NestedCatalogList::const_iterator i =
         nested_catalogs->begin(), iEnd = nested_catalogs->end(); 
         i != iEnd; ++i) 
    {
      catalog_stack_.push(CatalogJob(*i, job.recursion_depth + 1));
    }

    // sucessfully traversed
    return true;
  }


  manifest::Manifest* LoadManifest() {
    manifest::Manifest *manifest = NULL;

    // grab manifest file
    if (!is_remote_) {
      // locally
      manifest = manifest::Manifest::LoadFile(repository_ + "/.cvmfspublished");

    } else {
      // remote
      const std::string url = repository_ + "/.cvmfspublished";

      // download manifest file
      download::JobInfo download_manifest(&url, false, false, NULL);
      download::Failures retval = download::Fetch(&download_manifest);
      if (retval != download::kFailOk) {
        LogCvmfs(kLogCvmfs, kLogStderr, "failed to download manifest (%d)",
                 retval);
        return NULL;
      }

      // load manifest
      char *buffer = download_manifest.destination_mem.data;
      const unsigned length = download_manifest.destination_mem.size;
      manifest = manifest::Manifest::LoadMem(
        reinterpret_cast<const unsigned char *>(buffer), length);

      free(download_manifest.destination_mem.data);
    }

    // Validate Manifest
    if (!manifest) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to load repository manifest");
      return NULL;
    }

    const std::string certificate_path =
      repository_ + "/data" + manifest->certificate().MakePath(1, 2) + "X";
    if (!Exists(certificate_path)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "failed to find certificate (%s)",
               certificate_path.c_str());
      return NULL;
    }

    return manifest;
  }


  inline std::string LoadAndExtractCatalog(const hash::Any& catalog_hash) {
    return (is_remote_) ? DownloadCatalog(catalog_hash)
                        : DecompressCatalog(catalog_hash);
  }


  /**
   * Downloads a catalog from a remote repository and extracts it in one shot
   * @param catalog_hash   the SHA1 hash of the catalog to be downloaded
   * @return               the path to the downloaded and extracted catalog file
   */
  std::string DownloadCatalog(const hash::Any& catalog_hash) {
    const std::string source = "data" + catalog_hash.MakePath(1,2) + "C";
    const std::string dest = "/tmp/" + catalog_hash.ToString();
    const std::string url = repository_ + "/" + source;

    download::JobInfo download_catalog(&url, true, false, &dest, &catalog_hash);
    download::Failures retval = download::Fetch(&download_catalog);

    if (retval != download::kFailOk) {
      LogCvmfs(kLogCvmfs, kLogStdout, "failed to download catalog %s (%d)",
             catalog_hash.ToString().c_str(), retval);
      return "";
    }

    return dest;
  }


  /**
   * Decompresses a catalog that resides on local storage.
   * @param catalog_hash   the SHA1 hash of the catalog to be extracted
   * @return               the path to the extracted catalog file
   */
  std::string DecompressCatalog(const hash::Any& catalog_hash) {
    const std::string source = 
      repository_ + "/data" + catalog_hash.MakePath(1,2) + "C";
    const std::string dest = "/tmp/" + catalog_hash.ToString();

    if (!zlib::DecompressPath2Path(source, dest))
      return "";

    return dest;
  }


  /**
   * Checks if a file exists, both remotely or locally, depending on the type
   * of repository currently traversed
   * @param file   the file to be checked for existence
   * @return       true if the file exists, false otherwise
   */
  bool Exists(const std::string &file) {
    if (is_remote_){
      const std::string url = repository_ + "/" + file;
      download::JobInfo head(&url, false);
      return download::Fetch(&head) == download::kFailOk;
    }
    else
      return FileExists(file);
  }

 private:
  T                *delegate_;
  Callback          catalog_callback_;
  const std::string repository_;
  const bool        is_remote_;
  CatalogJobStack   catalog_stack_;
};

}
