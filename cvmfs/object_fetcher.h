/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_OBJECT_FETCHER_H_
#define CVMFS_OBJECT_FETCHER_H_

#include <unistd.h>

#include <string>

#include "catalog.h"
#include "crypto/signature.h"
#include "history_sqlite.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "network/download.h"
#include "reflog.h"
#include "util/posix.h"

/**
 * Trait class to define the concrete object types produced by the methods of
 * concrete instantiations of AbstractObjectFetcher<>. For each implementation
 * of AbstractObjectFetcher<> one needs to provide a specialisation of this
 * trait. Note that this specialisation can be templated with the actual para-
 * meters, hence the parameter space does not explode.
 *
 * See: http://stackoverflow.com/questions/6006614/
 *      c-static-polymorphism-crtp-and-using-typedefs-from-derived-classes
 */
template<class ConcreteObjectFetcherT>
struct object_fetcher_traits;

struct ObjectFetcherFailures {
  enum Failures {
    kFailOk,
    kFailNotFound,
    kFailLocalIO,
    kFailNetwork,
    kFailDecompression,
    kFailManifestNameMismatch,
    kFailManifestSignatureMismatch,
    kFailBadData,
    kFailUnknown,

    kFailNumEntries
  };
};

inline const char *Code2Ascii(const ObjectFetcherFailures::Failures error) {
  const char *texts[ObjectFetcherFailures::kFailNumEntries + 1];
  texts[0] = "OK";
  texts[1] = "object not found";
  texts[2] = "local I/O failure";
  texts[3] = "network failure";
  texts[4] = "decompression failed";
  texts[5] = "manifest name doesn't match";
  texts[6] = "manifest signature is invalid";
  texts[7] = "bad data received";
  texts[8] = "no text";
  return texts[error];
}

/**
 * This is the default class implementing the data object fetching strategy. It
 * is meant to be used when CVMFS specific data structures need to be downloaded
 * from a backend storage of a repository.
 *
 * ObjectFetchers are supposed to be configured for one specific repository. How
 * this is done depends on the concrete implementation of this base class. When
 * a concrete implementation of ObjectFetcher<> needs to deal with files on the
 * local file system it is obliged to take measures for proper cleanup of those
 * files after usage.
 *
 * It abstracts all accesses to external file or HTTP resources and gathers this
 * access logic in one central point. This also comes in handy when unit testing
 * components that depend on downloading CVMFS data structures from a repository
 * backend storage like CatalogTraversal<> or GarbageCollector<>.
 */
template<class DerivedT>
class AbstractObjectFetcher : public ObjectFetcherFailures {
 public:
  typedef typename object_fetcher_traits<DerivedT>::CatalogTN CatalogTN;
  typedef typename object_fetcher_traits<DerivedT>::HistoryTN HistoryTN;
  typedef typename object_fetcher_traits<DerivedT>::ReflogTN ReflogTN;

  typedef ObjectFetcherFailures::Failures Failures;

  static const std::string kManifestFilename;
  static const std::string kReflogFilename;

 public:
  /**
   * Fetches and opens the manifest of the repository this object fetcher is
   * configured for. Note that the user is responsible to clean up this object.
   *
   * @param manifest  pointer to a manifest object pointer
   * @return          failure code, specifying the action's result
   */
  Failures FetchManifest(manifest::Manifest **manifest) {
    return static_cast<DerivedT *>(this)->FetchManifest(manifest);
  }

  /**
   * Downloads and opens (read-only) a history database. Note that the user is
   * responsible to remove the history object after usage. The fetched SQLite
   * database file will be unlinked automatically during the destruction of the
   * HistoryTN object.
   *
   * @param history       pointer to a history database object pointer
   * @param history_hash  (optional) the content hash of the history database
   *                                 if left blank, the latest one is downloaded
   * @return              failure code, specifying the action's result
   */
  Failures FetchHistory(HistoryTN **history,
                        const shash::Any &history_hash = shash::Any()) {
    // retrieve the current HEAD history hash (if nothing else given)
    shash::Any effective_history_hash = (!history_hash.IsNull())
                                            ? history_hash
                                            : GetHistoryHash();
    if (effective_history_hash.IsNull()) {
      return kFailNotFound;
    }
    assert(history_hash.suffix == shash::kSuffixHistory
           || history_hash.IsNull());

    // download the history hash
    std::string path;
    const Failures retval = Fetch(effective_history_hash, &path);
    if (retval != kFailOk) {
      return retval;
    }

    // open the history file
    *history = HistoryTN::Open(path);
    if (NULL == *history) {
      return kFailLocalIO;
    }

    (*history)->TakeDatabaseFileOwnership();
    return kFailOk;
  }

  /**
   * Downloads and opens a catalog. Note that the user is responsible to remove
   * the catalog object after usage.
   *
   * @param catalog_hash   the content hash of the catalog object
   * @param catalog_path   the root_path the catalog is mounted on
   * @param catalog        pointer to the fetched catalog object pointer
   * @param is_nested      a hint if the catalog to be loaded is a nested one
   * @param parent         (optional) parent catalog of the requested catalog
   * @return               failure code, specifying the action's result
   */
  Failures FetchCatalog(const shash::Any &catalog_hash,
                        const std::string &catalog_path,
                        CatalogTN **catalog,
                        const bool is_nested = false,
                        CatalogTN *parent = NULL) {
    assert(!catalog_hash.IsNull());
    assert(catalog_hash.suffix == shash::kSuffixCatalog);

    std::string path;
    const Failures retval = Fetch(catalog_hash, &path);
    if (retval != kFailOk) {
      return retval;
    }

    *catalog = CatalogTN::AttachFreely(catalog_path, path, catalog_hash, parent,
                                       is_nested);
    if (NULL == *catalog) {
      return kFailLocalIO;
    }

    (*catalog)->TakeDatabaseFileOwnership();
    return kFailOk;
  }

  Failures FetchReflog(const shash::Any &reflog_hash, ReflogTN **reflog) {
    assert(!reflog_hash.IsNull());
    assert(reflog_hash.suffix == shash::kSuffixNone);

    std::string tmp_path;
    const bool decompress = false;
    const bool nocache = true;
    Failures failure = Fetch(kReflogFilename, decompress, nocache, &tmp_path);
    if (failure != kFailOk) {
      return failure;
    }

    // Ensure data integrity
    shash::Any computed_hash(reflog_hash.algorithm);
    ReflogTN::HashDatabase(tmp_path, &computed_hash);
    if (computed_hash != reflog_hash) {
      unlink(tmp_path.c_str());
      return kFailBadData;
    }

    *reflog = ReflogTN::Open(tmp_path);
    if (NULL == *reflog) {
      return kFailLocalIO;
    }

    (*reflog)->TakeDatabaseFileOwnership();
    return kFailOk;
  }

  Failures FetchManifest(UniquePtr<manifest::Manifest> *manifest) {
    manifest::Manifest *raw_manifest_ptr = NULL;
    Failures failure = FetchManifest(&raw_manifest_ptr);
    if (failure == kFailOk)
      *manifest = raw_manifest_ptr;
    return failure;
  }

  Failures FetchHistory(UniquePtr<HistoryTN> *history,
                        const shash::Any &history_hash = shash::Any()) {
    HistoryTN *raw_history_ptr = NULL;
    Failures failure = FetchHistory(&raw_history_ptr, history_hash);
    if (failure == kFailOk)
      *history = raw_history_ptr;
    return failure;
  }

  Failures FetchCatalog(const shash::Any &catalog_hash,
                        const std::string &catalog_path,
                        UniquePtr<CatalogTN> *catalog,
                        const bool is_nested = false,
                        CatalogTN *parent = NULL) {
    CatalogTN *raw_catalog_ptr = NULL;
    Failures failure = FetchCatalog(catalog_hash, catalog_path,
                                    &raw_catalog_ptr, is_nested, parent);
    if (failure == kFailOk)
      *catalog = raw_catalog_ptr;
    return failure;
  }

  Failures FetchReflog(const shash::Any &reflog_hash,
                       UniquePtr<ReflogTN> *reflog) {
    ReflogTN *raw_reflog_ptr = NULL;
    Failures failure = FetchReflog(reflog_hash, &raw_reflog_ptr);
    if (failure == kFailOk)
      *reflog = raw_reflog_ptr;
    return failure;
  }

  std::string GetUrl(const shash::Any &hash) const {
    return static_cast<DerivedT *>(this)->GetUrl(hash);
  }

  bool HasHistory() {
    shash::Any history_hash = GetHistoryHash();
    return !history_hash.IsNull();
  }

  const std::string &temporary_directory() const {
    return temporary_directory_;
  }

 protected:
  explicit AbstractObjectFetcher(const std::string &temp_dir)
      : temporary_directory_(temp_dir) { }

  /**
   * Internal function used to download objects defined by the given content
   * hash. This needs to be implemented depending on the concrete implementation
   * of this base class.
   *
   * @param object_hash  the content hash of the object to be downloaded
   * @param file_path    temporary file path to store the download result
   * @return             failure code (if not kFailOk, file_path is invalid)
   */
  Failures Fetch(const shash::Any &object_hash, std::string *file_path) {
    return static_cast<DerivedT *>(this)->Fetch(object_hash, file_path);
  }

  Failures Fetch(const std::string &relative_path,
                 const bool decompress,
                 const bool nocache,
                 std::string *file_path) {
    return static_cast<DerivedT *>(this)->Fetch(relative_path, decompress,
                                                nocache, file_path);
  }

  /**
   * Retrieves the history content hash of the HEAD history database from the
   * repository's manifest
   *
   * @return  the content hash of the HEAD history db or a null-hash on error
   */
  shash::Any GetHistoryHash() {
    UniquePtr<manifest::Manifest> manifest;
    const Failures retval = FetchManifest(&manifest);

    if (retval != kFailOk || !manifest.IsValid()
        || manifest->history().IsNull()) {
      return shash::Any();
    }

    return manifest->history();
  }

 private:
  const std::string temporary_directory_;
};

template<class DerivedT>
const std::string
    AbstractObjectFetcher<DerivedT>::kManifestFilename = ".cvmfspublished";
template<class DerivedT>
const std::string
    AbstractObjectFetcher<DerivedT>::kReflogFilename = ".cvmfsreflog";


/**
 * This is an AbstractObjectFetcher<> accessing locally stored repository files.
 * Note that this implementation does not take care of any repository signature
 * verification.
 */
template<class CatalogT = catalog::Catalog,
         class HistoryT = history::SqliteHistory,
         class ReflogT = manifest::Reflog>
class LocalObjectFetcher
    : public AbstractObjectFetcher<
          LocalObjectFetcher<CatalogT, HistoryT, ReflogT> > {
 protected:
  typedef LocalObjectFetcher<CatalogT, HistoryT, ReflogT> ThisTN;
  typedef AbstractObjectFetcher<ThisTN> BaseTN;

 public:
  typedef typename BaseTN::Failures Failures;

 public:
  /**
   * LocalObjectFetcher can reside on the stack or the heap.
   *
   * @param base_path  the path to the repository's backend storage
   * @param temp_dir   location to store decompressed tmp data
   */
  LocalObjectFetcher(const std::string &base_path, const std::string &temp_dir)
      : BaseTN(temp_dir), base_path_(base_path) { }

  using BaseTN::FetchManifest;  // un-hiding convenience overload
  Failures FetchManifest(manifest::Manifest **manifest) {
    const std::string path = BuildPath(BaseTN::kManifestFilename);
    if (!FileExists(path)) {
      return BaseTN::kFailNotFound;
    }

    *manifest = manifest::Manifest::LoadFile(path);
    return (*manifest != NULL) ? BaseTN::kFailOk : BaseTN::kFailUnknown;
  }

  std::string GetUrl(const shash::Any &hash) const {
    return "file://" + BuildPath(BuildRelativePath(hash));
  }

  Failures Fetch(const shash::Any &object_hash, std::string *file_path) {
    assert(file_path != NULL);
    file_path->clear();

    const std::string relative_path = BuildRelativePath(object_hash);
    const bool decompress = true;
    const bool nocache = false;
    return Fetch(relative_path, decompress, nocache, file_path);
  }


  Failures Fetch(const std::string &relative_path,
                 const bool decompress,
                 const bool /* nocache */,
                 std::string *file_path) {
    assert(file_path != NULL);
    file_path->clear();

    // check if the requested file object is available locally
    const std::string source = BuildPath(relative_path);
    if (!FileExists(source)) {
      LogCvmfs(kLogDownload, kLogDebug, "failed to locate file '%s'",
               relative_path.c_str());
      return BaseTN::kFailNotFound;
    }

    // create a temporary file to store the (decompressed) object file
    const std::string tmp_path = BaseTN::temporary_directory() + "/"
                                 + GetFileName(relative_path);
    FILE *f = CreateTempFile(tmp_path, 0600, "w", file_path);
    if (NULL == f) {
      LogCvmfs(kLogDownload, kLogStderr,
               "failed to create temp file '%s' (errno: %d)", tmp_path.c_str(),
               errno);
      return BaseTN::kFailLocalIO;
    }

    // decompress or copy the requested object file
    const bool success = (decompress) ? zlib::DecompressPath2File(source, f)
                                      : CopyPath2File(source, f);
    fclose(f);

    // check the decompression success and remove the temporary file otherwise
    if (!success) {
      LogCvmfs(kLogDownload, kLogDebug,
               "failed to fetch file from '%s' "
               "to '%s' (errno: %d)",
               source.c_str(), file_path->c_str(), errno);
      unlink(file_path->c_str());
      file_path->clear();
      return BaseTN::kFailDecompression;
    }

    return BaseTN::kFailOk;
  }

 protected:
  std::string BuildPath(const std::string &relative_path) const {
    return base_path_ + "/" + relative_path;
  }

  std::string BuildRelativePath(const shash::Any &hash) const {
    return "data/" + hash.MakePath();
  }

 private:
  const std::string base_path_;
};

template<class CatalogT, class HistoryT, class ReflogT>
struct object_fetcher_traits<LocalObjectFetcher<CatalogT, HistoryT, ReflogT> > {
  typedef CatalogT CatalogTN;
  typedef HistoryT HistoryTN;
  typedef ReflogT ReflogTN;
};


/**
 * This implements the AbstractObjectFetcher<> to retrieve repository objects
 * from a remote location through HTTP. It verifies the repository's signature
 * and the downloaded data integrity.
 */
template<class CatalogT = catalog::Catalog,
         class HistoryT = history::SqliteHistory,
         class ReflogT = manifest::Reflog>
class HttpObjectFetcher : public AbstractObjectFetcher<
                              HttpObjectFetcher<CatalogT, HistoryT, ReflogT> > {
 protected:
  typedef HttpObjectFetcher<CatalogT, HistoryT, ReflogT> ThisTN;
  typedef AbstractObjectFetcher<ThisTN> BaseTN;

 public:
  typedef typename BaseTN::Failures Failures;

 public:
  /**
   * HttpObjectFetcher<> contains external DownloadManager and SignatureManager
   * hence it essentially is a wrapper object and can be copied.
   *
   * @param repo_name      the name of the repository to download objects from
   * @param repo_url       the URL to the repository's backend storage
   * @param temp_dir       location to store decompressed tmp data
   * @param download_mgr   pointer to the download manager to be used
   * @param signature_mgr  pointer to the signature manager to be used
   *
   * @return               a HttpObjectFetcher<> object or NULL on error
   */
  HttpObjectFetcher(const std::string &repo_name,
                    const std::string &repo_url,
                    const std::string &temp_dir,
                    download::DownloadManager *download_mgr,
                    signature::SignatureManager *signature_mgr)
      : BaseTN(temp_dir)
      , repo_url_(repo_url)
      , repo_name_(repo_name)
      , download_manager_(download_mgr)
      , signature_manager_(signature_mgr) { }

 public:
  using BaseTN::FetchManifest;  // un-hiding convenience overload
  Failures FetchManifest(manifest::Manifest **manifest) {
    const std::string url = BuildUrl(BaseTN::kManifestFilename);

    // Download manifest file
    struct manifest::ManifestEnsemble manifest_ensemble;
    manifest::Failures retval = manifest::Fetch(repo_url_,
                                                repo_name_,
                                                0,
                                                NULL,
                                                signature_manager_,
                                                download_manager_,
                                                &manifest_ensemble);

    // Check if manifest was loaded correctly
    switch (retval) {
      case manifest::kFailOk:
        break;

      case manifest::kFailNameMismatch:
        LogCvmfs(kLogDownload, kLogDebug,
                 "repository name mismatch. No name provided?");
        return BaseTN::kFailManifestNameMismatch;

      case manifest::kFailBadSignature:
      case manifest::kFailBadCertificate:
      case manifest::kFailBadWhitelist:
        LogCvmfs(kLogDownload, kLogDebug,
                 "repository signature mismatch. No key(s) provided?");
        return BaseTN::kFailManifestSignatureMismatch;

      default:
        LogCvmfs(kLogDownload, kLogDebug, "failed to load manifest (%d - %s)",
                 retval, Code2Ascii(retval));
        return BaseTN::kFailUnknown;
    }

    assert(retval == manifest::kFailOk);
    *manifest = new manifest::Manifest(*manifest_ensemble.manifest);
    return (*manifest != NULL) ? BaseTN::kFailOk : BaseTN::kFailUnknown;
  }

  std::string GetUrl(const shash::Any &hash) const {
    return BuildUrl(BuildRelativeUrl(hash));
  }

  Failures Fetch(const shash::Any &object_hash, std::string *object_file) {
    assert(object_file != NULL);
    assert(!object_hash.IsNull());

    const bool decompress = true;
    const bool nocache = false;
    const std::string url = BuildRelativeUrl(object_hash);
    return Download(url, decompress, nocache, &object_hash, object_file);
  }

  Failures Fetch(const std::string &relative_path,
                 const bool decompress,
                 const bool nocache,
                 std::string *file_path) {
    const shash::Any *expected_hash = NULL;
    return Download(relative_path, decompress, nocache, expected_hash,
                    file_path);
  }

 protected:
  std::string BuildUrl(const std::string &relative_path) const {
    return repo_url_ + "/" + relative_path;
  }

  std::string BuildRelativeUrl(const shash::Any &hash) const {
    return "data/" + hash.MakePath();
  }

  Failures Download(const std::string &relative_path,
                    const bool decompress,
                    const bool nocache,
                    const shash::Any *expected_hash,
                    std::string *file_path) {
    file_path->clear();

    // create temporary file to host the fetching result
    const std::string tmp_path = BaseTN::temporary_directory() + "/"
                                 + GetFileName(relative_path);
    FILE *f = CreateTempFile(tmp_path, 0600, "w", file_path);
    if (NULL == f) {
      LogCvmfs(kLogDownload, kLogStderr,
               "failed to create temp file '%s' (errno: %d)", tmp_path.c_str(),
               errno);
      return BaseTN::kFailLocalIO;
    }

    // fetch and decompress the requested object
    const std::string url = BuildUrl(relative_path);
    const bool probe_hosts = false;
    cvmfs::FileSink filesink(f);
    download::JobInfo download_job(&url, decompress, probe_hosts, expected_hash,
                                   &filesink);
    download_job.SetForceNocache(nocache);
    download::Failures retval = download_manager_->Fetch(&download_job);
    const bool success = (retval == download::kFailOk);
    fclose(f);

    // check if download worked and remove temporary file if not
    if (!success) {
      LogCvmfs(kLogDownload, kLogDebug,
               "failed to download file "
               "%s to '%s' (%d - %s)",
               relative_path.c_str(), file_path->c_str(), retval,
               Code2Ascii(retval));
      unlink(file_path->c_str());
      file_path->clear();

      // hand out the error status
      switch (retval) {
        case download::kFailLocalIO:
          return BaseTN::kFailLocalIO;

        case download::kFailBadUrl:
        case download::kFailProxyResolve:
        case download::kFailHostResolve:
        case download::kFailUnsupportedProtocol:
          LogCvmfs(kLogDownload, kLogDebug | kLogStderr,
                   "HTTP connection error %d: %s", retval, url.c_str());
          return BaseTN::kFailNetwork;

        case download::kFailProxyHttp:
        case download::kFailHostHttp:
          if (download_job.http_code() == 404)
            return BaseTN::kFailNotFound;
          LogCvmfs(kLogDownload, kLogDebug | kLogStderr,
                   "HTTP protocol error %d: %s (%d)", download_job.http_code(),
                   url.c_str(), retval);
          return BaseTN::kFailNetwork;

        case download::kFailBadData:
        case download::kFailTooBig:
          return BaseTN::kFailBadData;

        default:
          if (download::IsProxyTransferError(retval)
              || download::IsHostTransferError(retval)) {
            LogCvmfs(kLogDownload, kLogDebug | kLogStderr,
                     "HTTP transfer error %d (HTTP code %d): %s", retval,
                     download_job.http_code(), url.c_str());
            return BaseTN::kFailNetwork;
          }
          return BaseTN::kFailUnknown;
      }
    }

    assert(success);
    return BaseTN::kFailOk;
  }

 private:
  const std::string repo_url_;
  const std::string repo_name_;
  download::DownloadManager *download_manager_;
  signature::SignatureManager *signature_manager_;
};

template<class CatalogT, class HistoryT, class ReflogT>
struct object_fetcher_traits<HttpObjectFetcher<CatalogT, HistoryT, ReflogT> > {
  typedef CatalogT CatalogTN;
  typedef HistoryT HistoryTN;
  typedef ReflogT ReflogTN;
};

#endif  // CVMFS_OBJECT_FETCHER_H_
