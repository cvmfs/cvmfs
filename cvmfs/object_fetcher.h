/**
 * This file is part of the CernVM File System.
 */

#ifndef OBJECT_FETCHER_H
#define OBJECT_FETCHER_H

#include <string>

#include "catalog.h"
#include "download.h"
#include "history_sqlite.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "signature.h"

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
template <class ConcreteObjectFetcherT>
struct object_fetcher_traits;

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
template <class DerivedT>
class AbstractObjectFetcher {
 public:
  typedef typename object_fetcher_traits<DerivedT>::CatalogTN CatalogTN;
  typedef typename object_fetcher_traits<DerivedT>::HistoryTN HistoryTN;

  static const std::string kManifestFilename;

 public:
  /**
   * Fetches and opens the manifest of the repository this object fetcher is
   * configured for. Note that the user is responsible to clean up this object.
   *
   * @return  a manifest object or NULL on error
   */
  manifest::Manifest* FetchManifest() {
    return static_cast<DerivedT*>(this)->FetchManifest();
  }

  /**
   * Downloads and opens (read-only) a history database. Note that the user is
   * responsible to remove the history object after usage. The fetched SQLite
   * database file will be unlinked automatically during the destruction of the
   * HistoryTN object.
   *
   * @param history_hash  (optional) the content hash of the history database
   *                                 if left blank, the latest one is downloaded
   * @return              a history database object or NULL on error
   */
  HistoryTN* FetchHistory(const shash::Any &history_hash = shash::Any()) {
    // retrieve the current HEAD history hash (if nothing else given)
    shash::Any effective_history_hash = (! history_hash.IsNull())
            ? history_hash
            : GetHistoryHash();

    // download the history hash
    std::string path;
    if (effective_history_hash.IsNull() ||
        ! Fetch(effective_history_hash, shash::kSuffixHistory, &path)) {
      return NULL;
    }

    // open the history file
    HistoryTN *history = HistoryTN::Open(path);
    if (NULL != history) {
      history->TakeDatabaseFileOwnership();
    }

    return history;
  }

  /**
   * Downloads and opens a catalog. Note that the user is responsible to remove
   * the catalog object after usage.
   *
   * @param catalog_hash   the content hash of the catalog object
   * @param catalog_path   the root_path the catalog is mounted on
   * @param is_nested      a hint if the catalog to be loaded is a nested one
   * @param parent         (optional) parent catalog of the requested catalog
   * @return               a catalog object or NULL on error
   */
  CatalogTN* FetchCatalog(const shash::Any  &catalog_hash,
                          const std::string &catalog_path,
                          const bool         is_nested = false,
                                CatalogTN   *parent    = NULL) {
    assert (! catalog_hash.IsNull());

    std::string path;
    if (! Fetch(catalog_hash, shash::kSuffixCatalog, &path)) {
      return NULL;
    }

    CatalogTN *catalog = CatalogTN::AttachFreely(catalog_path,
                                                 path,
                                                 catalog_hash,
                                                 parent,
                                                 is_nested);
    if (NULL != catalog) {
      catalog->TakeDatabaseFileOwnership();
    }

    return catalog;
  }

 public:
  bool HasHistory() {
    shash::Any history_hash = GetHistoryHash();
    return ! history_hash.IsNull();
  }

 protected:
  /**
   * Internal function used to download objects defined by the given content
   * hash. This needs to be implemented depending on the concrete implementation
   * of this base class.
   *
   * @param object_hash  the content hash of the object to be downloaded
   * @param hash_suffix  the (optional) hash suffix of the object to be fetched
   * @param file_path    temporary file path to store the download result
   * @return             true on success (if false, file_path is invalid)
   */
  bool Fetch(const shash::Any    &object_hash,
             const shash::Suffix  hash_suffix,
             std::string         *file_path) {
    return static_cast<DerivedT*>(this)->Fetch(object_hash,
                                               hash_suffix,
                                               file_path);
  }

  /**
   * Retrieves the history content hash of the HEAD history database from the
   * repository's manifest
   *
   * @return  the content hash of the HEAD history db or a null-hash on error
   */
  shash::Any GetHistoryHash() {
    UniquePtr<manifest::Manifest> manifest(FetchManifest());
    if (! manifest || manifest->history().IsNull()) {
      return shash::Any();
    }

    return manifest->history();
  }
};

template <class DerivedT>
const std::string AbstractObjectFetcher<DerivedT>::kManifestFilename =
                                                              ".cvmfspublished";


/**
 * This is an AbstractObjectFetcher<> accessing locally stored repository files.
 * Note that this implementation does not take care of any repository signature
 * verification.
 */
template <class CatalogT = catalog::Catalog,
          class HistoryT = history::SqliteHistory>
class LocalObjectFetcher :
  public AbstractObjectFetcher<LocalObjectFetcher<CatalogT, HistoryT> >
{
 protected:
  typedef LocalObjectFetcher<CatalogT, HistoryT> ThisTN;
  typedef AbstractObjectFetcher<ThisTN>          BaseTN;

 public:
  /**
   * LocalObjectFetcher can reside on the stack or the heap.
   *
   * @param base_path  the path to the repository's backend storage
   * @param temp_dir   location to store decompressed tmp data
   */
  LocalObjectFetcher(const std::string &base_path,
                     const std::string &temp_dir)
    : base_path_(base_path)
    , temporary_directory_(temp_dir) {}

  manifest::Manifest* FetchManifest() {
    return manifest::Manifest::LoadFile(BuildPath(BaseTN::kManifestFilename));
  }

  bool Fetch(const shash::Any    &object_hash,
             const shash::Suffix  hash_suffix,
             std::string         *file_path) {
    assert (file_path != NULL);
    file_path->clear();

    const std::string source = BuildPath(object_hash, hash_suffix);
    const std::string dest   = CreateTempPath(temporary_directory_ + "/" +
                                              object_hash.ToStringWithSuffix(),
                                              0600);

    if (! FileExists(source)) {
      LogCvmfs(kLogDownload, kLogDebug, "failed to locate object %s at '%s'",
               object_hash.ToString().c_str(), dest.c_str());
      return false;
    }

    if (! zlib::DecompressPath2Path(source, dest)) {
      LogCvmfs(kLogDownload, kLogDebug, "failed to extract object %s from '%s' "
                                        "to '%s' (errno: %d)",
               object_hash.ToString().c_str(), source.c_str(), dest.c_str(),
               errno);
      return false;
    }

    *file_path = dest;
    return true;
  }


 protected:
  std::string BuildPath(const std::string &relative_path) const {
    return base_path_ + "/" + relative_path;
  }

  std::string BuildPath(const shash::Any    &hash,
                        const shash::Suffix  suffix) const {
    return BuildPath("data" + hash.MakePathWithSuffix(1, 2, suffix));
  }

 private:
  const std::string base_path_;
  const std::string temporary_directory_;
};

template <class CatalogT, class HistoryT>
struct object_fetcher_traits<LocalObjectFetcher<CatalogT, HistoryT> > {
    typedef CatalogT CatalogTN;
    typedef HistoryT HistoryTN;
};


/**
 * This implements the AbstractObjectFetcher<> to retrieve repository objects
 * from a remote location through HTTP. It verifies the repository's signature
 * and the downloaded data integrity.
 */
template <class CatalogT = catalog::Catalog,
          class HistoryT = history::SqliteHistory>
class HttpObjectFetcher :
  public AbstractObjectFetcher<HttpObjectFetcher<CatalogT, HistoryT> >
{
 protected:
  typedef HttpObjectFetcher<CatalogT, HistoryT>  ThisTN;
  typedef AbstractObjectFetcher<ThisTN>          BaseTN;

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
  HttpObjectFetcher(const std::string           &repo_name,
                    const std::string           &repo_url,
                    const std::string           &temp_dir,
                    download::DownloadManager   *download_mgr,
                    signature::SignatureManager *signature_mgr) :
    repo_url_(repo_url), repo_name_(repo_name), temporary_directory_(temp_dir),
    download_manager_(download_mgr), signature_manager_(signature_mgr) {}

 public:
  manifest::Manifest* FetchManifest() {
    manifest::Manifest *manifest = NULL;

    const std::string url = BuildUrl(BaseTN::kManifestFilename);

    // Download manifest file
    struct manifest::ManifestEnsemble manifest_ensemble;
    manifest::Failures retval = manifest::Fetch(
                                  repo_url_,
                                  repo_name_,
                                  0,
                                  NULL,
                                  signature_manager_,
                                  download_manager_,
                                  &manifest_ensemble);

    // Check if manifest was loaded correctly
    if (retval == manifest::kFailOk) {
      manifest = new manifest::Manifest(*manifest_ensemble.manifest);
    } else if (retval == manifest::kFailNameMismatch) {
      LogCvmfs(kLogDownload, kLogDebug,
               "repository name mismatch. No name provided?");
    } else if (retval == manifest::kFailBadSignature   ||
               retval == manifest::kFailBadCertificate ||
               retval == manifest::kFailBadWhitelist) {
      LogCvmfs(kLogDownload, kLogDebug,
               "repository signature mismatch. No key(s) provided?");
    } else {
      LogCvmfs(kLogDownload, kLogDebug,
               "failed to load manifest (%d - %s)",
               retval, Code2Ascii(retval));
    }

    return manifest;
  }

  bool Fetch(const shash::Any     &object_hash,
             const shash::Suffix   hash_suffix,
             std::string          *object_file) {
    assert (object_file != NULL);
    assert (! object_hash.IsNull());

    object_file->clear();

    const std::string url  = BuildUrl(object_hash, hash_suffix);
    const std::string dest = CreateTempPath(temporary_directory_ + "/" +
                                            object_hash.ToStringWithSuffix(),
                                            0600);

    download::JobInfo download_catalog(&url, true, false, &dest, &object_hash);
    download::Failures retval = download_manager_->Fetch(&download_catalog);

    if (retval != download::kFailOk) {
      LogCvmfs(kLogDownload, kLogDebug, "failed to download object "
                                        "%s (%d - %s)",
               object_hash.ToString().c_str(), retval, Code2Ascii(retval));
    }

    *object_file = dest;
    return retval == download::kFailOk;
  }

 protected:
  std::string BuildUrl(const std::string &relative_path) const {
    return repo_url_ + "/" + relative_path;
  }

  std::string BuildUrl(const shash::Any     &hash,
                       const shash::Suffix  &suffix) const {
    return BuildUrl("data" + hash.MakePathWithSuffix(1, 2, suffix));
  }

 private:
  const std::string            repo_url_;
  const std::string            repo_name_;
  const std::string            temporary_directory_;
  download::DownloadManager   *download_manager_;
  signature::SignatureManager *signature_manager_;
};

template <class CatalogT, class HistoryT>
struct object_fetcher_traits<HttpObjectFetcher<CatalogT, HistoryT> > {
    typedef CatalogT CatalogTN;
    typedef HistoryT HistoryTN;
};

#endif /* OBJECT_FETCHER_H */
