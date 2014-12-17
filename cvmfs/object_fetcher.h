/**
 * This file is part of the CernVM File System.
 */

#ifndef OBJECT_FETCHER_H
#define OBJECT_FETCHER_H

#include <string>

#include "manifest.h"
#include "manifest_fetch.h"
#include "history_sqlite.h"
#include "catalog.h"

#include "download.h"
#include "signature.h"


/**
 * This is the default class implementing the data object fetching strategy. It
 * is meant to be used when CVMFS specific data structures need to be downloaded
 * from a backend storage of a repository.
 *
 * It abstracts all accesses to external file or HTTP resources and gathers this
 * access logic in one central point. This also comes in handy when unit testing
 * components that depend on downloading CVMFS data structures from a repository
 * backen storage like CatalogTraversal<> or GarbageCollector.
 *
 * Since we are handling I/O here, it is implemented using pure virtual method
 * calls in an abstract interface class.
 */
template <class CatalogT>
class AbstractObjectFetcher {
 public:
  virtual ~AbstractObjectFetcher() {}

  virtual manifest::Manifest*  FetchManifest() = 0;
  virtual history::History*    FetchHistory(const shash::Any &hash = shash::Any()) = 0;
  virtual CatalogT*            FetchCatalog(const shash::Any  &catalog_hash,
                                            const std::string &catalog_path,
                                            const bool         is_nested = false,
                                                  CatalogT    *parent    = NULL)  = 0;

 public:
  bool HasHistory() {
    shash::Any history_hash = GetHistoryHash();
    return ! history_hash.IsNull();
  }

 protected:
  shash::Any GetHistoryHash() {
    UniquePtr<manifest::Manifest> manifest(FetchManifest());
    if (! manifest || manifest->history().IsNull()) {
      return shash::Any();
    }

    return manifest->history();
  }
};

template <class CatalogT>
class HttpObjectFetcher : public AbstractObjectFetcher<CatalogT> {
 public:
  static HttpObjectFetcher<CatalogT>* Create(const std::string &repo_name,
                                             const std::string &repo_url,
                                             const std::string &repo_keys,
                                             const std::string &temp_dir) {
    UniquePtr<HttpObjectFetcher> fetcher(new HttpObjectFetcher(repo_name,
                                                               repo_url,
                                                               repo_keys,
                                                               temp_dir));
    assert (fetcher);
    return (fetcher->Initialize()) ? fetcher.Release() : NULL;
  }

  virtual ~HttpObjectFetcher() {
    signature_manager_.Fini();
    download_manager_.Fini();
  }


 protected:
  bool Initialize() {
    download_manager_.Init(1, true);
    signature_manager_.Init();
    return signature_manager_.LoadPublicRsaKeys(repo_keys_);
  }


 public:
  manifest::Manifest* FetchManifest() {
    manifest::Manifest *manifest = NULL;

    const std::string url = BuildUrl(".cvmfspublished");

    // Download manifest file
    struct manifest::ManifestEnsemble manifest_ensemble;
    manifest::Failures retval = manifest::Fetch(
                                  repo_url_,
                                  repo_name_,
                                  0,
                                  NULL,
                                  &signature_manager_,
                                  &download_manager_,
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


  history::History* FetchHistory(const shash::Any &history_hash = shash::Any()) {
    // retrieve the current HEAD history hash (if nothing else given)
    shash::Any effective_history_hash = (! history_hash.IsNull())
                      ? history_hash
                      : this->AbstractObjectFetcher<CatalogT>::GetHistoryHash();

    // download the history hash
    std::string path;
    if (effective_history_hash.IsNull() ||
        ! Fetch(effective_history_hash, shash::kSuffixHistory, &path)) {
      return NULL;
    }

    // open the history file
    return history::SqliteHistory::Open(path);
  }


  CatalogT* FetchCatalog(const shash::Any  &catalog_hash,
                         const std::string &catalog_path,
                         const bool         is_nested = false,
                               CatalogT    *parent    = NULL) {
    assert (! catalog_hash.IsNull());

    std::string path;
    if (! Fetch(catalog_hash, shash::kSuffixCatalog, &path)) {
      return NULL;
    }

    return CatalogT::AttachFreely(catalog_path,
                                  path,
                                  catalog_hash,
                                  parent,
                                  is_nested);
  }


 protected:
  bool Fetch(const shash::Any     &object_hash,
             const shash::Suffix   hash_suffix,
             std::string          *object_file) {
    assert (object_file != NULL);
    assert (! object_hash.IsNull());

    object_file->clear();

    const std::string url  = BuildUrl(object_hash, hash_suffix);
    const std::string dest = temporary_directory_ + "/" + object_hash.ToString();

    download::JobInfo download_catalog(&url, true, false, &dest, &object_hash);
    download::Failures retval = download_manager_.Fetch(&download_catalog);

    if (retval != download::kFailOk) {
      LogCvmfs(kLogDownload, kLogDebug, "failed to download object "
                                        "%s (%d - %s)",
               object_hash.ToString().c_str(), retval, Code2Ascii(retval));
    }

    *object_file = dest;
    return retval == download::kFailOk;
  }

  HttpObjectFetcher(const std::string &repo_name,
                    const std::string &repo_url,
                    const std::string &repo_keys,
                    const std::string &temp_dir) :
    repo_url_(repo_url), repo_name_(repo_name), repo_keys_(repo_keys),
    temporary_directory_(temp_dir) {}

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
  const std::string            repo_keys_;
  const std::string            temporary_directory_;
  download::DownloadManager    download_manager_;
  signature::SignatureManager  signature_manager_;
};

#endif /* OBJECT_FETCHER_H */
