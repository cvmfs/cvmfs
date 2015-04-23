/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_H_
#define CVMFS_CACHE_H_

#include <stdint.h>
#include <sys/types.h>

#include <map>
#include <string>
#include <vector>

#include "atomic.h"
#include "backoff.h"
#include "catalog_mgr.h"
#include "file_chunk.h"
#include "manifest_fetch.h"
#include "shortstring.h"
#include "signature.h"
#include "statistics.h"

namespace catalog {
class DirectoryEntry;
class Catalog;
}

namespace hash {
struct Any;
}

namespace download {
class DownloadManager;
}

namespace cache {

enum CacheModes {
  kCacheReadWrite = 0,
  kCacheReadOnly,
};

bool Init(const std::string &cache_path, const bool alien_cache);
void Fini();

int Open(const shash::Any &id);
int FetchDirent(const catalog::DirectoryEntry &d,
                const std::string &cvmfs_path,
                const bool volatile_content,
                download::DownloadManager *download_manager);
int FetchChunk(const FileChunk &chunk,
               const std::string &cvmfs_path,
               const bool volatile_content,
               download::DownloadManager *download_manager);
int64_t GetNumDownloads();

CacheModes GetCacheMode();
void TearDown2ReadOnly();


/**
 * A catalog manager that fetches its catalogs remotely and stores
 * them in the cache.
 */
class CatalogManager : public catalog::AbstractCatalogManager {
  friend class ManifestEnsemble;  // Maintains certificate hit/miss counters

 public:
  CatalogManager(const std::string &repo_name,
                 signature::SignatureManager *signature_manager,
                 download::DownloadManager *download_manager,
                 perf::Statistics *statistics);
  virtual ~CatalogManager();

  bool InitFixed(const shash::Any &root_hash);

  shash::Any GetRootHash() {
    ReadLock();
    shash::Any result = mounted_catalogs_[PathString("", 0)];
    Unlock();
    return result;
  }
  bool offline_mode() const { return offline_mode_; }
  uint64_t all_inodes() const { return all_inodes_; }
  uint64_t loaded_inodes() const { return loaded_inodes_; }

 protected:
  catalog::LoadError LoadCatalog(const PathString  &mountpoint,
                                 const shash::Any  &hash,
                                 std::string       *catalog_path,
                                 shash::Any        *catalog_hash);
  void UnloadCatalog(const catalog::Catalog *catalog);
  catalog::Catalog* CreateCatalog(const PathString &mountpoint,
                                  const shash::Any  &catalog_hash,
                                  catalog::Catalog *parent_catalog);
  void ActivateCatalog(catalog::Catalog *catalog);

 private:
  catalog::LoadError LoadCatalogCas(const shash::Any &hash,
                                    const std::string &cvmfs_path,
                                    std::string *catalog_path);

  /**
   * required for unpinning
   */
  std::map<PathString, shash::Any> loaded_catalogs_;
  std::map<PathString, shash::Any> mounted_catalogs_;

  std::string repo_name_;
  signature::SignatureManager *signature_manager_;
  download::DownloadManager *download_manager_;
  bool offline_mode_;  /**< cached copy used because there is no network */
  perf::Counter *n_certificate_hits_;
  perf::Counter *n_certificate_misses_;
  uint64_t all_inodes_;
  uint64_t loaded_inodes_;
  BackoffThrottle backoff_throttle_;
};


/**
 * Tries to fetch the certificate from cache
 */
class ManifestEnsemble : public manifest::ManifestEnsemble {
 public:
  explicit ManifestEnsemble(cache::CatalogManager *catalog_mgr) {
    catalog_mgr_ = catalog_mgr;
  }
  void FetchCertificate(const shash::Any &hash);
 private:
  cache::CatalogManager *catalog_mgr_;
};

}  // namespace cache

#endif  // CVMFS_CACHE_H_
