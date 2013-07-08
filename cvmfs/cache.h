/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_H_
#define CVMFS_CACHE_H_

#include <stdint.h>

#include <string>
#include <map>
#include <vector>

#include "catalog_mgr.h"
#include "file_chunk.h"
#include "shortstring.h"
#include "atomic.h"
#include "manifest_fetch.h"

namespace catalog {
class DirectoryEntry;
class Catalog;
}

namespace hash {
struct Any;
}

namespace cache {

enum CacheModes {
  kCacheReadWrite = 0,
  kCacheReadOnly,
};

bool Init(const std::string &cache_path);
void Fini();

std::string GetPathInCache(const hash::Any &id);
int Open(const hash::Any &id);
int FetchDirent(const catalog::DirectoryEntry &d,
                const std::string &cvmfs_path);
int FetchChunk(const FileChunk &chunk, const std::string &cvmfs_path);
int Fetch(const hash::Any &hash, const std::string &, uint64_t size, const std::string &cvmfs_path);
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
                 const bool ignore_signature);
  virtual ~CatalogManager() { };

  bool InitFixed(const hash::Any &root_hash);

  hash::Any GetRootHash() {
    ReadLock();
    hash::Any result = mounted_catalogs_[PathString("", 0)];
    Unlock();
    return result;
  }
  std::string GetCertificateStats() {
    return "hits: " + StringifyInt(atomic_read32(&certificate_hits_)) + "    " +
    "misses: " + StringifyInt(atomic_read32(&certificate_misses_)) + "\n";
  }
  bool offline_mode() const { return offline_mode_; }
  uint64_t all_inodes() const { return all_inodes_; }
  uint64_t loaded_inodes() const { return loaded_inodes_; }

 protected:
  catalog::LoadError LoadCatalog(const PathString &mountpoint,
                                 const hash::Any  &hash,
                                 std::string      *catalog_path,
                                 hash::Any        *catalog_hash);
  void UnloadCatalog(const catalog::Catalog *catalog);
  catalog::Catalog* CreateCatalog(const PathString &mountpoint,
                                  const hash::Any  &catalog_hash,
                                  catalog::Catalog *parent_catalog);
  void ActivateCatalog(const catalog::Catalog *catalog);

 private:
  catalog::LoadError LoadCatalogCas(const hash::Any &hash,
                                    const std::string &cvmfs_path,
                                    std::string *catalog_path);

  /**
   * required for unpinning
   */
  std::map<PathString, hash::Any> loaded_catalogs_;
  std::map<PathString, hash::Any> mounted_catalogs_;

  std::string repo_name_;
  bool ignore_signature_;
  bool offline_mode_;  /**< cached copy used because there is no network */
  atomic_int32 certificate_hits_;
  atomic_int32 certificate_misses_;
  uint64_t all_inodes_;
  uint64_t loaded_inodes_;
};


/**
 * Tries to fetch the certificate from cache
 */
class ManifestEnsemble : public manifest::ManifestEnsemble {
 public:
  explicit ManifestEnsemble(cache::CatalogManager *catalog_mgr) {
    catalog_mgr_ = catalog_mgr;
  }
  void FetchCertificate(const hash::Any &hash);
 private:
   cache::CatalogManager *catalog_mgr_;
};

}  // namespace cache

#endif  // CVMFS_CACHE_H_
