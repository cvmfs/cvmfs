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
#include "shortstring.h"
#include "atomic.h"

namespace catalog {
class DirectoryEntry;
class Catalog;
}

namespace hash {
struct Any;
}

namespace cache {

bool Init(const std::string &cache_path);
void Fini();

int Open(const hash::Any &id);
bool Open2Mem(const hash::Any &id, unsigned char **buffer, uint64_t *size);
int StartTransaction(const hash::Any &id,
                     std::string *final_path, std::string *temp_path);
int AbortTransaction(const std::string &temp_path);
int CommitTransaction(const std::string &final_path,
                      const std::string &temp_path,
                      const std::string &cvmfs_path,
                      const hash::Any &hash,
                      const uint64_t size);
bool CommitFromMem(const hash::Any &id, const unsigned char *buffer,
                   const uint64_t size, const std::string &cvmfs_path);
bool Contains(const hash::Any &id);
int Fetch(const catalog::DirectoryEntry &d, const std::string &cvmfs_path);
int64_t GetNumDownloads();


/**
 * A catalog manager that fetches its catalogs remotely and stores
 * them in the cache.
 */
class CatalogManager : public catalog::AbstractCatalogManager {
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

 protected:
  catalog::LoadError LoadCatalog(const PathString &mountpoint,
                                 const hash::Any &hash,
                                 std::string *catalog_path);
  void UnloadCatalog(const catalog::Catalog *catalog);
  catalog::Catalog* CreateCatalog(const PathString &mountpoint,
                                  catalog::Catalog *parent_catalog);

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
};

}  // namespace cache

#endif  // CVMFS_CACHE_H_
