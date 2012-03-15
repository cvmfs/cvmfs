/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_H_
#define CVMFS_CACHE_H_

#include <stdint.h>
#include <string>

#include "catalog_mgr.h"
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


/**
 * A catalog manager that fetches its catalogs remotely and stores
 * them in the cache.
 */
class CatalogManager : public catalog::AbstractCatalogManager {
 public:
  CatalogManager(const std::string &repo_name,
                 const bool ignore_signature);
  virtual ~CatalogManager() { };

 protected:
  catalog::LoadError LoadCatalog(const std::string &mountpoint,
                                 const hash::Any &hash,
                                 std::string *catalog_path);
  catalog::Catalog* CreateCatalog(const std::string &mountpoint,
                                  catalog::Catalog *parent_catalog) const;

 private:
  catalog::LoadError LoadCatalogCas(const hash::Any &hash,
                                    std::string *catalog_path);

  std::string repo_name_;
  bool ignore_signature_;
  atomic_int32 certificate_hits_;
  atomic_int32 certificate_misses_;
};

}  // namespace cache

#endif  // CVMFS_CACHE_H_
