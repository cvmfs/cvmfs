/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_MGR_CLIENT_H_
#define CVMFS_CATALOG_MGR_CLIENT_H_

#include "catalog_mgr.h"

#include <inttypes.h>

#include <map>
#include <string>

#include "backoff.h"
#include "hash.h"
#include "manifest_fetch.h"
#include "shortstring.h"

namespace cache {
class CacheManager;
}

namespace cvmfs {
class Fetcher;
}

namespace perf {
class Counter;
class Statistics;
}

namespace signature {
class SignatureManager;
}

namespace catalog {

/**
 * A catalog manager that uses a Fetcher to get file catalgs in the form of
 * (virtual) file descriptors from a cache manager.  Sqlite has a path based
 * interface.  This catalog manager returns @<FILE DESCRIPTOR> as a path and
 * thus requires a sqlite vfs that supports this syntax, such as the cvmfs
 * default vfs for clients.
 *
 * This class uses the Fetcher in order to get access to the download manager
 * and the cache manager, too.  It requires a download manager and a signature
 * manager as it calls manifest::Fetch in order to get the manifest of new and
 * updated root catalogs.  It requires the cache manager to get access to the
 * Unpin() method of the corresponding quota manager; loaded catalogs need to
 * be unpinned when the class is destructed.
 */
class ClientCatalogManager : public AbstractCatalogManager<Catalog> {
  // Maintains certificate hit/miss counters
  friend class CachedManifestEnsemble;

 public:
  ClientCatalogManager(const std::string &repo_name,
    cvmfs::Fetcher *fetcher,
    signature::SignatureManager *signature_mgr,
    perf::Statistics *statistics);
  virtual ~ClientCatalogManager();

  bool InitFixed(const shash::Any &root_hash, bool alternative_path);

  shash::Any GetRootHash();

  bool offline_mode() const { return offline_mode_; }
  uint64_t all_inodes() const { return all_inodes_; }
  uint64_t loaded_inodes() const { return loaded_inodes_; }

 protected:
  LoadError LoadCatalog(const PathString  &mountpoint,
                        const shash::Any  &hash,
                        std::string       *catalog_path,
                        shash::Any        *catalog_hash);
  void UnloadCatalog(const catalog::Catalog *catalog);
  catalog::Catalog* CreateCatalog(const PathString &mountpoint,
                                  const shash::Any  &catalog_hash,
                                  catalog::Catalog *parent_catalog);
  void ActivateCatalog(catalog::Catalog *catalog);

 private:
  LoadError LoadCatalogCas(const shash::Any &hash,
                           const std::string &name,
                           const std::string &alt_catalog_path,
                           std::string *catalog_path);

  /**
   * Required for unpinning
   */
  std::map<PathString, shash::Any> loaded_catalogs_;
  std::map<PathString, shash::Any> mounted_catalogs_;

  std::string repo_name_;
  cvmfs::Fetcher *fetcher_;
  signature::SignatureManager *signature_mgr_;
  bool offline_mode_;  /**< cached copy used because there is no network */
  uint64_t all_inodes_;
  uint64_t loaded_inodes_;
  bool fixed_alt_root_catalog_;  /**< fixed root hash but alternative url */
  BackoffThrottle backoff_throttle_;
  perf::Counter *n_certificate_hits_;
  perf::Counter *n_certificate_misses_;
};


/**
 * Tries to fetch the certificate from cache
 */
class CachedManifestEnsemble : public manifest::ManifestEnsemble {
 public:
  CachedManifestEnsemble(
    cache::CacheManager *cache_mgr,
    ClientCatalogManager *catalog_mgr)
    : cache_mgr_(cache_mgr)
    , catalog_mgr_(catalog_mgr)
  { }
  void FetchCertificate(const shash::Any &hash);

 private:
  cache::CacheManager *cache_mgr_;
  ClientCatalogManager *catalog_mgr_;
};

}  // namespace catalog

#endif  // CVMFS_CATALOG_MGR_CLIENT_H_
