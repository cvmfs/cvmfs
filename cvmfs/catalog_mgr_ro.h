/**
 * This file is part of the CernVM File System.
 *
 * The SimpleCatalogManager is a simplistic concrete implementation of the
 * AbstractCatalogManager and allows for easy-to-use access to the catalog
 * structure of a given repository. This class is tailored for simplicity, not
 * for performance. Have a look into ClientCatalogManager if you are working
 * on the CVMFS client.
 */

#ifndef CVMFS_CATALOG_MGR_RO_H_
#define CVMFS_CATALOG_MGR_RO_H_

#include <string>

#include "catalog_mgr.h"

namespace download {
class DownloadManager;
}

namespace manifest {
class Manifest;
}

namespace perf {
class Statistics;
}

namespace catalog {

class SimpleCatalogManager : public AbstractCatalogManager<Catalog> {
 public:
  SimpleCatalogManager(const shash::Any &base_hash,
                       const std::string &stratum0,
                       const std::string &dir_temp,
                       download::DownloadManager *download_manager,
                       perf::Statistics *statistics,
                       const bool manage_catalog_files = false,
                       const std::string &dir_cache = "",
                       const bool copy_to_tmp_dir = false);

  virtual LoadReturn LoadCatalogByHash(CatalogContext *ctlg_context);

 protected:
  virtual LoadReturn GetNewRootCatalogContext(CatalogContext *result);
  virtual Catalog *CreateCatalog(const PathString &mountpoint,
                                 const shash::Any &catalog_hash,
                                 Catalog *parent_catalog);

  const shash::Any &base_hash() const { return base_hash_; }
  void set_base_hash(const shash::Any &hash) { base_hash_ = hash; }
  const std::string &dir_temp() const { return dir_temp_; }

  /**
   * Makes the given path relative to the catalog structure
   * Paths coming out here can be used for lookups in catalogs
   * @param relativePath the path to be mangled
   * @return the mangled path
   */
  inline std::string MakeRelativePath(const std::string &relative_path) const {
    return (relative_path == "") ? "" : "/" + relative_path;
  }

  bool UseLocalCache() const { return !dir_cache_.empty(); }

  std::string dir_cache_;  // absolute path to local cache
                           // directory
  bool copy_to_tmp_dir_;   // only relevant if using local
                           // cache directory:
                           // for writeable catalogs a copy
                           // must be created in dir_temp_

 private:
  std::string CopyCatalogToTempFile(const std::string &cache_path);

  shash::Any base_hash_;
  std::string stratum0_;
  std::string dir_temp_;
  download::DownloadManager *download_manager_;
  const bool manage_catalog_files_;
};  // class SimpleCatalogManager

}  // namespace catalog

#endif  // CVMFS_CATALOG_MGR_RO_H_
