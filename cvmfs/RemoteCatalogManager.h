/**
 *  This is a concrete CatalogManager derived from AbstractCatalogManager
 *  It performs the catalog management for the client application.
 *  I.e. catalogs are loaded from the web and all catalogs are read-only
 *
 *  TODO: there is still lots of file loading code in this class
 *        this should be moved to a dedicated FileManager or something
 */

#ifndef REMOTE_CATALOG_MANAGER_H
#define REMOTE_CATALOG_MANAGER_H 1

#include "AbstractCatalogManager.h"

#include "atomic.h"

namespace catalog {

class RemoteCatalogManager : public AbstractCatalogManager {
 public:
  RemoteCatalogManager(const std::string &root_url, const std::string &repo_name, const std::string &whitelist,
                       const std::string &blacklist, const bool force_signing);
  virtual ~RemoteCatalogManager();

 protected:
  /**
   *  This is a concrete implementation of the virtual method
   *  defined in AbstractCatalogManager
   *  actually it is just a convenience wrapper to hide all the
   *  nasty loading stuff which I do not understand
   *  @param url_path the path of the catalog to load
   *  @param mount_point the hash of the desired mount point of the loaded catalog
   *  @param catalog_file this pointer contains the path of the loaded catalog file
   *  @return 0 on success otherwise an error code
   */
  inline int LoadCatalogFile(const std::string &url_path, const hash::Md5 &mount_point,
                            std::string *catalog_file)
  {
    return LoadCatalogFile(url_path, mount_point, -1, false, hash::Any(hash::kSha1), catalog_file);
  }

  /** see AbstractCatalogManager for details */
  Catalog* CreateCatalogStub(const std::string &mountpoint, Catalog *parent_catalog) const;

 private:
  /**
   *  well... heaven knows, what is actually going on here
   *  Jakob... this is your job. ;-)
   */
  int FetchCatalog(const std::string &url_path, const bool no_proxy, const hash::Md5 &mount_point,
                   std::string &cat_file, hash::Any &cat_sha1, std::string &old_file, hash::Any &old_sha1,
                   bool &cached_copy, const hash::Any &sha1_expected, const bool dry_run = false);

  /**
   *  yeah... what ever...
   */
  int LoadCatalogFile(const std::string &url_path, const hash::Md5 &mount_point,
                      const int existing_cat_id, const bool no_cache,
                      const hash::Any &expected_clg, std::string *catalog_file);

  /**
   * ??
   */
  bool IsValidCertificate(bool nocache);

  /**
   * ??
   */
  std::string MakeFilesystemKey(std::string url) const;

  bool RefreshCatalog(Catalog *catalog);

 private:
  std::string root_url_;
  std::string repo_name_;
  std::string whitelist_;
  std::string blacklist_;
  bool force_signing_;

  atomic_int32 certificate_hits_;
  atomic_int32 certificate_misses_;
};

}

#endif /* REMOTE_CATALOG_MANAGER_H */
