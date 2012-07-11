/**
 * This file is part of the CernVM File System.
 *
 * A WritableCatalogManager adds write functionality to the catalog
 * manager.  It is used for the server side of CVMFS
 * All nested catalog loading functionality is inherited from
 * AbstractCatalogManager.
 *
 * The inode assignment is based on the fact that the number of entries in a
 * catalog do not change (expect on reload). As we do exactly that with the
 * WritableCatalogManager here, inode numbers derived from WritableCatalogs
 * and the WritableCatalogManager may (and will) be screwed.  This is not an
 * issue in the current implementation, as they are not used in the synching
 * process.  Just keep in mind.
 *
 * The WritableCatalogManager starts with a base repository (given by the
 * root hash), and downloads and uncompresses all required catalogs into
 * temporary storage.
 */

#ifndef CVMFS_CATALOG_MGR_RW_H_
#define CVMFS_CATALOG_MGR_RW_H_

#include <set>
#include <string>

#include "catalog_rw.h"
#include "catalog_mgr.h"

namespace upload {
class Spooler;
}
class Manifest;

namespace catalog {

class WritableCatalogManager : public AbstractCatalogManager {
 public:
  WritableCatalogManager(const hash::Any &base_hash,
                         const std::string &stratum0,
                         const std::string &dir_temp,
                         upload::Spooler *spooler);
  static Manifest *CreateRepository(const std::string &dir_temp, 
                                    upload::Spooler *spooler);

  bool Init();

  bool AddFile(const DirectoryEntry &entry,
               const std::string &parent_directory);
  bool RemoveFile(const std::string &file_path);
  bool AddDirectory(const DirectoryEntry &entry,
                    const std::string &parent_directory);
  bool RemoveDirectory(const std::string &directory_path);

  bool TouchEntry(const DirectoryEntry entry, const std::string &path);
  bool TouchFile(const DirectoryEntry entry, const std::string &file_path)
  {
    return TouchEntry(entry, file_path);
  }
  bool TouchDirectory(const DirectoryEntry entry,
                      const std::string &directory_path)
  {
    return TouchEntry(entry, directory_path);
  }

	bool AddHardlinkGroup(DirectoryEntryList &entries,
                        const std::string &parent_directory);

  bool CreateNestedCatalog(const std::string &mountpoint);
  bool RemoveNestedCatalog(const std::string &mountpoint);

	/**
	 * TODO
	 */
  bool PrecalculateListings();

  Manifest *Commit();

 protected:
  void EnforceSqliteMemLimit() { }

  LoadError LoadCatalog(const PathString &mountpoint, const hash::Any &hash,
                        std::string *catalog_path);
  Catalog* CreateCatalog(const PathString &mountpoint, Catalog *parent_catalog);

 private:
  bool FindCatalog(const std::string &path, WritableCatalog **result);

  /**
   * Makes the given path relative to the catalog structure
   * Pathes coming out here can be used for lookups in catalogs
   * @param relativePath the path to be mangled
   * @return the mangled path
   */
	inline std::string MakeRelativePath(const std::string &relative_path) const {
    return (relative_path == "") ? "" : "/" + relative_path;
  }

  /**
   * Traverses all open catalogs and determines which catalogs need updated
   * snapshots.
   * @param[out] result the list of catalogs to snapshot
   */
  void GetModifiedCatalogs(WritableCatalogList *result) const {
    const unsigned int number_of_dirty_catalogs =
      GetModifiedCatalogsRecursively(GetRootCatalog(), result);
    assert (number_of_dirty_catalogs <= result->size());
  }
  int GetModifiedCatalogsRecursively(const Catalog *catalog,
                                     WritableCatalogList *result) const;

  hash::Any SnapshotCatalog(WritableCatalog *catalog) const;

 private:
  // defined in catalog_mgr_rw.cc
  const static std::string kCatalogFilename;

  hash::Any base_hash_;
  std::string stratum0_;
  std::string dir_temp_;
  upload::Spooler *spooler_;
};  // class WritableCatalogManager

}  // namespace catalog

#endif  // CVMFS_CATALOG_MGR_RW_H_
