/**
 *  A WritableCatalogManager adds write functionality to the catalog
 *  managers. It is used for the server side of CVMFS
 *  All nested catalog loading functionality is inherited from AbstractCatalogManager
 *  Thus, you don't have to worry about catalog attaching and loading here.
 *
 *  Note: The inode assignment is based on the fact that the number of entries
 *        in a catalog do not change (expect on reload). As we do exactly that
 *        with the WritableCatalogManager here, inode numbers derived from
 *        WritableCatalogs and the WritableCatalogManager may (and will) be 
 *        screwed. This is not an issue in the current implementation, as they
 *        are not used in the synching process. But keep that in mind!!
 *
 *  TODO: the documentation of this file is not full featured
 *        this is highly work in progress, do not depend on any of the stuff down here!
 *
 */

#ifndef WRITABLE_CATALOG_MANAGER_H
#define WRITABLE_CATALOG_MANAGER_H

#include "AbstractCatalogManager.h"

#include <set>
#include <string>

#include "WritableCatalog.h"

namespace cvmfs {
  
class WritableCatalogManager : public AbstractCatalogManager {
 private:
  const static std::string kCatalogFilename; // see top of WritableCatalogManager.cc
  
 public:
  WritableCatalogManager(const std::string catalog_directory,
                         const std::string data_directory,
                         const bool lazy_attaching);
  virtual ~WritableCatalogManager();
  
  bool Init();
  
  bool RemoveFile(const std::string &file_path);
	bool RemoveDirectory(const std::string &directory_path);
	
	bool AddDirectory(const DirectoryEntry &entry, const std::string &parent_directory);
	bool AddFile(const DirectoryEntry &entry, const std::string &parent_directory);
	bool AddHardlinkGroup(DirectoryEntryList &entries, const std::string &parent_directory);
	
  inline bool TouchFile(const DirectoryEntry entry, const std::string &file_path) {
    return TouchEntry(entry, file_path);
  }
  inline bool TouchDirectory(const DirectoryEntry entry, const std::string &directory_path) {
    return TouchEntry(entry, directory_path);
  }
  bool TouchEntry(const DirectoryEntry entry, const std::string &entry_path);
	
  bool CreateNestedCatalog(const std::string &mountpoint);
  bool RemoveNestedCatalog(const std::string &mountpoint);
	
  bool PrecalculateListings();
  bool Commit();
	
	inline bool IsLazyAttaching() const { return lazy_attach_; }
	
 protected:
  int LoadCatalogFile(const std::string &url_path, const hash::t_md5 &mount_point, 
                      std::string *catalog_file);
  
  Catalog* CreateCatalogStub(const std::string &mountpoint, Catalog *parent_catalog) const;

 private:
  inline std::string GetCatalogFilenameForPath(const std::string &url_path) const {
    return (url_path.empty()) ?
      catalog_directory_ + "/" + kCatalogFilename :
      catalog_directory_ + url_path + "/" + kCatalogFilename;
  }
   
  bool CreateAndAttachRootCatalog();
  bool LoadAndAttachCatalogsRecursively();
  
  bool CheckForExistanceAndAddEntry(WritableCatalog *catalog, const DirectoryEntry &entry, const std::string &entry_path, const std::string &parent_path);
  
  bool GetCatalogByPath(const std::string &path, WritableCatalog **result);
  
	inline std::string RelativeToCatalogPath(const std::string &relativePath) const { return (relativePath == "") ? "" : "/" + relativePath; }
  
  /**
   *  goes through all open catalogs and determines which catalogs need updated
   *  snapshots.
   *  @param[out] result the list of catalogs to snapshot
   */
  void GetCatalogsToSnapshot(WritableCatalogList &result) const { 
    unsigned int number_of_dirty_catalogs = GetCatalogsToSnapshotRecursively(GetRootCatalog(), result);
    assert (number_of_dirty_catalogs <= result.size());
  }
  int GetCatalogsToSnapshotRecursively(const Catalog *catalog, WritableCatalogList &result) const;
  bool SnapshotCatalog(WritableCatalog *catalog) const;
  
 private:
  std::string catalog_directory_;
 	std::string union_directory_;
 	std::string data_directory_;
 	std::set<std::string> immutables_;
 	std::string keyfile_;
 	bool lazy_attach_;
};

}

#endif /* WRITABLE_CATALOG_MANAGER_H */
