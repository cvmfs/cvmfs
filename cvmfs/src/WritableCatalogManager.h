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

#include <string>

#include "cvmfs_sync_recursion.h"

namespace cvmfs {

class WritableCatalog;
  
class WritableCatalogManager : public AbstractCatalogManager {
 private:
  const static std::string kCatalogFilename; // see top of WritableCatalogManager.cc
  
 public:
  WritableCatalogManager(const std::string catalog_directory,
                         const std::string data_directory,
                         const bool lazy_attaching);
  virtual ~WritableCatalogManager();
  
  bool Init();
  
  bool RemoveFile(DirEntry *entry);
	bool RemoveDirectory(DirEntry *entry);
	
	bool AddDirectory(DirEntry *entry);
	bool AddFile(DirEntry *entry);
	bool AddHardlinkGroup(DirEntryList group);
	
  inline bool TouchFile(DirEntry *entry) { return TouchEntry(entry); }
  inline bool TouchDirectory(DirEntry *entry) { return TouchEntry(entry); }
  bool TouchEntry(DirEntry *entry);
	
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
      catalog_directory_ + "/" + url_path + "/" + kCatalogFilename;
  }
   
  bool CreateAndAttachRootCatalog();
  bool LoadAndAttachCatalogsRecursively();
  
  bool CheckForExistanceAndAddEntry(WritableCatalog *catalog, const DirectoryEntry &entry, const std::string &entry_path, const std::string &parent_path);
  
  bool GetCatalogByPath(const std::string &path, WritableCatalog **result);
  
	inline std::string RelativeToCatalogPath(const std::string &relativePath) const { return (relativePath == "") ? "" : "/" + relativePath; }
  DirectoryEntry CreateNewDirectoryEntry(DirEntry *entry, Catalog *catalog, const int hardlink_group_id = 0) const;
  
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
