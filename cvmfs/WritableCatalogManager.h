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

#include "catalog_mgr.h"

#include <set>
#include <string>

#include "WritableCatalog.h"

namespace catalog {

class WritableCatalogManager : public CatalogManager {
 private:
  const static std::string kCatalogFilename; // see top of WritableCatalogManager.cc

 public:
  WritableCatalogManager(const std::string catalog_directory,
                         const std::string data_directory,
                         const bool lazy_attaching);
  virtual ~WritableCatalogManager();

  /**
   *  initializes the WritableCatalogManager. (I.e. loads all neccessary catalogs)
   *  @return true on success, false otherwise
   */
  bool Init();

  /**
   *  remove the given file from the catalogs
   *  @param file_path the full path to the file to be removed
   *  @return true on success, false otherwise
   */
  bool RemoveFile(const std::string &file_path);

  /**
   *  remove the given directory from the catalogs
   *  @param directory_path the full path to the directory to be removed
   *  @return true on success, false otherwise
   */
	bool RemoveDirectory(const std::string &directory_path);

	/**
   *  add a new directory to the catalogs
   *  @param entry a DirectoryEntry structure describing the new directory
   *  @param parent_directory the absolute path of the directory containing the
   *                          directory to be created
   *  @return true on success, false otherwise
   */
	bool AddDirectory(const DirectoryEntry &entry, const std::string &parent_directory);

 	/**
    *  add a new file to the catalogs
    *  @param entry a DirectoryEntry structure describing the new file
    *  @param parent_directory the absolute path of the directory containing the
    *                          file to be created
    *  @return true on success, false otherwise
    */
	bool AddFile(const DirectoryEntry &entry, const std::string &parent_directory);

	/**
   *  add a hardlink group to the catalogs
   *  @param entries a list of DirectoryEntries describing the new files
   *  @param parent_directory the absolute path of the directory containing the
   *                          files to be created
   *  @return true on success, false otherwise
   */
	bool AddHardlinkGroup(DirectoryEntryList &entries, const std::string &parent_directory);

	/**
   *  touches a file (basically just updates the mtime)
   *  @param entry a DirectoryEntry structure describing the file to touch
   *  @param file_path the absolute path of the file to be touched
   *  @return true on success, false otherwise
   */
  inline bool TouchFile(const DirectoryEntry entry, const std::string &file_path) {
    return TouchEntry(entry, file_path);
  }

  /**
   *  touches a directory (basically just updates the mtime)
   *  @param entry a DirectoryEntry structure describing the directory to touch
   *  @param directory_path the absolute path of the directory to be touched
   *  @return true on success, false otherwise
   */
  inline bool TouchDirectory(const DirectoryEntry entry, const std::string &directory_path) {
    return TouchEntry(entry, directory_path);
  }

  /**
    *  touches an entry (basically just updates the mtime)
    *  @param entry a DirectoryEntry structure describing the entry to touch
    *  @param entry_path the absolute path of the entry to be touched
    *  @return true on success, false otherwise
    */
  bool TouchEntry(const DirectoryEntry entry, const std::string &entry_path);

	/**
	 *  Create a new nested catalog.
	 *  This is tricky as it has to create a new catalog and move all entries
	 *  belonging there from it's parent catalog.
	 *  @param mountpoint the path of the directory to become a nested root
	 *  @return true on success, false otherwise
	 */
  bool CreateNestedCatalog(const std::string &mountpoint);

  /**
   *  Remove a nested catalog
   *  When you remove a nested catalog all entries currently held by it
   *  will be copied to it's parent catalog. Afterwards all structures
   *  describing the removed catalogs are dangling.
   *  @param mountpoint the path of the nested catalog to be removed
   *  @return true on success, false otherwise
   */
  bool RemoveNestedCatalog(const std::string &mountpoint);

	/**
	 *  No functionality here at the moment!
	 *  TODO
	 */
  bool PrecalculateListings();

  /**
   *  Committing does all stuff needed to publish the updated catalogs
   *  It finds out about all updated catalogs and snapshots them.
   *  @return true on success, false otherwise
   */
  bool Commit();

	/**
	 *  Checks if this WritableCatalogManager is configured to be lazy attaching
	 *  @return true if lazy attach is enabled, false otherwise
	 */
	inline bool IsLazyAttaching() const { return lazy_attach_; }

 protected:
  /**
   *  'Loads' a catalog. Actually the WritableCatalogManager assumes
   *  that it runs in an environment where all catalog files are already
   *  accessible. Therefore it does not actually 'load' the catalog.
   *  @param url_path the url of the catalog to load
   *  @param mount_point the file system path where the catalog should be mounted
   *  @param catalog_file a pointer to the string containing the full qualified
   *                      name of the catalog afterwards
   *  @return 0 on success, different otherwise
   */
  int LoadCatalogFile(const std::string &url_path, const hash::Md5 &mount_point,
                      std::string *catalog_file);

  /**
   *  This method is virtual in AbstractCatalogManager. It returns a new catalog
   *  structure in the form the different CatalogManagers need it.
   *  In this case it returns a stub for a WritableCatalog
   *  @param mountpoint the mount point of the catalog stub to create
   *  @param parent_catalog the parent of the catalog stub to create
   *  @return a pointer to the catalog stub structure created
   */
  Catalog* CreateCatalogStub(const std::string &mountpoint, Catalog *parent_catalog) const;

 private:
  /**
   *  Creates a fully qualified catalog path which can be loaded and attached
   *  afterwards.
   *  @param url_path the absolute path of the directory for which a catalog
   *                  name should be obtained
   *  @return the name of the catalog file to load
   */
  inline std::string GetCatalogFilenameForPath(const std::string &url_path) const {
    return (url_path.empty()) ?
      catalog_directory_ + "/" + kCatalogFilename :
      catalog_directory_ + url_path + "/" + kCatalogFilename;
  }

  /**
   *  This method is invoked if we create a completely new repository.
   *  It initializes a new root catalog and attaches it afterwards.
   *  The new root catalog will already contain a root entry.
   *  @return true on success, false otherwise
   */
  bool CreateAndAttachRootCatalog();

  /**
   *  Tries to retrieve the catalog containing the given path
   *  This method is just a wrapper around the GetCatalogByPath method of
   *  AbstractCatalogManager to provide a direct interface returning
   *  WritableCatalog classes.
   *  @param path the path to look for
   *  @param result the retrieved catalog (as a pointer)
   *  @return true if catalog was found, false otherwise
   */
  bool GetCatalogByPath(const std::string &path, WritableCatalog **result);

  /**
   *  Makes the given path relative to the catalog structure
   *  Pathes coming out here can be used for lookups in catalogs
   *  @param relativePath the path to be mangled
   *  @return the mangled path
   */
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

  /**
   *  TODO!!
   *  This method is basically the same function used before the last major
   *  refactoring. I (René) have no idea about the bits and pieces it does
   *  This should definitely be revised.
   */
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
