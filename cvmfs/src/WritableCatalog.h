/**
 *  This class is derived from Catalog and adds write capabilities to it.
 *  It is used by the WritableCatalogManager on the server side.
 *
 *  Note: Catalogs are meant to be thread safe... keep this in mind
 *        TODO: actually keep this in mind here :o)
 *
 *  The main functionality is in:
 *    - CheckForExistanceAndAddEntry
 *    - TouchEntry
 *    - RemoveEntry
 */

#ifndef WRITABLE_CATALOG_H
#define WRITABLE_CATALOG_H 1

#include "Catalog.h"

#include <list>
#include <string>

namespace cvmfs {

class WritableCatalog : public Catalog {
 public: 
  WritableCatalog(const std::string &path, Catalog *parent);
  virtual ~WritableCatalog();
  
  /**
   *  checks if the catalog is actually a writable catalog
   *  See: Catalog::IsWritable()
   *  @return true if catalog is writable, otherwise false
   */
  inline bool IsWritable() const { return true; }
  
  /**
   *  checks if this catalog was changed in some way
   *  @return true if catalog was modified, false otherwise
   */
  inline bool IsDirty() const { return dirty_; }
  
  /**
   *  this method checks, if the provided DirectoryEntry is already resident
   *  in this catalog. In case not, it will add it
   *  @param entry the DirectoryEntry to add to the catalog
   *  @param entry_path the full path of the DirectoryEntry to add
   *  @param parent_path the full path of the containing directory
   *  @return true if DirectoryEntry was added, false otherwise
   */
  bool CheckForExistanceAndAddEntry(const DirectoryEntry &entry, const std::string &entry_path, const std::string &parent_path);
  
  /**
   *  with this method you can set the mtime of a DirectoryEntry in the catalog
   *  to the current time (touch semantic of unix shells)
   *  @param entry_path the full path of the entry to touch
   *  @param timestamp the timestamp to set the mtime of this entry to
   *  @return true on successful touching, false otherwise
   */
  bool TouchEntry(const std::string &entry_path, const time_t timestamp);
  
  /**
   *  this method removes the specified entry from the catalog
   *  Note: if you remove a directory which is non-empty, you might end up
   *        with dangling entries (this should be treated in upper layers)
   *  @param entry_path the full path of the DirectoryEntry to delete
   *  @return true if entry was removed, false otherwise
   */
  bool RemoveEntry(const std::string &entry_path);
  
  /**
   *  find out the maximal present hardlink group id in this catalog
   *  @return the maximal hardlink group id in this catalog
   */
  int GetMaximalHardlinkGroupId() const;
  
  /**
   *  sets the last modified time stamp of this catalog
   *  @return true on success, false otherwise
   */
  bool UpdateLastModified();
  
  /**
   *  increments the revision of the catalog in the database
   *  @return true on success, false otherwise
   */
  bool IncrementRevision();
  
  /**
   *  sets the SHA1 content hash of the previous catalog revision
   *  @return true on success, false otherwise
   */
  bool SetPreviousRevision(const hash::t_sha1 &hash);
  
  /**
   *  updates the link to a nested catalog in the database
   *  @param path the path of the nested catalog to update
   *  @param hash the hash to set the given nested catalog link to
   *  @return true on success, false otherwise
   */
  bool UpdateNestedCatalogLink(const std::string &path, const hash::t_sha1 &hash);

   /**
    *  TODO: document this
    */
   bool SplitContentIntoNewNestedCatalog(WritableCatalog *new_nested_catalog);
  
  /**
   *  static method to create a new database file and initialize the
   *  database schema in it
   *  @param file_path the absolute location the file should end up in
   *  @param root_entry a DirectoryEntry which should serve as root entry
   *                    of the newly created catalog
   *  @param root_entry_parent_path the path of the parent directory of the
   *                                root entry (i.e. the nested catalog mount
   *                                point or "" if you're creating a root catalog)
   */
  static bool CreateNewCatalogDatabase(const std::string &file_path, 
                                       const DirectoryEntry &root_entry,
                                       const std::string &root_entry_parent_path,
                                       const bool root_catalog);
                                        
  /**
  *  insert a nested catalog reference to this catalog
  *  you can specify the attached catalog object of this mountpoint to
  *  also update the in-memory representation of the catalog tree
  *  @param mountpoint the path to the catalog to add a reference to
  *  @param attached_reference can contain a reference to the attached catalog object of mountpoint
  *  @return true on success, false otherwise
  */
  bool InsertNestedCatalogReference(const std::string &mountpoint, Catalog *attached_reference = NULL);

  /**
  *  remove a nested catalog reference from the database
  *  if the catalog 'mountpoint' is currently attached as a child, it will be removed (but not detached)
  *  though you can catch the object through the second parameter (i.e. pass it to InsertNestedCatalogReference)
  *  @param[in] mountpoint the mountpoint of the nested catalog to dereference in the database
  *  @param[out] attached_reference is set to the object of the attached child or to NULL
  *  @return true on success, false otherwise
  */
  bool RemoveNestedCatalogReference(const std::string &mountpoint, Catalog **attached_reference = NULL);
  
 protected:
  /**
   *  overrides the virtual method in Catalog to specify custom database
   *  open flags. SQLite will use them to open the database file for this catalog
   *  @return an integer containing the specified open flags
   */
  inline int DatabaseOpenFlags() const { return SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READWRITE; }
  
  /**
   *  actually adds a DirectoryEntry to the catalog
   *  designed for internal use only and called by CheckForExistanceAndAddEntry
   *  @param entry the DirectoryEntry to add to the catalog
   *  @param path_hash the MD5 hash of the entry to add
   *  @param parent_hash the MD5 hash of the entry to add
   *  @return true if entry was successfully added, otherwise false
   */
  bool AddEntry(const DirectoryEntry &entry,
                const hash::t_md5 &path_hash, 
                const hash::t_md5 &parent_hash);
  
  inline bool AddEntry(const DirectoryEntry &entry,
                       const std::string &path) {
    return AddEntry(entry, hash::t_md5(path), hash::t_md5(get_parent_path(path)));
  }
  
  bool UpdateEntry(const DirectoryEntry &entry, const hash::t_md5 &path_hash);
  inline bool UpdateEntry(const DirectoryEntry &entry, const std::string &path) {
    return UpdateEntry(entry, hash::t_md5(path));
  }
  
  void InitPreparedStatements();
  void FinalizePreparedStatements();
  
 private:
  /**
   *  this method creates a new database file and initializes the database schema
   *  @param file_path the absolute path to the file to create
   *  @return true on success, false otherwise
   */
  static bool CreateNewDatabaseSchema(const std::string &file_path);
  
  bool MakeNestedCatalogMountpoint(const std::string &mountpoint);
  bool MakeNestedCatalogRootEntry();
  inline bool MoveDirectoryStructureToNewNestedCatalog(const std::string dir_structure_root,
                                                       WritableCatalog *new_nested_catalog,
                                                       std::list<std::string> &nested_catalog_mountpoints) {
    return MoveDirectoryStructureToNewNestedCatalogRecursively(dir_structure_root, new_nested_catalog, nested_catalog_mountpoints);
  }
  bool MoveDirectoryStructureToNewNestedCatalogRecursively(const std::string dir_structure_root,
                                                           WritableCatalog *new_nested_catalog,
                                                           std::list<std::string> &nested_catalog_mountpoints);
  bool MoveNestedCatalogReferencesToNewNestedCatalog(const std::list<std::string> &nested_catalog_references,
                                                     WritableCatalog *new_nested_catalog);
  
  /**
   *  mark this catalog as dirty
   *  meaning: something changed in this catalog, it definitely needs a new snapshot
   */
  inline void SetDirty() { dirty_ = true; }
  
 private:
  InsertDirectoryEntrySqlStatement   *insert_statement_;
  TouchSqlStatement                  *touch_statement_;
  UnlinkSqlStatement                 *unlink_statement_;
  UpdateDirectoryEntrySqlStatement   *update_statement_;
  GetMaximalHardlinkGroupIdStatement *max_hardlink_group_id_statement_;
                                                    
  bool dirty_;                     
};

typedef std::list<WritableCatalog*> WritableCatalogList;

}

#endif /* CVMFS_WRITABLE_CATALOG_H */
