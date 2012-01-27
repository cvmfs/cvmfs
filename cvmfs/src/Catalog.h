/**
 *  This class wraps a catalog database and provides methods
 *  to query this catalog
 *  Furthermore it has a pointer to it's parent catalog and it's children
 *  to create a tree structure of nested catalogs.
 *
 *  Catalog is meant to be derived to add additional functionality.
 *  I.e. this catalog implementation is read-only and there exists a WritableCatalog,
 *  which adds write access.
 *
 *  Every Catalog gets a specific InodeChunk assigned on attachment to
 *  a CatalogManager. Inodes of containing files are assigned at runtime,
 *  out of this InodeChunk.
 */

#ifndef CATALOG_CLASS_H
#define CATALOG_CLASS_H 1

#include <stdint.h>
#include <string>
#include <list>
#include <map>
#include <assert.h>

#include "catalog_queries.h"
#include "DirectoryEntry.h"
#include "hash.h"
#include "thread_safe.h"

extern "C" {
   #include "sqlite3-duplex.h"
}

namespace cvmfs {

class Catalog;
typedef std::list<Catalog*> CatalogList;

class CatalogManager;

/**
 *  this is just a simple struct to provide easy access to an
 *  InodeChunk. It holds two things:
 *   - the offset
 *   - the size
 *  an inode is computed by <rowID of entry> + offset
 */
struct InodeChunk {
  uint64_t offset;
  uint64_t size;
  
  InodeChunk() {
    offset = 0;
    size = 0;
  }
  
  /**
   *  simply checks if a given inode is within the
   *  range of this InodeChunk
   *  @param inode the inode to check
   *  @return true if the given inode is in this InodeChunk, false otherwise
   */
  inline bool ContainsInode(const inode_t inode) const {
    return ((uint64_t)inode > offset && inode <= size + offset);
  }
  
  inline bool IsInitialized() const { return offset > 0 && size > 0; }
};

class Catalog {
 public:
  Catalog(const std::string &path, Catalog *parent);
  virtual ~Catalog();
  
  /**
   *  this method must be called after the Catalog object was created
   *  it establishes the database structures and opens the sqlite database file
   *  @param db_file the absolute path to the database file on local file system
   *  @return true on successful initialization otherwise false
   */
  bool OpenDatabase(const std::string &db_file);
  inline bool IsInitialized() const { return inode_chunk_.IsInitialized() && max_row_id_ > 0; }
  
 public:
  /**
   *  checks if this Catalog is considered to be the root catalog (of a CatalogManager)
   *  @return true if it seems to be a root catalog, false otherwise
   */
  inline bool IsRoot() const { return NULL == parent_; }
  
  /**
   *  virtual method specifying if this Catalog has write capabilities
   *  this method is overridden by WritableCatalog
   *  @return true if this Catalog is writable, false otherwise
   */
  inline virtual bool IsWritable() const { return false; }
  
  /**
   *  performs a lookup on this Catalog for a given inode
   *  @param inode the inode to perform the lookup for
   *  @param entry this will be set to the found entry
   *  @param parent_hash this will be set to the hash of the parent path
   *  @return true if lookup was successful, false otherwise
   */
  bool Lookup(const inode_t inode, 
              DirectoryEntry *entry = NULL, 
              hash::t_md5 *parent_hash = NULL) const;
              
  /**
   *  performs a lookup on this Catalog for a given path hash
   *  @param path_hash the MD5 hash of the searched path
   *  @param entry will be set to the found DirectoryEntry
   *  @return true if DirectoryEntry was successfully found, false otherwise
   */
  bool Lookup(const hash::t_md5 &path_hash,
              DirectoryEntry *entry = NULL) const;
  
  /**
   *  performs a lookup on this Catalog for a given path
   *  @param path the path of the searched entry
   *  @param entry will be set to the found DirectoryEntry
   *  @return true if DirectoryEntry was successfully found, false otherwise
   */ 
  inline bool Lookup(const std::string &path, 
                     DirectoryEntry *entry = NULL) const {
    return Lookup(hash::t_md5(path), entry);
  }
  
  /**
   *  perform a listing of the directory with the given inode
   *  this method is currently not implemented due to complications
   *  with nested catalogs, but might be in the future
   *  @param inode the inode of the directory to list
   *  @param listing will be set to the resulting DirectoryEntryList
   *  @return true on successful listing, false otherwise
   */
  bool Listing(const inode_t inode, 
               DirectoryEntryList *listing) const;
  
  /**
   *  perform a listing of the directory with the given path hash
   *  @param path_hash the MD5 hash of the path of the directory to list
   *  @param listing will be set to the resulting DirectoryEntryList
   *  @return true on successful listing, false otherwise
   */ 
  bool Listing(const hash::t_md5 &path_hash,
               DirectoryEntryList *listing) const;
  
  /**
   *  perform a listing of the directory with the given path 
   *  @param path the path of the directory to list
   *  @param listing will be set to the resulting DirectoryEntryList
   *  @return true on successful listing, false otherwise
   */ 
  inline bool Listing(const std::string &path, 
                      DirectoryEntryList *listing) const {
    return Listing(hash::t_md5(path), listing);
  }
  
  /**
   *  add a Catalog as child to this Catalog
   *  this is meant to be used only internally!!
   *  @param child the Catalog to define as child
   */
  void AddChild(Catalog *child);
  
  /**
   *  removes a Catalog from the children list of this Catalog
   *  this is meant to be used only internally!!
   *  @param child the Catalog to delete as child
   */
  void RemoveChild(const Catalog *child);
  
  /**
   *  checks if a given inode might be maintained by this Catalog
   *  @return true if given inodes lies in range, false otherwise
   */
  inline bool ContainsInode(const inode_t inode) const {
    assert(IsInitialized()); 
    return inode_chunk_.ContainsInode(inode);
  }
  
  /**
   *  retrieve the root entry of this catalog
   *  @param[out] root_entry the root_entry we look for
   *  @return true if root entry was found, false otherwise
   */
  inline bool GetRootEntry(DirectoryEntry *root_entry) const { 
    return Lookup(path(), root_entry);
  }
  
  /**
   *  retrieve the TTL value for this Catalog
   *  @return the TTL in seconds
   */
  uint64_t GetTTL() const;
  
  /**
   *  retrieve the revision number of this Catalog
   *  @return the revision number of this Catalog
   */
  uint64_t GetRevision() const;
  
  inline const CatalogList& children() const { return children_; }
  inline std::string path() const { return path_; }
  inline Catalog* parent() const { return parent_; }
  inline uint64_t max_row_id() const { return max_row_id_; }
  inline InodeChunk inode_chunk() const { return inode_chunk_; }
  inline void set_inode_chunk(const InodeChunk chunk) { inode_chunk_ = chunk; }
  
  /**
   *  compute the actual inode of a DirectoryEntry
   *  @param row_id the row id of a read row in the sqlite database
   *  @param hardlink_group_id the id of a possibly present hardlink group
   *  @return the assigned inode number
   */
  inode_t GetInodeFromRowIdAndHardlinkGroupId(uint64_t row_id, 
                                              uint64_t hardlink_group_id);

  typedef struct {
    std::string path;
    hash::t_sha1 content_hash;
  } NestedCatalogReference;
  typedef std::list<NestedCatalogReference> NestedCatalogReferenceList;

  /**
   *  get a list of all registered nested catalogs in this catalog
   *  @return a list of all nested catalog references of this catalog
   */
  NestedCatalogReferenceList ListNestedCatalogReferences() const;
  
 protected:
  /**
   *  this virtual method specifies the SQLite flags with which
   *  SQLite will open the database flag. This might depend on derived classes
   *  and can be overridden (f.e. see WritableCatalog)
   *  @return an integer containing the configured database open flags
   */
  virtual inline int DatabaseOpenFlags() const { return SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READONLY; }
  
  // !!!!!!!!!!!!!!!!!!!!!!!!!! CAUTION HOT !!!!!!!!!!!!!!!!!!!!!!!!!!
  // InitPreparedStatement is called in Catalog::OpenDatabase and uses
  // polymorphism in case of a WritableCatalog object.
  // While FinalizePreparedStatements is called in the destructor where
  // polymorphism does not work any more and has to be called both in
  // the WritableCatalog and the Catalog destructor
  // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  virtual void InitPreparedStatements();
  void FinalizePreparedStatements();
  
  /**
   *  check if we have a child embodying the given path
   *  @param mountpoint the mountpoint of the child nested catalog to look for
   *  @return a pointer to the found Catalog or NULL if not found
   */
  Catalog* FindChildWithMountpoint(const std::string &mountpoint) const;
  
  inline sqlite3* database() const { return database_; }
  inline std::string database_file() const { return database_file_; }
  inline void set_parent(Catalog *catalog) { parent_ = catalog; }
  
 private:
  /**
   *  use this method to get the row_id in the database from a given inode
   *  @param inode the inode to conclude from
   *  @return the row id of the inode in the specific database
   */
  inline uint64_t GetRowIdFromInode(const inode_t inode) const { return inode - inode_chunk_.offset; }
  
  /**
   *  if a nested Catalog is loaded we have ambiquitous inode information
   *  for the root directory of this nested catalog. To keep inodes consistent
   *  we use this method.
   *  @param path_hash the MD5 hash of the entry to check
   *  @param entry the DirectoryEntry to perform coherence fixes on
   *  @return true on success, false otherwise
   */
  bool EnsureCoherenceOfInodes(const hash::t_md5 &path_hash, DirectoryEntry *entry) const;
  
 private:
  static const uint64_t kDefaultTTL = 3600; ///< Default TTL for a catalog is one hour.
  static const uint64_t GROW_EPOCH;
  static const int      SQLITE_THREAD_MEM; ///< SQLite3 heap limit per thread
  
  typedef std::map<int, inode_t> HardlinkGroupIdMap;

 private:
  std::string database_file_;
  sqlite3 *database_; ///< The SQLite3 database handle for this catalog

  std::string root_prefix_; ///< If we mount deep into a nested catalog, we need the full preceeding path to calculate the correct MD5 hash
  std::string path_;
  
  Catalog *parent_;
  CatalogList children_;
  
  InodeChunk inode_chunk_;
  uint64_t max_row_id_;
  
  HardlinkGroupIdMap hardlink_groups_;
  
  ListingLookupSqlStatement *listing_statement_;
  PathHashLookupSqlStatement *path_hash_lookup_statement_;
  InodeLookupSqlStatement *inode_lookup_statement_;
  FindNestedCatalogSqlStatement *find_nested_catalog_statement_;
  ListNestedCatalogsSqlStatement *list_nested_catalogs_statement_;
};

}

#endif /* CATALOG_CLASS_H */
