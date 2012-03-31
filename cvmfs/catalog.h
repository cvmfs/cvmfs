/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_H_
#define CVMFS_CATALOG_H_

#include <stdint.h>
#include <pthread.h>

#include <cassert>

#include <string>
#include <list>
#include <map>

#include "catalog_queries.h"
#include "dirent.h"
#include "hash.h"
#include "duplex_sqlite3.h"

namespace catalog {

class AbstractCatalogManager;
class Catalog;

typedef std::vector<Catalog *> CatalogList;


/**
 * Every Catalog gets an InodeRange assigned when attached to
 * a CatalogManager.  Inodes are assigned at runtime out of this InodeRange.
 * An inode is computed by <row ID of entry> + offset
 */
struct InodeRange {
  uint64_t offset;
  uint64_t size;

  InodeRange() : offset(0), size(0) { }

  inline bool ContainsInode(const inode_t inode) const {
    return ((inode > offset) && (inode <= size + offset));
  }

  inline bool IsInitialized() const { return (offset > 0) && (size > 0); }
};


/**
 * This class wraps a catalog database and provides methods
 * to query for directory entries.
 * It has a pointer to its parent catalog and its children, thereby creating
 * a tree structure of nested catalogs.
 *
 * Read-only catalog. A sub-class provides read-write access.
 */
class Catalog {
  friend class AbstractCatalogManager;
  friend class LookupSqlStatement;  // for mangled inode
 public:
  Catalog(const std::string &path, Catalog *parent);
  virtual ~Catalog();

  bool OpenDatabase(const std::string &db_path);

  bool LookupInode(const inode_t inode,
                   DirectoryEntry *dirent, hash::Md5 *parent_md5path) const;
  bool LookupMd5Path(const hash::Md5 &md5path, DirectoryEntry *dirent) const;
  inline bool LookupPath(const std::string &path, DirectoryEntry *dirent) const
  {
    return LookupMd5Path(hash::Md5(hash::AsciiPtr(path)), dirent);
  }

  bool ListingMd5Path(const hash::Md5 &md5path,
                      DirectoryEntryList *listing) const;
  inline bool ListingPath(const std::string &path,
                      DirectoryEntryList *listing) const
  {
    return ListingMd5Path(hash::Md5(hash::AsciiPtr(path)), listing);
  }

  uint64_t GetTTL() const;
  uint64_t GetRevision() const;

  inline std::string path() const { return path_; }
  inline Catalog* parent() const { return parent_; }
  inline uint64_t max_row_id() const { return max_row_id_; }
  inline InodeRange inode_range() const { return inode_range_; }
  inline void set_inode_range(const InodeRange value) { inode_range_ = value; }

  inline bool IsInitialized() const {
    return inode_range_.IsInitialized() && (max_row_id_ > 0);
  }
  inline bool IsRoot() const { return NULL == parent_; }
  inline virtual bool IsWritable() const { return false; }

  typedef struct {
    std::string path;
    hash::Any hash;
  } NestedCatalog;
  typedef std::list<NestedCatalog> NestedCatalogList;
  NestedCatalogList ListNestedCatalogs() const;
  bool FindNested(const std::string &mountpoint, hash::Any *hash) const;

 protected:
  /**
   * Specifies the SQLite open flags.  Overwritten by r/w catalog.
   */
  virtual inline int DatabaseOpenFlags() const {
    return SQLITE_OPEN_NOMUTEX | SQLITE_OPEN_READONLY;
  }

  virtual void InitPreparedStatements();
  void FinalizePreparedStatements();

  void AddChild(Catalog *child);
  void RemoveChild(Catalog *child);
  CatalogList GetChildren() const;
  Catalog* FindSubtree(const std::string &path) const;
  Catalog* FindChild(const std::string &mountpoint) const;

  inline sqlite3* database() const { return database_; }
  inline std::string database_path() const { return database_path_; }
  inline void set_parent(Catalog *catalog) { parent_ = catalog; }

 private:
  static const uint64_t kDefaultTTL = 3600;  /**< 1 hour default TTL */
  typedef std::map<int, inode_t> HardlinkGroupMap;
  typedef std::map<std::string, Catalog*> NestedCatalogMap;

  inline uint64_t GetRowIdFromInode(const inode_t inode) const {
    return inode - inode_range_.offset;
  }
  inode_t GetMangledInode(const uint64_t row_id,
                          const uint64_t hardlink_group);

  void FixTransitionPoint(const hash::Md5 &md5path,
                          DirectoryEntry *dirent) const;

  sqlite3 *database_;
  std::string database_path_;
  pthread_mutex_t *lock_;

  std::string root_prefix_;
  std::string path_;

  Catalog *parent_;
  NestedCatalogMap children_;

  InodeRange inode_range_;
  uint64_t max_row_id_;

  HardlinkGroupMap hardlink_groups_;

  ListingLookupSqlStatement *sql_listing_;
  PathHashLookupSqlStatement *sql_lookup_md5path_;
  InodeLookupSqlStatement *sql_lookup_inode_;
  FindNestedCatalogSqlStatement *sql_lookup_nested_;
  ListNestedCatalogsSqlStatement *sql_list_nested_;
};  // class Catalog

}  // namespace catalog

#endif  // CVMFS_CATALOG_H_
