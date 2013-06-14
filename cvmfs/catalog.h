/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_H_
#define CVMFS_CATALOG_H_

#include <stdint.h>
#include <pthread.h>

#include <cassert>

#include <string>
#include <map>
#include <vector>

#include "catalog_sql.h"
#include "directory_entry.h"
#include "file_chunk.h"
#include "hash.h"
#include "shortstring.h"
#include "sql.h"
#include "util.h"
#include "catalog_counters.h"

namespace swissknife {
  class CommandMigrate;
}

namespace catalog {

class AbstractCatalogManager;
class Catalog;

class Counters;

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
 * Allows to define a class that transforms the inode in order to ensure
 * that inodes are not reused after reloads (catalog or fuse module).
 * Currently, annotation is used to set an offset starting at the highest
 * so far issued inode.  The implementation is in the catalog manager.
 */
class InodeAnnotation {
 public:
  virtual ~InodeAnnotation() { };
  virtual inode_t Annotate(const inode_t raw_inode) = 0;
  virtual void IncGeneration(const uint64_t by) = 0;
  virtual inode_t GetGeneration() = 0;
  virtual bool ValidInode(const uint64_t inode) = 0;
  virtual inode_t Strip(const inode_t annotated_inode) = 0;
};


/**
 * This class wraps a catalog database and provides methods
 * to query for directory entries.
 * It has a pointer to its parent catalog and its children, thereby creating
 * a tree structure of nested catalogs.
 *
 * Read-only catalog. A sub-class provides read-write access.
 */
class Catalog : public SingleCopy {
  friend class AbstractCatalogManager;
  friend class SqlLookup;                  // for mangled inode
  friend class swissknife::CommandMigrate; // for catalog version migration
 public:
  static const uint64_t kDefaultTTL = 3600;  /**< 1 hour default TTL */

  Catalog(const PathString  &path,
          const hash::Any   &catalog_hash,
                Catalog     *parent);
  virtual ~Catalog();

  bool OpenDatabase(const std::string &db_path);

  bool LookupInode(const inode_t inode,
                   DirectoryEntry *dirent, hash::Md5 *parent_md5path) const;
  bool LookupMd5Path(const hash::Md5 &md5path, DirectoryEntry *dirent) const;
  inline bool LookupPath(const PathString &path, DirectoryEntry *dirent) const
  {
    return LookupMd5Path(hash::Md5(path.GetChars(), path.GetLength()), dirent);
  }

  bool ListingMd5Path(const hash::Md5 &md5path,
                      DirectoryEntryList *listing) const;
  inline bool ListingPath(const PathString &path,
                      DirectoryEntryList *listing) const
  {
    return ListingMd5Path(hash::Md5(path.GetChars(), path.GetLength()),
                          listing);
  }
  bool ListingMd5PathStat(const hash::Md5 &md5path,
                          StatEntryList *listing) const;
  bool ListingPathStat(const PathString &path,
                       StatEntryList *listing) const
  {
    return ListingMd5PathStat(hash::Md5(path.GetChars(), path.GetLength()),
                              listing);
  }
  bool AllChunksBegin();
  bool AllChunksNext(hash::Any *hash, ChunkTypes *type);
  bool AllChunksEnd();

  inline bool ListFileChunks(const PathString &path, FileChunkList *chunks) const
  {
    return ListMd5PathChunks(hash::Md5(path.GetChars(), path.GetLength()),
                             chunks);
  }
  bool ListMd5PathChunks(const hash::Md5 &md5path, FileChunkList *chunks) const;

  uint64_t GetTTL() const;
  uint64_t GetRevision() const;
  uint64_t GetNumEntries() const;
  hash::Any GetPreviousRevision() const;
  const Counters& GetCounters() const { return counters_; };

  inline bool read_only() const { return read_only_; }
  inline float schema() const { return database().schema_version(); }
  inline PathString path() const { return path_; }
  inline Catalog* parent() const { return parent_; }
  inline uint64_t max_row_id() const { return max_row_id_; }
  inline InodeRange inode_range() const { return inode_range_; }
  inline void set_inode_range(const InodeRange value) { inode_range_ = value; }
  inline std::string database_path() const { return database_->filename(); }
  inline PathString root_prefix() const { return root_prefix_; }
  inline hash::Any hash() const { return catalog_hash_; }

  inline bool IsInitialized() const {
    return inode_range_.IsInitialized() && (max_row_id_ > 0);
  }
  inline bool IsRoot() const { return NULL == parent_; }
  inline virtual bool IsWritable() const { return false; }

  typedef struct {
    PathString path;
    hash::Any hash;
  } NestedCatalog;
  typedef std::vector<NestedCatalog> NestedCatalogList;
  NestedCatalogList *ListNestedCatalogs() const;
  bool FindNested(const PathString &mountpoint, hash::Any *hash) const;

  void SetInodeAnnotation(InodeAnnotation *new_annotation);

 protected:
  typedef std::map<uint64_t, inode_t> HardlinkGroupMap;
  HardlinkGroupMap hardlink_groups_;

  /**
   * Specifies the SQLite open flags.  Overwritten by r/w catalog.
   */
  virtual sqlite::DbOpenMode DatabaseOpenMode() const {
    return sqlite::kDbOpenReadOnly;
  }

  virtual void InitPreparedStatements();
  void FinalizePreparedStatements();

  void AddChild(Catalog *child);
  void RemoveChild(Catalog *child);
  CatalogList GetChildren() const;
  Catalog* FindSubtree(const PathString &path) const;
  Catalog* FindChild(const PathString &mountpoint) const;

  Counters& GetCounters() { return counters_; };

  inline const Database &database() const { return *database_; }
  inline void set_parent(Catalog *catalog) { parent_ = catalog; }

  bool read_only_;

 private:
  typedef std::map<PathString, Catalog*> NestedCatalogMap;

  uint64_t GetRowIdFromInode(const inode_t inode) const;
  inode_t GetMangledInode(const uint64_t row_id,
                          const uint64_t hardlink_group);

  void FixTransitionPoint(const hash::Md5 &md5path,
                          DirectoryEntry *dirent) const;

  Database *database_;
  pthread_mutex_t *lock_;

  const hash::Any catalog_hash_;
  PathString root_prefix_;
  PathString path_;

  Catalog *parent_;
  NestedCatalogMap children_;
  mutable NestedCatalogList *nested_catalog_cache_;

  InodeRange inode_range_;
  uint64_t max_row_id_;
  InodeAnnotation *inode_annotation_;
  Counters counters_;

  SqlListing               *sql_listing_;
  SqlLookupPathHash        *sql_lookup_md5path_;
  SqlLookupInode           *sql_lookup_inode_;
  SqlNestedCatalogLookup   *sql_lookup_nested_;
  SqlNestedCatalogListing  *sql_list_nested_;
  SqlAllChunks             *sql_all_chunks_;
  SqlChunksListing         *sql_chunks_listing_;
};  // class Catalog

Catalog *AttachFreely(const std::string  &root_path,
                      const std::string  &file,
                      const hash::Any    &catalog_hash,
                            Catalog      *parent = NULL);

}  // namespace catalog

#endif  // CVMFS_CATALOG_H_
