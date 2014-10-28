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
typedef std::map<uint64_t, uint64_t> OwnerMap;  // used to map uid/gid


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

  inline void MakeDummy() { offset = 1; }

  inline bool IsInitialized() const { return offset > 0; }
  inline bool IsDummy() const { return IsInitialized() && size == 0; }
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
  friend class SqlLookup;                  // for mangled inode and uid/gid maps
  friend class swissknife::CommandMigrate; // for catalog version migration
 public:
  static const uint64_t kDefaultTTL = 900;  /**< 15 minutes default TTL */

  Catalog(const PathString  &path,
          const shash::Any   &catalog_hash,
                Catalog     *parent);
  virtual ~Catalog();

  static Catalog *AttachFreely(const std::string  &root_path,
                               const std::string  &file,
                               const shash::Any   &catalog_hash,
                                     Catalog      *parent = NULL);

  bool OpenDatabase(const std::string &db_path);

  bool LookupInode(const inode_t inode,
                   DirectoryEntry *dirent, shash::Md5 *parent_md5path) const;
  bool LookupMd5Path(const shash::Md5 &md5path, DirectoryEntry *dirent) const;
  inline bool LookupPath(const PathString &path, DirectoryEntry *dirent) const
  {
    return LookupMd5Path(shash::Md5(path.GetChars(), path.GetLength()), dirent);
  }
  bool LookupRawSymlink(const PathString &path, LinkString *raw_symlink) const;

  bool ListingMd5Path(const shash::Md5 &md5path,
                      DirectoryEntryList *listing) const;
  inline bool ListingPath(const PathString &path,
                      DirectoryEntryList *listing) const
  {
    return ListingMd5Path(shash::Md5(path.GetChars(), path.GetLength()),
                          listing);
  }
  bool ListingMd5PathStat(const shash::Md5 &md5path,
                          StatEntryList *listing) const;
  bool ListingPathStat(const PathString &path,
                       StatEntryList *listing) const
  {
    return ListingMd5PathStat(shash::Md5(path.GetChars(), path.GetLength()),
                              listing);
  }
  bool AllChunksBegin();
  bool AllChunksNext(shash::Any *hash, ChunkTypes *type);
  bool AllChunksEnd();

  inline bool ListPathChunks(const PathString &path,
                             const shash::Algorithms interpret_hashes_as,
                             FileChunkList *chunks) const
  {
    return ListMd5PathChunks(shash::Md5(path.GetChars(), path.GetLength()),
                             interpret_hashes_as, chunks);
  }
  bool ListMd5PathChunks(const shash::Md5 &md5path,
                         const shash::Algorithms interpret_hashes_as,
                         FileChunkList *chunks) const;

  uint64_t GetTTL() const;
  uint64_t GetRevision() const;
  uint64_t GetNumEntries() const;
  shash::Any GetPreviousRevision() const;
  const Counters& GetCounters() const { return counters_; };

  inline float schema() const { return database().schema_version(); }
  inline PathString path() const { return path_; }
  inline Catalog* parent() const { return parent_; }
  inline uint64_t max_row_id() const { return max_row_id_; }
  inline InodeRange inode_range() const { return inode_range_; }
  inline void set_inode_range(const InodeRange value) { inode_range_ = value; }
  inline std::string database_path() const { return database_->filename(); }
  inline PathString root_prefix() const { return root_prefix_; }
  inline shash::Any hash() const { return catalog_hash_; }
  inline bool volatile_flag() const { return volatile_flag_; }

  inline bool IsInitialized() const {
    return inode_range_.IsInitialized() && initialized_;
  }
  inline bool IsRoot() const { return NULL == parent_; }
  inline virtual bool IsWritable() const { return false; }

  typedef struct {
    PathString path;
    shash::Any hash;
    uint64_t size;
  } NestedCatalog;
  typedef std::vector<NestedCatalog> NestedCatalogList;
  const NestedCatalogList& ListNestedCatalogs() const;
  bool FindNested(const PathString &mountpoint,
                  shash::Any *hash, uint64_t *size) const;

  void SetInodeAnnotation(InodeAnnotation *new_annotation);
  void SetOwnerMaps(const OwnerMap *uid_map, const OwnerMap *gid_map);

 protected:
  typedef std::map<uint64_t, inode_t> HardlinkGroupMap;
  mutable HardlinkGroupMap hardlink_groups_;

  bool InitStandalone(const std::string &database_file);
  bool ReadCatalogCounters();

  /**
   * Specifies the SQLite open flags.  Overwritten by r/w catalog.
   */
  virtual CatalogDatabase::OpenMode DatabaseOpenMode() const {
    return CatalogDatabase::kOpenReadOnly;
  }

  virtual void InitPreparedStatements();
  void FinalizePreparedStatements();

  void AddChild(Catalog *child);
  void RemoveChild(Catalog *child);
  CatalogList GetChildren() const;
  Catalog* FindSubtree(const PathString &path) const;
  Catalog* FindChild(const PathString &mountpoint) const;

  Counters& GetCounters() { return counters_; };

  inline const CatalogDatabase &database() const { return *database_; }
  inline void set_parent(Catalog *catalog) { parent_ = catalog; }

  void ResetNestedCatalogCache();

 private:
  typedef std::map<PathString, Catalog*> NestedCatalogMap;

  uint64_t GetRowIdFromInode(const inode_t inode) const;
  inode_t GetMangledInode(const uint64_t row_id,
                          const uint64_t hardlink_group) const;

  void FixTransitionPoint(const shash::Md5 &md5path,
                          DirectoryEntry *dirent) const;

 private:
  bool LookupEntry(const shash::Md5 &md5path, const bool expand_symlink,
                   DirectoryEntry *dirent) const;
  CatalogDatabase *database_;
  pthread_mutex_t *lock_;

  const shash::Any catalog_hash_;
  PathString root_prefix_;
  PathString path_;
  bool volatile_flag_;

  Catalog *parent_;
  NestedCatalogMap children_;
  mutable NestedCatalogList nested_catalog_cache_;
  mutable bool              nested_catalog_cache_dirty_;

  bool initialized_;
  InodeRange inode_range_;
  uint64_t max_row_id_;
  InodeAnnotation *inode_annotation_;
  Counters counters_;
  // Point to the maps in the catalog manager
  const OwnerMap *uid_map_;
  const OwnerMap *gid_map_;

  SqlListing               *sql_listing_;
  SqlLookupPathHash        *sql_lookup_md5path_;
  SqlLookupInode           *sql_lookup_inode_;
  SqlNestedCatalogLookup   *sql_lookup_nested_;
  SqlNestedCatalogListing  *sql_list_nested_;
  SqlAllChunks             *sql_all_chunks_;
  SqlChunksListing         *sql_chunks_listing_;
};  // class Catalog

}  // namespace catalog

#endif  // CVMFS_CATALOG_H_
