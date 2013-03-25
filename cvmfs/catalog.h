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
#include "dirent.h"
#include "file_chunk.h"
#include "hash.h"
#include "shortstring.h"
#include "duplex_sqlite3.h"
#include "util.h"


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


struct DeltaCounters {
  DeltaCounters() {
    SetZero();
  }
  void SetZero();
  void PopulateToParent(DeltaCounters *parent);
  void DeltaDirent(const DirectoryEntry &dirent, const int delta);

  int64_t d_self_regular;
  int64_t d_self_symlink;
  int64_t d_self_dir;
  int64_t d_self_nested;
  int64_t d_subtree_regular;
  int64_t d_subtree_symlink;
  int64_t d_subtree_dir;
  int64_t d_subtree_nested;
};


struct Counters {
  Counters() {
    self_regular = self_symlink = self_dir = self_nested =
      subtree_regular = subtree_symlink = subtree_dir = subtree_nested = 0;
  }

  void ApplyDelta(const DeltaCounters &delta);
  void AddAsSubtree(DeltaCounters *delta);
  void MergeIntoParent(DeltaCounters *parent_delta);
  uint64_t GetSelfEntries() const;
  uint64_t GetSubtreeEntries() const;
  uint64_t GetAllEntries() const;

  uint64_t self_regular;
  uint64_t self_symlink;
  uint64_t self_dir;
  uint64_t self_nested;
  uint64_t subtree_regular;
  uint64_t subtree_symlink;
  uint64_t subtree_dir;
  uint64_t subtree_nested;
};

/**
 * Allows to define a class that stores additional information in the upper
 * bits of an inode.  Cvmfs will abort if the inode is too large to fit into
 * the protected lower bits.
 * Currently, annotation is used to mangle the catalog revision number into
 * the inode.  The implementation is in the catalog manager.  Mangling of the
 * revision number is necessary in order to avoid the following situation:
 *   1) Process A opens file /foo and starts to read it.
 *   2) The catalog is reloaded, all caches are flushed.  In the new catalog,
 *      file /foo has another content.
 *   3) Process B opens file /foo (same inode, different content) and reads
 *      the new content completely, thereby fills the kernel caches.
 *   4) Process A reads the rest of /foo, which is now served in the new version
 *      from the kernel caches.
 * Also, reloading of the fuse module increases the generation (it invalidates)
 * the inodes
 */
class InodeAnnotation {
 public:
  virtual ~InodeAnnotation() { };
  virtual inode_t Annotate(const inode_t raw_inode) = 0;
  virtual void SetGeneration(const uint64_t new_generation) = 0;
  // Used to detect ancient inodes from previous generations
  virtual bool ValidInode(const uint64_t inode) { return true; }
  
  inode_t Strip(const inode_t annotated_inode) {
    // Clear upper bits
    return ((uint64_t(1) << num_protected_bits_) - 1) & annotated_inode;
  }
  unsigned num_protected_bits() { return num_protected_bits_; }

 protected:
  unsigned num_protected_bits_;
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
  friend class SqlLookup;  // for mangled inode
 public:
  static const uint64_t kDefaultTTL = 3600;  /**< 1 hour default TTL */

  Catalog(const PathString &path, Catalog *parent);
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

  inline bool ListFileChunks(const PathString &path, FileChunks *chunks) const {
    return ListMd5PathChunks(hash::Md5(path.GetChars(), path.GetLength()),
                             chunks);
  }
  bool ListMd5PathChunks(const hash::Md5 &md5path, FileChunks *chunks) const;

  uint64_t GetTTL() const;
  uint64_t GetRevision() const;
  uint64_t GetGeneration() const { return generation_; }
  uint64_t GetNumEntries() const;
  hash::Any GetPreviousRevision() const;
  bool GetCounters(Counters *counters) const;

  inline bool read_only() const { return read_only_; }
  inline float schema() const { return database().schema_version(); }
  inline PathString path() const { return path_; }
  inline Catalog* parent() const { return parent_; }
  inline uint64_t max_row_id() const { return max_row_id_; }
  inline InodeRange inode_range() const { return inode_range_; }
  inline void set_inode_range(const InodeRange value) { inode_range_ = value; }
  inline std::string database_path() const { return database_->filename(); }
  inline PathString root_prefix() const { return root_prefix_; }

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
  virtual Database::OpenMode DatabaseOpenMode() const {
    return Database::kOpenReadOnly;
  }

  virtual void InitPreparedStatements();
  void FinalizePreparedStatements();

  void AddChild(Catalog *child);
  void RemoveChild(Catalog *child);
  CatalogList GetChildren() const;
  Catalog* FindSubtree(const PathString &path) const;
  Catalog* FindChild(const PathString &mountpoint) const;

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

  PathString root_prefix_;
  PathString path_;

  // The revision of the tree at the time the catalog was attached plus the
  // "reload counter", i.e. how often the tree inodes have been invalidated
  // e.g. by reloading cvmfs
  uint64_t generation_;
  Catalog *parent_;
  NestedCatalogMap children_;
  mutable NestedCatalogList *nested_catalog_cache_;

  InodeRange inode_range_;
  uint64_t max_row_id_;
  InodeAnnotation *inode_annotation;

  SqlListing               *sql_listing_;
  SqlLookupPathHash        *sql_lookup_md5path_;
  SqlLookupInode           *sql_lookup_inode_;
  SqlNestedCatalogLookup   *sql_lookup_nested_;
  SqlNestedCatalogListing  *sql_list_nested_;
  SqlAllChunks             *sql_all_chunks_;
  SqlChunksListing         *sql_chunks_listing_;
};  // class Catalog

Catalog *AttachFreely(const std::string &root_path, const std::string &file);

}  // namespace catalog

#endif  // CVMFS_CATALOG_H_
