/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_H_
#define CVMFS_CATALOG_H_

#include <pthread.h>
#include <stdint.h>

#include <cassert>
#include <map>
#include <string>
#include <vector>

#include "catalog_counters.h"
#include "catalog_sql.h"
#include "crypto/hash.h"
#include "directory_entry.h"
#include "file_chunk.h"
#include "gtest/gtest_prod.h"
#include "shortstring.h"
#include "sql.h"
#include "uid_map.h"
#include "xattr.h"

namespace swissknife {
class CommandMigrate;
}

namespace catalog {

template<class CatalogT>
class AbstractCatalogManager;

class Catalog;

class Counters;

typedef std::vector<Catalog *> CatalogList;
typedef IntegerMap<uint64_t> OwnerMap;  // used to map uid/gid


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
  virtual ~InodeAnnotation() { }
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
class Catalog : SingleCopy {
  FRIEND_TEST(T_Catalog, NormalizePath);
  FRIEND_TEST(T_Catalog, PlantPath);
  friend class swissknife::CommandMigrate;  // for catalog version migration

 public:
  typedef std::vector<shash::Any> HashVector;

  /**
   * The default TTL should be shorter than the autofs idle unmount time
   * which is 5 minutes, because the config repo is accessed on every root
   * catalog refresh and we want to avoid thrashing that mountpoint.
   */
  static const uint64_t kDefaultTTL = 240; /**< 4 minutes default TTL */

  /**
   * Note: is_nested only has an effect if parent == NULL otherwise being
   *       a root catalog is determined by having a parent pointer or not.
   */
  Catalog(const PathString &mountpoint,
          const shash::Any &catalog_hash,
          Catalog *parent,
          const bool is_nested = false);
  virtual ~Catalog();

  static Catalog *AttachFreely(const std::string &imaginary_mountpoint,
                               const std::string &file,
                               const shash::Any &catalog_hash,
                               Catalog *parent = NULL,
                               const bool is_nested = false);

  bool OpenDatabase(const std::string &db_path);

  inline bool LookupPath(const PathString &path, DirectoryEntry *dirent) const {
    return LookupMd5Path(NormalizePath(path), dirent);
  }
  bool LookupRawSymlink(const PathString &path, LinkString *raw_symlink) const;
  bool LookupXattrsPath(const PathString &path, XattrList *xattrs) const {
    return LookupXattrsMd5Path(NormalizePath(path), xattrs);
  }

  inline bool ListingPath(const PathString &path,
                          DirectoryEntryList *listing,
                          const bool expand_symlink = true) const {
    return ListingMd5Path(NormalizePath(path), listing, expand_symlink);
  }
  bool ListingPathStat(const PathString &path, StatEntryList *listing) const {
    return ListingMd5PathStat(NormalizePath(path), listing);
  }
  bool AllChunksBegin();
  bool AllChunksNext(shash::Any *hash, zlib::Algorithms *compression_alg);
  bool AllChunksEnd();

  inline bool ListPathChunks(const PathString &path,
                             const shash::Algorithms interpret_hashes_as,
                             FileChunkList *chunks) const {
    return ListMd5PathChunks(NormalizePath(path), interpret_hashes_as, chunks);
  }

  CatalogList GetChildren() const;
  Catalog *FindSubtree(const PathString &path) const;
  Catalog *FindChild(const PathString &mountpoint) const;
  void AddChild(Catalog *child);
  void RemoveChild(Catalog *child);

  const HashVector &GetReferencedObjects() const;
  void TakeDatabaseFileOwnership();
  void DropDatabaseFileOwnership();
  bool OwnsDatabaseFile() const {
    return ((database_ != NULL) && database_->OwnsFile()) || managed_database_;
  }

  uint64_t GetTTL() const;
  bool HasExplicitTTL() const;
  uint64_t GetRevision() const;
  bool GetVOMSAuthz(std::string *authz) const;
  uint64_t GetLastModified() const;
  uint64_t GetNumEntries() const;
  uint64_t GetNumChunks() const;
  shash::Any GetPreviousRevision() const;
  const Counters &GetCounters() const { return counters_; }
  std::string PrintMemStatistics() const;

  inline float schema() const { return database().schema_version(); }
  inline PathString mountpoint() const { return mountpoint_; }
  inline Catalog *parent() const { return parent_; }
  inline uint64_t max_row_id() const { return max_row_id_; }
  inline InodeRange inode_range() const { return inode_range_; }
  inline void set_inode_range(const InodeRange value) { inode_range_ = value; }
  inline std::string database_path() const { return database_->filename(); }
  inline PathString root_prefix() const { return root_prefix_; }
  inline shash::Any hash() const { return catalog_hash_; }
  inline bool volatile_flag() const { return volatile_flag_; }
  inline uint64_t revision() const { return GetRevision(); }

  inline bool IsInitialized() const {
    return inode_range_.IsInitialized() && initialized_;
  }
  inline bool IsRoot() const { return is_root_; }
  bool IsAutogenerated() const {
    DirectoryEntry dirent;
    assert(IsInitialized());
    return LookupPath(PathString(mountpoint_.ToString() + "/.cvmfsautocatalog"),
                      &dirent);
  }
  inline bool HasParent() const { return parent_ != NULL; }
  inline virtual bool IsWritable() const { return false; }

  typedef struct {
    PathString mountpoint;
    shash::Any hash;
    uint64_t size;
  } NestedCatalog;
  typedef std::vector<NestedCatalog> NestedCatalogList;
  const NestedCatalogList &ListNestedCatalogs() const;
  const NestedCatalogList ListOwnNestedCatalogs() const;
  bool FindNested(const PathString &mountpoint, shash::Any *hash,
                  uint64_t *size) const;

  void SetInodeAnnotation(InodeAnnotation *new_annotation);
  inode_t GetMangledInode(const uint64_t row_id,
                          const uint64_t hardlink_group) const;

  void SetOwnerMaps(const OwnerMap *uid_map, const OwnerMap *gid_map);
  uint64_t MapUid(const uint64_t uid) const {
    if (uid_map_) {
      return uid_map_->Map(uid);
    }
    return uid;
  }
  uint64_t MapGid(const uint64_t gid) const {
    if (gid_map_) {
      return gid_map_->Map(gid);
    }
    return gid;
  }

 protected:
  typedef std::map<uint64_t, inode_t> HardlinkGroupMap;
  mutable HardlinkGroupMap hardlink_groups_;

  pthread_mutex_t *lock_;

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

  Counters &GetWritableCounters() { return counters_; }

  inline const CatalogDatabase &database() const { return *database_; }
  inline CatalogDatabase &database() { return *database_; }
  inline void set_parent(Catalog *catalog) { parent_ = catalog; }

  void ResetNestedCatalogCacheUnprotected();

  bool LookupMd5Path(const shash::Md5 &md5path, DirectoryEntry *dirent) const;

 private:
  typedef std::map<PathString, Catalog *> NestedCatalogMap;

  /**
   * The hash of the empty string.  Used to identify the root entry of a
   * repository, which is the child transition point of a bind mountpoint.
   */
  static const shash::Md5 kMd5PathEmpty;

  enum VomsAuthzStatus {
    kVomsUnknown,  // Not yet looked up
    kVomsNone,     // No voms_authz key in properties table
    kVomsPresent,  // voms_authz property available
  };

  shash::Md5 NormalizePath(const PathString &path) const;
  PathString NormalizePath2(const PathString &path) const;
  PathString PlantPath(const PathString &path) const;

  void FixTransitionPoint(const shash::Md5 &md5path,
                          DirectoryEntry *dirent) const;

  bool LookupXattrsMd5Path(const shash::Md5 &md5path, XattrList *xattrs) const;
  bool ListMd5PathChunks(const shash::Md5 &md5path,
                         const shash::Algorithms interpret_hashes_as,
                         FileChunkList *chunks) const;
  bool ListingMd5Path(const shash::Md5 &md5path,
                      DirectoryEntryList *listing,
                      const bool expand_symlink = true) const;
  bool ListingMd5PathStat(const shash::Md5 &md5path,
                          StatEntryList *listing) const;
  bool LookupEntry(const shash::Md5 &md5path, const bool expand_symlink,
                   DirectoryEntry *dirent) const;

  CatalogDatabase *database_;

  const shash::Any catalog_hash_;
  PathString root_prefix_;
  /**
   * Normally, catalogs are mounted at their root_prefix_. But for the structure
   * under /.cvmfs/snapshots/..., that's not the case.
   */
  PathString mountpoint_;
  /**
   * True, iff root_prefix_ == mountpoint_
   */
  bool is_regular_mountpoint_;
  bool volatile_flag_;
  /**
   * For catalogs in a catalog manager: doesn't have a parent catalog
   */
  const bool is_root_;
  bool managed_database_;

  Catalog *parent_;
  NestedCatalogMap children_;
  mutable NestedCatalogList nested_catalog_cache_;
  mutable bool nested_catalog_cache_dirty_;

  mutable VomsAuthzStatus voms_authz_status_;
  mutable std::string voms_authz_;

  bool initialized_;
  InodeRange inode_range_;
  uint64_t max_row_id_;
  InodeAnnotation *inode_annotation_;
  Counters counters_;
  // Point to the maps in the catalog manager
  const OwnerMap *uid_map_;
  const OwnerMap *gid_map_;

  SqlListing *sql_listing_;
  SqlLookupPathHash *sql_lookup_md5path_;
  SqlNestedCatalogLookup *sql_lookup_nested_;
  SqlNestedCatalogListing *sql_list_nested_;
  SqlOwnNestedCatalogListing *sql_own_list_nested_;
  SqlAllChunks *sql_all_chunks_;
  SqlChunksListing *sql_chunks_listing_;
  SqlLookupXattrs *sql_lookup_xattrs_;

  mutable HashVector referenced_hashes_;
};  // class Catalog

}  // namespace catalog

#endif  // CVMFS_CATALOG_H_
