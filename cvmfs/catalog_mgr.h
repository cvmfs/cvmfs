/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_MGR_H_
#define CVMFS_CATALOG_MGR_H_

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <pthread.h>
#include <cassert>

#include <vector>
#include <map>
#include <string>

#include "catalog.h"
#include "directory_entry.h"
#include "file_chunk.h"
#include "hash.h"
#include "atomic.h"
#include "util.h"
#include "logging.h"

namespace catalog {

const unsigned kSqliteMemPerThread = 1*1024*1024;

/**
 * Lookup a directory entry including its parent entry or not.
 */
enum LookupOptions {
  kLookupSole        = 0x01,
  kLookupFull        = 0x02,
  kLookupRawSymlink  = 0x10,
};


/**
 * Results upon loading a catalog file.
 */
enum LoadError {
  kLoadNew = 0,
  kLoadUp2Date,
  kLoadNoSpace,
  kLoadFail,
};

inline const char *Code2Ascii(const LoadError error) {
  const int kNumElems = 4;
  if (error >= kNumElems)
    return "no text available (internal error)";

  const char *texts[kNumElems];
  texts[0] = "loaded new catalog";
  texts[1] = "catalog was up to date";
  texts[2] = "not enough space to load catalog";
  texts[3] = "failed to load catalog";

  return texts[error];
}


struct Statistics {
  atomic_int64 num_lookup_inode;
  atomic_int64 num_lookup_path;
  atomic_int64 num_lookup_path_negative;
  atomic_int64 num_listing;
  atomic_int64 num_nested_listing;

  Statistics() {
    atomic_init64(&num_lookup_inode);
    atomic_init64(&num_lookup_path);
    atomic_init64(&num_lookup_path_negative);
    atomic_init64(&num_listing);
    atomic_init64(&num_nested_listing);
  }

  std::string Print() {
    return
      "lookup(inode): " + StringifyInt(atomic_read64(&num_lookup_inode)) +
      "    " +
      "lookup(path-all): " + StringifyInt(atomic_read64(&num_lookup_path)) +
      "    " +
      "lookup(path-negative): " +
        StringifyInt(atomic_read64(&num_lookup_path_negative)) +
      "    " +
      "listing: " + StringifyInt(atomic_read64(&num_listing)) +
      "    " +
      "listing nested catalogs: " +
        StringifyInt(atomic_read64(&num_nested_listing)) + "\n";
  }
};


class InodeGenerationAnnotation : public InodeAnnotation {
 public:
  InodeGenerationAnnotation() { inode_offset_ = 0; };
  ~InodeGenerationAnnotation() { };
  bool ValidInode(const uint64_t inode) {
    return inode >= inode_offset_;
  }
  inode_t Annotate(const inode_t raw_inode) {
    return raw_inode + inode_offset_;
  }
  inode_t Strip(const inode_t annotated_inode) {
    return annotated_inode - inode_offset_;
  }
  void IncGeneration(const uint64_t by) {
    inode_offset_ += by;
    LogCvmfs(kLogCatalog, kLogDebug, "set inode generation to %lu",
             inode_offset_);
  }
  inode_t GetGeneration() { return inode_offset_; };

 private:
  uint64_t inode_offset_;
};


class AbstractCatalogManager;
/**
 * Here, the Cwd Buffer is registered in order to save the inodes of
 * processes' cwd before a new catalog snapshot is applied
 */
class RemountListener {
 public:
  virtual ~RemountListener() { }
  virtual void BeforeRemount(AbstractCatalogManager *source) = 0;
};


/**
 * This class provides the read-only interface to a tree of catalogs
 * representing a (subtree of a) repository.
 * Mostly lookup functions filling DirectoryEntry objects.
 * Reloading of expired catalogs, attaching of nested catalogs and delegating
 * of lookups to the appropriate catalog is done transparently.
 *
 * The loading / creating of catalogs is up to derived classes.
 *
 * Usage:
 *   DerivedCatalogManager *catalog_manager = new DerivedCatalogManager();
 *   catalog_manager->Init();
 *   catalog_manager->Lookup(<inode>, &<result_entry>);
 */
class AbstractCatalogManager : public SingleCopy {
 public:
  const static inode_t kInodeOffset = 255;
  AbstractCatalogManager();
  virtual ~AbstractCatalogManager();

  void SetInodeAnnotation(InodeAnnotation *new_annotation);
  virtual bool Init();
  LoadError Remount(const bool dry_run);
  void DetachNested();

  //bool LookupInode(const inode_t inode, const LookupOptions options,
  //                 DirectoryEntry *entry);
  bool LookupPath(const PathString &path, const LookupOptions options,
                  DirectoryEntry *entry);
  bool LookupPath(const std::string &path, const LookupOptions options,
                  DirectoryEntry *entry)
  {
    PathString p;
    p.Assign(&path[0], path.length());
    return LookupPath(p, options, entry);
  }
  bool Listing(const PathString &path, DirectoryEntryList *listing);
  bool Listing(const std::string &path, DirectoryEntryList *listing) {
    PathString p;
    p.Assign(&path[0], path.length());
    return Listing(p, listing);
  }
  bool ListingStat(const PathString &path, StatEntryList *listing);

  bool ListFileChunks(const PathString &path,
                      const shash::Algorithms interpret_hashes_as,
                      FileChunkList *chunks);

  void RegisterRemountListener(RemountListener *listener) {
    WriteLock();
    remount_listener_ = listener;
    Unlock();
  }
  void SetOwnerMaps(const OwnerMap &uid_map, const OwnerMap &gid_map);

  Statistics statistics() const { return statistics_; }
  uint64_t inode_gauge() {
    ReadLock(); uint64_t r = inode_gauge_; Unlock(); return r;
  }
  uint64_t GetRevision() const;
  bool GetVolatileFlag() const;
  uint64_t GetTTL() const;
  int GetNumCatalogs() const;
  std::string PrintHierarchy() const;

  /**
   * Get the inode number of the root DirectoryEntry
   * ('root' means the root of the whole file system)
   * @return the root inode number
   */
  inline inode_t GetRootInode() const {
    return inode_annotation_ ?
      inode_annotation_->Annotate(kInodeOffset + 1) : kInodeOffset + 1;
  }
  /**
   * Inodes are ambiquitous under some circumstances, to prevent problems
   * they must be passed through this method first
   * @param inode the raw inode
   * @return the revised inode
   */
  inline inode_t MangleInode(const inode_t inode) const {
    return (inode <= kInodeOffset) ? GetRootInode() : inode;
  }

 protected:
  /**
   * Load the catalog and return a file name and the catalog hash. Derived
   * class can decide if it wants to use the hash or the path.
   * Both the input as well as the output hash can be 0.
   */
  virtual LoadError LoadCatalog(const PathString &mountpoint,
                                const shash::Any &hash,
                                std::string  *catalog_path,
                                shash::Any   *catalog_hash) = 0;
  virtual void UnloadCatalog(const Catalog *catalog) { };
  virtual void ActivateCatalog(Catalog *catalog) { };

  /**
   * Create a new Catalog object.
   * Every derived class has to implement this and return a newly
   * created (derived) Catalog structure of it's desired type.
   * @param mountpoint      the future mountpoint of the catalog to create
   * @param catalog_hash    the content hash of the catalog database
   * @param parent_catalog  the parent of the catalog to create
   * @return a newly created (derived) Catalog
   */
  virtual Catalog* CreateCatalog(const PathString  &mountpoint,
                                 const shash::Any  &catalog_hash,
                                 Catalog *parent_catalog) = 0;

  Catalog *MountCatalog(const PathString &mountpoint, const shash::Any &hash,
                        Catalog *parent_catalog);
  bool MountSubtree(const PathString &path, const Catalog *entry_point,
                    Catalog **leaf_catalog);

  bool AttachCatalog(const std::string &db_path, Catalog *new_catalog);
  void DetachCatalog(Catalog *catalog);
  void DetachSubtree(Catalog *catalog);
  void DetachAll() { if (!catalogs_.empty()) DetachSubtree(GetRootCatalog()); }
  bool IsAttached(const PathString &root_path,
                  Catalog **attached_catalog) const;

  inline Catalog* GetRootCatalog() const { return catalogs_.front(); }
  Catalog *FindCatalog(const PathString &path) const;

  inline void ReadLock() const {
    int retval = pthread_rwlock_rdlock(rwlock_);
    assert(retval == 0);
  }
  inline void WriteLock() const {
    int retval = pthread_rwlock_wrlock(rwlock_);
    assert(retval == 0);
  }
  inline void Unlock() const {
    int retval = pthread_rwlock_unlock(rwlock_);
    assert(retval == 0);
  }
  virtual void EnforceSqliteMemLimit();

 private:
  void CheckInodeWatermark();

  /**
   * This list is only needed to find a catalog given an inode.
   * This might possibly be done by walking the catalog tree, similar to
   * finding a catalog given the path.
   */
  CatalogList catalogs_;
  int inode_watermark_status_;  /**< 0: OK, 1: > 32bit */
  uint64_t inode_gauge_;  /**< highest issued inode */
  uint64_t revision_cache_;
  uint64_t incarnation_;  /**< counts how often the inodes have been invalidated */
  InodeAnnotation *inode_annotation_;  /**< applied to all catalogs */
  pthread_rwlock_t *rwlock_;
  Statistics statistics_;
  pthread_key_t pkey_sqlitemem_;
  RemountListener *remount_listener_;
  OwnerMap uid_map_;
  OwnerMap gid_map_;

  //Catalog *Inode2Catalog(const inode_t inode);
  std::string PrintHierarchyRecursively(const Catalog *catalog,
                                        const int level) const;

  InodeRange AcquireInodes(uint64_t size);
  void ReleaseInodes(const InodeRange chunk);
};  // class CatalogManager

}  // namespace catalog

#endif  // CVMFS_CATALOG_MGR_H_
