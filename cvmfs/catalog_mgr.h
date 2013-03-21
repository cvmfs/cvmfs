/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_MGR_H_
#define CVMFS_CATALOG_MGR_H_

#include <pthread.h>
#include <cassert>

#include <vector>
#include <string>

#include "catalog.h"
#include "dirent.h"
#include "hash.h"
#include "atomic.h"
#include "util.h"

namespace catalog {

const unsigned kSqliteMemPerThread = 1*1024*1024;

/**
 * Lookup a directory entry including its parent entry or not.
 */
enum LookupOptions {
  kLookupSole = 0,
  kLookupFull,
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
  InodeGenerationAnnotation(const unsigned inode_width);
  ~InodeGenerationAnnotation() { };
  bool ValidInode(const uint64_t inode) {
    return (inode & generation_annotation_) == generation_annotation_;
  }
  inode_t Annotate(const inode_t raw_inode) {
    return raw_inode | generation_annotation_;
  }
  void SetGeneration(const uint64_t new_generation);
  void CheckForOverflow(const uint64_t new_generation, 
                        const uint64_t initial_generation,
                        uint32_t *overflow_counter);

 private:
  unsigned inode_width_;
  uint64_t generation_annotation_;
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
class AbstractCatalogManager {
 public:
  const static inode_t kInodeOffset = 255;
  AbstractCatalogManager();
  virtual ~AbstractCatalogManager();

  void SetInodeAnnotation(InodeAnnotation *new_annotation);
  virtual bool Init();
  LoadError Remount(const bool dry_run);
  void DetachAll() { DetachSubtree(GetRootCatalog()); }

  bool LookupInode(const inode_t inode, const LookupOptions options,
                   DirectoryEntry *entry);
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

  /**
   * Don't use.  Only for the CwdBuffer.
   */
  bool Path2InodeUnprotected(const PathString &path, inode_t *inode);
  
  void SetIncarnation(const uint64_t new_incarnation);
  void RegisterRemountListener(RemountListener *listener) {
    WriteLock();
    remount_listener_ = listener;
    Unlock();
  }
  
  Statistics statistics() const { return statistics_; }
  uint64_t GetRevision() const;
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
   * Load the catalog and return a file name.  Derived class can decide if it
   * wants to use the hash or the path.  The hash can be 0.
   */
  virtual LoadError LoadCatalog(const PathString &mountpoint,
                                const hash::Any &hash,
                                std::string *catalog_path) = 0;
  virtual void UnloadCatalog(const Catalog *catalog) { };
  virtual void ActivateCatalog(const Catalog *catalog) { };

  /**
   * Create a new Catalog object.
   * Every derived class has to implement this and return a newly
   * created (derived) Catalog structure of it's desired type.
   * @param mountpoint the future mountpoint of the catalog to create
   * @param parent_catalog the parent of the catalog to create
   * @return a newly created (derived) Catalog
   */
  virtual Catalog* CreateCatalog(const PathString &mountpoint,
                                 Catalog *parent_catalog) = 0;

  Catalog *MountCatalog(const PathString &mountpoint, const hash::Any &hash,
                        Catalog *parent_catalog);
  bool MountSubtree(const PathString &path, const Catalog *entry_point,
                    Catalog **leaf_catalog);

  bool AttachCatalog(const std::string &db_path, Catalog *new_catalog);
  void DetachCatalog(Catalog *catalog);
  void DetachSubtree(Catalog *catalog);
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
  /**
   * This list is only needed to find a catalog given an inode.
   * This might possibly be done by walking the catalog tree, similar to
   * finding a catalog given the path.
   */
  CatalogList catalogs_;
  uint64_t inode_gauge_;  /**< highest issued inode */
  uint64_t revision_cache_;
  uint64_t incarnation_;  /**< counts how often the inodes have been invalidated */
  InodeAnnotation *inode_annotation_;  /**< applied to all catalogs */
  pthread_rwlock_t *rwlock_;
  Statistics statistics_;
  pthread_key_t pkey_sqlitemem_;
  RemountListener *remount_listener_;

  std::string PrintHierarchyRecursively(const Catalog *catalog,
                                        const int level) const;

  InodeRange AcquireInodes(uint64_t size);
  void ReleaseInodes(const InodeRange chunk);
};  // class CatalogManager

}  // namespace catalog

#endif  // CVMFS_CATALOG_MGR_H_
