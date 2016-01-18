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
#include <map>
#include <string>
#include <vector>

#include "atomic.h"
#include "catalog.h"
#include "directory_entry.h"
#include "file_chunk.h"
#include "hash.h"
#include "logging.h"
#include "statistics.h"
#include "util.h"

class XattrList;

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

  kLoadNumEntries
};

inline const char *Code2Ascii(const LoadError error) {
  const char *texts[kLoadNumEntries + 1];
  texts[0] = "loaded new catalog";
  texts[1] = "catalog was up to date";
  texts[2] = "not enough space to load catalog";
  texts[3] = "failed to load catalog";
  texts[4] = "no text";
  return texts[error];
}


struct Statistics {
  perf::Counter *n_lookup_inode;
  perf::Counter *n_lookup_path;
  perf::Counter *n_lookup_path_negative;
  perf::Counter *n_lookup_xattrs;
  perf::Counter *n_listing;
  perf::Counter *n_nested_listing;

  explicit Statistics(perf::Statistics *statistics) {
    n_lookup_inode = statistics->Register("catalog_mgr.n_lookup_inode",
        "Number of inode lookups");
    n_lookup_path = statistics->Register("catalog_mgr.n_lookup_path",
        "Number of path lookups");
    n_lookup_path_negative = statistics->Register(
        "catalog_mgr.n_lookup_path_negative",
        "Number of negative path lookups");
    n_lookup_xattrs = statistics->Register("catalog_mgr.n_lookup_xattrs",
        "Number of xattrs lookups");
    n_listing = statistics->Register("catalog_mgr.n_listing",
        "Number of listings");
    n_nested_listing = statistics->Register("catalog_mgr.n_nested_listing",
        "Number of listings of nested catalogs");
  }
};


class InodeGenerationAnnotation : public InodeAnnotation {
 public:
  InodeGenerationAnnotation() { inode_offset_ = 0; }
  ~InodeGenerationAnnotation() { }
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
  inode_t GetGeneration() { return inode_offset_; }

 private:
  uint64_t inode_offset_;
};

template <class CatalogT>
class AbstractCatalogManager;


/**
 * This class provides the read-only interface to a tree of catalogs
 * representing a (subtree of a) repository.
 * Mostly lookup functions filling DirectoryEntry objects.
 * Reloading of expired catalogs, attaching of nested catalogs and delegating
 * of lookups to the appropriate catalog is done transparently.
 *
 * The loading / creating of catalogs is up to derived classes.
 *
 * CatalogT is either Catalog or MockCatalog.
 *
 * Usage:
 *   DerivedCatalogManager *catalog_manager = new DerivedCatalogManager();
 *   catalog_manager->Init();
 *   catalog_manager->Lookup(<inode>, &<result_entry>);
 */
template <class CatalogT>
class AbstractCatalogManager : public SingleCopy {
 public:
  typedef std::vector<CatalogT*> CatalogList;
  typedef CatalogT catalog_t;

  static const inode_t kInodeOffset = 255;
  explicit AbstractCatalogManager(perf::Statistics *statistics);
  virtual ~AbstractCatalogManager();

  void SetInodeAnnotation(InodeAnnotation *new_annotation);
  virtual bool Init();
  LoadError Remount(const bool dry_run);
  void DetachNested();

  //  Not needed anymore since there are the glue buffers
  //  bool LookupInode(const inode_t inode, const LookupOptions options,
  //                   DirectoryEntry *entry);
  bool LookupPath(const PathString &path, const LookupOptions options,
                  DirectoryEntry *entry);
  bool LookupPath(const std::string &path, const LookupOptions options,
                  DirectoryEntry *entry)
  {
    PathString p;
    p.Assign(&path[0], path.length());
    return LookupPath(p, options, entry);
  }
  bool LookupXattrs(const PathString &path, XattrList *xattrs);

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
  void SetOwnerMaps(const OwnerMap &uid_map, const OwnerMap &gid_map);

  Statistics statistics() const { return statistics_; }
  uint64_t inode_gauge() {
    ReadLock(); uint64_t r = inode_gauge_; Unlock(); return r;
  }
  uint64_t GetRevision() const;
  bool GetVolatileFlag() const;
  uint64_t GetTTL() const;
  bool GetVOMSAuthz(std::string *authz) const;
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
  virtual void UnloadCatalog(const CatalogT *catalog) { }
  virtual void ActivateCatalog(CatalogT *catalog) { }
  const std::vector<CatalogT*>& GetCatalogs() const { return catalogs_; }

  /**
   * Create a new Catalog object.
   * Every derived class has to implement this and return a newly
   * created (derived) Catalog structure of it's desired type.
   * @param mountpoint      the future mountpoint of the catalog to create
   * @param catalog_hash    the content hash of the catalog database
   * @param parent_catalog  the parent of the catalog to create
   * @return a newly created (derived) Catalog
   */
  virtual CatalogT* CreateCatalog(const PathString  &mountpoint,
                                  const shash::Any  &catalog_hash,
                                  CatalogT *parent_catalog) = 0;

  CatalogT *MountCatalog(const PathString &mountpoint, const shash::Any &hash,
                         CatalogT *parent_catalog);
  bool MountSubtree(const PathString &path, const CatalogT *entry_point,
                    CatalogT **leaf_catalog);

  bool AttachCatalog(const std::string &db_path, CatalogT *new_catalog);
  void DetachCatalog(CatalogT *catalog);
  void DetachSubtree(CatalogT *catalog);
  void DetachAll() { if (!catalogs_.empty()) DetachSubtree(GetRootCatalog()); }
  bool IsAttached(const PathString &root_path,
                  CatalogT **attached_catalog) const;

  inline CatalogT* GetRootCatalog() const { return catalogs_.front(); }
  CatalogT *FindCatalog(const PathString &path) const;

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
  /**
   * Saves the result of GetVOMSAuthz when a root catalog is attached
   */
  bool has_authz_cache_;
  /**
   * Saves the VOMS requirements when a root catalog is attached
   */
  std::string authz_cache_;
  /**
   * Counts how often the inodes have been invalidated.
   */
  uint64_t incarnation_;
  // TODO(molina) we could just add an atomic global counter instead
  InodeAnnotation *inode_annotation_;  /**< applied to all catalogs */
  pthread_rwlock_t *rwlock_;
  Statistics statistics_;
  pthread_key_t pkey_sqlitemem_;
  OwnerMap uid_map_;
  OwnerMap gid_map_;

  // Not needed anymore since there are the glue buffers
  // Catalog *Inode2Catalog(const inode_t inode);
  std::string PrintHierarchyRecursively(const CatalogT *catalog,
                                        const int level) const;

  InodeRange AcquireInodes(uint64_t size);
  void ReleaseInodes(const InodeRange chunk);
};  // class CatalogManager

}  // namespace catalog

#include "catalog_mgr_impl.h"

#endif  // CVMFS_CATALOG_MGR_H_
