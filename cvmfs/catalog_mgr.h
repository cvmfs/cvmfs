/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CATALOG_MGR_H_
#define CVMFS_CATALOG_MGR_H_

#include <vector>
#include <string>

#include "catalog.h"
#include "dirent.h"
#include "hash.h"

namespace catalog {

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
  AbstractCatalogManager();
  virtual ~AbstractCatalogManager();

  virtual bool Init();

  bool LookupInode(const inode_t inode, const LookupOptions options,
                   DirectoryEntry *entry) const;
  bool LookupPath(const std::string &path, const LookupOptions options,
                  DirectoryEntry *entry);
  bool Listing(const std::string &path, DirectoryEntryList *listing);

  uint64_t GetRevision() const;
  int GetNumCatalogs() const;
  std::string PrintHierarchy() const;

  /**
   * Get the inode number of the root DirectoryEntry
   * ('root' means the root of the whole file system)
   * @return the root inode number
   */
  inline inode_t GetRootInode() const { return kInodeOffset + 1; }
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
  virtual LoadError LoadCatalog(const std::string &mountpoint,
                                const hash::Any &hash,
                                std::string *catalog_path) = 0;

  /**
   * Create a new Catalog object.
   * Every derived class has to implement this and return a newly
   * created (derived) Catalog structure of it's desired type.
   * @param mountpoint the future mountpoint of the catalog to create
   * @param parent_catalog the parent of the catalog to create
   * @return a newly created (derived) Catalog
   */
  virtual Catalog* CreateCatalog(const std::string &mountpoint,
                                 Catalog *parent_catalog) const = 0;

  Catalog *MountCatalog(const std::string &mountpoint, const hash::Any &hash,
                        Catalog *parent_catalog);
  bool MountSubtree(const std::string &path, const Catalog *entry_point,
                    Catalog **leaf_catalog);
  inline bool MountAll() { return MountRecursively(GetRootCatalog()); }

  bool AttachCatalog(const std::string &db_path, Catalog *new_catalog);
  void DetachCatalog(Catalog *catalog);
  void DetachSubtree(Catalog *catalog);
  inline void DetachAll() { DetachSubtree(GetRootCatalog()); }
  bool IsAttached(const std::string &root_path,
                  Catalog **attached_catalog) const;

  inline Catalog* GetRootCatalog() const { return catalogs_.front(); }
  Catalog *FindCatalog(const std::string &path) const;

 private:
  const static inode_t kInodeOffset = 255;
  /**
   * This list is only needed to find a catalog given an inode.
   * This might possibly be done by walking the catalog tree, similar to
   * finding a catalog given the path.
   */
  CatalogList catalogs_;
  uint64_t inode_gauge_;  /**< highest issued inode */

  // TODO: remove these stubs and replace them with actual locking
  inline void ReadLock() const { }
  inline void WriteLock() const { }
  inline void Unlock() const { }
  inline void UpgradeLock() const { Unlock(); WriteLock(); }
  inline void DowngradeLock() const { Unlock(); ReadLock(); }

  std::string PrintHierarchyRecursively(const Catalog *catalog,
                                        const int level) const;

  InodeRange AcquireInodes(uint64_t size);
  void ReleaseInodes(const InodeRange chunk);

  bool MountRecursively(Catalog *catalog);
};  // class CatalogManager

}  // namespace catalog

#endif  // CVMFS_CATALOG_MGR_H_
