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
  inline inode_t GetRootInode() const { return kInitialInodeOffset + 1; }
  /**
   * Inodes are ambiquitous under some circumstances, to prevent problems
   * they must be passed through this method first
   * @param inode the raw inode
   * @return the revised inode
   */
  inline inode_t MangleInode(const inode_t inode) const {
    return (inode < kInitialInodeOffset) ? GetRootInode() : inode;
  }

 protected:


  virtual LoadError LoadCatalog(const std::string &mountpoint,
                                const hash::Any &hash,
                                std::string *catalog_path) = 0;

  /**
   *  Pure virtual method to create a new catalog structure
   *  Under different circumstances we might need different types of catalog
   *  structures. Every derived class has to implement this and return a newly
   *  created (derived) Catalog structure of it's desired type.
   *  @param mountpoint the future mountpoint of the catalog to create
   *  @param parent_catalog the parent of the catalog to create
   *  @return a newly created (derived) Catalog for future usage
   */
  virtual Catalog* CreateCatalog(const std::string &mountpoint,
                                 Catalog *parent_catalog) const = 0;

  Catalog *MountCatalog(const std::string &mountpoint, const hash::Any &hash,
                        Catalog *parent_catalog);


  /**
   *  Attaches all catalogs of the repository recursively
   *  This is useful when updating a repository on the server.
   *  Be careful when using it on remote catalogs
   *  @return true on success, false otherwise
   */
  inline bool LoadAndAttachCatalogsRecursively() { return LoadAndAttachCatalogsRecursively(GetRootCatalog()); }

  /**
   *  attaches a newly created catalog
   *  @param db_file the file on a local file system containing the database
   *  @param new_catalog the catalog to attach to this CatalogManager
   *  @param open_transaction ????
   *  @return true on success, false otherwise
   */
  bool AttachCatalog(const std::string &db_file,
                     Catalog *new_catalog,
                     const bool open_transaction);

  /**
   *  removes a catalog (and all of it's children) from this CatalogManager
   *  the given catalog and all children are freed, if this call succeeds!
   *  @param catalog the catalog to detach
   *  @return true on success, false otherwise
   */
  bool DetachCatalogTree(Catalog *catalog);

   /**
    *  removes a catalog from this CatalogManager
    *  the given catalog pointer is freed if the call succeeds!
    *  CAUTION: This method can create dangling children.
    *           use DetachCatalogTree() if you are unsure!
    *  @param catalog the catalog to detach
    *  @return true on success, false otherwise
    */
   bool DetachCatalog(Catalog *catalog);

  /**
   *  detach all catalogs from this CatalogManager
   *  this is mainly called in the destructor of this class
   *  @return true on success, false otherwise
   */
  inline bool DetachAllCatalogs() { return DetachCatalogTree(GetRootCatalog()); }

  inline Catalog* GetRootCatalog() const { return catalogs_.front(); }

  /**
   *  checks if a searched catalog is already present in this CatalogManager
   *  based on it's path.
   *  @param root_path the root path of the searched catalog
   *  @param attached_catalog is set to the searched catalog, in case of presence
   *  @return true if catalog is already present, false otherwise
   */
  bool IsCatalogAttached(const std::string &root_path,
                         Catalog **attached_catalog) const;

  Catalog *FindCatalog(const std::string &path) const;

  bool MountSubtree(const std::string &path, const Catalog *entry_point,
                    Catalog **leaf_catalog);
 private:
  const static inode_t kInitialInodeOffset = 255;
  /**
   * This list is only needed to find a catalog given an inode.
   * This might possibly be done by walking the catalog tree, similar to
   * finding a catalog given the path.
   */
  CatalogList catalogs_;
  uint64_t current_inode_offset_;

  // TODO: remove these stubs and replace them with actual locking
  inline void ReadLock() const { }
  inline void WriteLock() const { }
  inline void Unlock() const { }
  inline void UpgradeLock() const { Unlock(); WriteLock(); }
  inline void DowngradeLock() const { Unlock(); ReadLock(); }

  std::string PrintHierarchyRecursively(const Catalog *catalog,
                                        const int level) const;

  /**
   *  allocate a chunk of inodes for the given size
   *  this is done while attaching a new catalog
   *  @param size the number of inodes needed
   *  @return a structure defining a chunk of inodes to use for this catalog
   */
  InodeRange GetInodeChunkOfSize(uint64_t size);

  /**
   *  this method is called if a catalog is detached
   *  which renders the associated InodeChunk invalid
   *  here you can clean caches or do some other fancy stuff
   *  @param chunk the InodeChunk to be freed
   */
  void AnnounceInvalidInodeChunk(const InodeRange chunk) const;

  /**
   *  Attaches all catalogs of the repository recursively
   *  This is useful when updating a repository on the server.
   *  Be careful when using it on remote catalogs.
   *  (This is the actual recursion, there is a convenience wrapper in the
   *   protected part of this class)
   *  @param catalog the catalog whose children are attached in this recursion step
   *  @return true on success, false otherwise
   */
  bool LoadAndAttachCatalogsRecursively(Catalog *catalog);
};  // class CatalogManager

}  // namespace catalog

#endif  // CVMFS_CATALOG_MGR_H_
