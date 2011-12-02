/**
 *  The AbstractCatalogManager provides all the functionality a read-only
 *  catalog has to provide. Namely you have various methods for lookups of
 *  DirectoryEntrys. Furthermore directory listing is supported.
 *  All intrinsic catalog magic is also happening here and transparent to
 *  the user of this class. Namely reloading of expired catalogs, attaching
 *  of nested catalogs and delegating of lookups to the appropriate catalog.
 *
 *  To be as extensible as possible it provides a couple of virtual methods
 *  which can be implemented by derived classes.
 *
 *  The concrete CatalogManager implementations are meant to be thread safe.
 *  Thus it is highly encouraged to use the synchronisation methods provided
 *  by the inherited class ThreadSafeReadWrite.
 *
 *  Usage:
 *    DerivedAbstractCatalogManager *catMngr = new DerivedAbstractCatalogManager();
 *    catMngr->Init();
 *    catMngr->Lookup(<inode>, &<result_entry>);
 */

#ifndef ABSTRACT_CATALOG_MANAGER_H
#define ABSTRACT_CATALOG_MANAGER_H 1

#include <vector>
#include <string>

#include "thread_safe.h"
#include "Catalog.h"
#include "DirectoryEntry.h"
#include "hash.h"

namespace cvmfs {
  
class AbstractCatalogManager : public ThreadSafeReadWrite {
 public:
  AbstractCatalogManager();
  virtual ~AbstractCatalogManager();
  
  /**
   *  Initializes the CatalogManager
   *  i.e. loads and attaches the root entry
   *  @return true on successful init otherwise false
   */
  virtual bool Init();
  
  /**
   *  convenience wrapper around the Lookup methods, to specifically set the
   *  lookup_without_parent flag in the call of Lookup.
   *  This means, the CatalogManager does not perform a second lookup in the
   *  catalogs to find about the inode of the result's parent. This is useful
   *  if that information is simply not needed or can be obtained differently.
   *  @param inode the inode to find in the catalogs
   *  @param entry the resulting DirectoryEntry
   *  @return true if lookup succeeded otherwise false
   */
  inline bool LookupWithoutParent(const inode_t inode, DirectoryEntry *entry) const { return Lookup(inode, entry, false); };

  /**
   *  convenience wrapper around the Lookup methods, to specifically set the
   *  lookup_without_parent flag in the call of Lookup.
   *  This means, the CatalogManager does not perform a second lookup in the
   *  catalogs to find about the inode of the result's parent. This is useful
   *  if that information is simply not needed or can be obtained differently.
   *  @param path the path to find in the catalogs
   *  @param entry the resulting DirectoryEntry
   *  @return true if lookup succeeded otherwise false
   */
  inline bool LookupWithoutParent(const std::string &path, DirectoryEntry *entry) { return Lookup(path, entry, false); };
  
  /**
   *  perform a lookup for a specific DirectoryEntry in the catalogs
   *  @param inode the inode to find in the catalogs
   *  @param entry the resulting DirectoryEntry
   *  @param with_parent perform a second lookup to get information about the parent
   *  @return true if lookup succeeded otherwise false
   */
  bool Lookup(const inode_t inode, DirectoryEntry *entry, const bool with_parent = true) const;
  
  /**
   *  perform a lookup for a specific DirectoryEntry in the catalogs
   *  @param path the path to find in the catalogs
   *  @param entry the resulting DirectoryEntry
   *  @param with_parent perform a second lookup to get information about the parent
   *  @return true if lookup succeeded otherwise false
   */
  bool Lookup(const std::string &path, DirectoryEntry *entry, const bool with_parent = true);
  
  /**
   *  do a listing of the specified directory
   *  @param path the path of the directory to list
   *  @param listing the resulting DirectoryEntryList
   *  @return true if listing succeeded otherwise false
   */
  bool Listing(const std::string &path, DirectoryEntryList *listing);
  
  /**
   *  get the inode number of the root DirectoryEntry
   *  'root' here means the actual root of the whole file system
   *  @return the root inode number
   */
  inline inode_t GetRootInode() const { return kInitialInodeOffset + 1; }
  
  /**
   *  get the revision of the catalog
   *  TODO: CURRENTLY NOT IMPLEMENTED
   *  @return the revision number of the catalog
   */
  inline uint64_t GetRevision() const { return 0; } // TODO: implement this
  
  /**
   *  count all attached catalogs
   *  @return the number of all attached catalogs
   */
  inline int GetNumberOfAttachedCatalogs() const { return catalogs_.size(); }
  
  /**
   *  Inodes are ambiquitous under some circumstances, to prevent problems
   *  they must be passed through this method first
   *  @param inode the raw inode
   *  @return the revised inode
   */
  inline inode_t MangleInode(const inode_t inode) const { return (inode < kInitialInodeOffset) ? GetRootInode() : inode; }
  
 protected:
  
  /**
   *  This pure virtual method has to be implemented by deriving classes
   *  It should perform a specific loading action, return 0 on success and
   *  communicate a path to the readily loaded catalog file for attachment.
   *  @param url_path the url path of the catalog to load
   *  @param mount_point the md5 hash of the mount point for this catalog
   *  @param catalog_file must be set to the path of the loaded file
   *  @return 0 on success otherwise a application specific error code
   */
  virtual int LoadCatalogFile(const std::string &url_path, const hash::t_md5 &mount_point, 
                              std::string *catalog_file) = 0;
                              
  /**
   *  Pure virtual method to create a new catalog structure
   *  Under different circumstances we might need different types of catalog
   *  structures. Every derived class has to implement this and return a newly
   *  created (derived) Catalog structure of it's desired type.
   *  @param mountpoint the future mountpoint of the catalog to create
   *  @param parent_catalog the parent of the catalog to create
   *  @return a newly created (derived) Catalog for future usage
   */
  virtual Catalog* CreateCatalogStub(const std::string &mountpoint, Catalog *parent_catalog) const = 0;

  /**
   *  loads a new catalog and attaches it on this CatalogManager
   *  for loading of the catalog the pure virtual method LoadCatalogFile is used.
   *  @param mountpoint the mount point path of the catalog to load/attach
   *  @param parent_catalog the direct parent of the catalog to load
   *  @param attached_catalog will be set to the newly attached catalog
   *  @return true on success otherwise false
   */
  bool LoadAndAttachCatalog(const std::string &mountpoint, Catalog *parent_catalog, Catalog **attached_catalog = NULL);
  
  /**
   *  attaches a newly created catalog
   *  @param db_file the file on a local file system containing the database
   *  @param new_catalog the catalog to attach to this CatalogManager
   *  @param open_transaction ????
   *  @return true on success, false otherwise
   */
  bool AttachCatalog(const std::string &db_file, Catalog *new_catalog, const bool open_transaction);
  
  /**
   *  removes a catalog (and all of it's children) from this CatalogManager
   *  @param catalog the catalog to detach
   *  @return true on success, false otherwise
   */
  bool DetachCatalog(Catalog *catalog);
  
  /**
   *  detach all catalogs from this CatalogManager
   *  this is mainly called in the destructor of this class
   *  @return true on success, false otherwise
   */
  inline bool DetachAllCatalogs() { return DetachCatalog(GetRootCatalog()); }
  
  /**
   *  get the root catalog of this CatalogManager
   *  @return the root catalog of this CatalogMananger
   */
  inline Catalog* GetRootCatalog() const { return catalogs_.front(); }
  
  /**
   *  find the appropriate catalog for a given path.
   *  this method might load additional nested catalogs.
   *  @param path the path for which the catalog is needed
   *  @param load_final_catalog if the last part of the given path is a nested catalog 
   *                            it is loaded as well, otherwise not (i.e. directory listing)
   *  @param catalog this pointer will be set to the searched catalog
   *  @param entry if a DirectoryEntry pointer is given, it will be set to the
   *               DirectoryEntry representing the last part of the given path
   *  @return true if catalog was found, false otherwise
   */
  bool GetCatalogByPath(const std::string &path, const bool load_final_catalog, Catalog **catalog = NULL, DirectoryEntry *entry = NULL);
  
  /**
   *  finds the appropriate catalog for a given inode
   *  Note: This method will NOT load additional nested catalogs
   *  it will match the inode to a allocated inode range.
   *  @param inode the inode to find the associated catalog for
   *  @param catalog this pointer will be set to the result catalog
   *  @return true if catalog was present, false otherwise
   */
  bool GetCatalogByInode(const inode_t inode, Catalog **catalog) const;
  
  /**
   *  checks if a searched catalog is already present in this CatalogManager
   *  based on it's path.
   *  @param root_path the root path of the searched catalog
   *  @param attached_catalog is set to the searched catalog, in case of presence
   *  @return true if catalog is already present, false otherwise
   */
  bool IsCatalogAttached(const std::string &root_path, Catalog **attached_catalog) const;
  
 private:
   
  /**
   *  this method finds the most probably fitting catalog for a given path
   *  Note: this might still not be the catalog you are looking for
   *  Mainly designed for internal use.
   *  @param path the path a catalog is searched for
   *  @return the catalog which is best fitting at the given path
   */
  Catalog* FindBestFittingCatalogForPath(const std::string &path) const;
  
  /**
   *  this method loads all nested catalogs neccessary to serve a certain path
   *  @param path the path to load the associated nested catalog for
   *  @param entry_point one can specify the catalog to start the search at
   *                     (i.e. the result of FindBestFittingCatalogForPath)
   *  @param load_final_catalog if the last part of path is a nested catalog
   *                            it will be loaded as well, otherwise not
   *  @param final_catalog this will be set to the resulting catalog
   *  @return true if desired nested catalog was successfully loaded, false otherwise
   */
  bool LoadNestedCatalogForPath(const std::string &path, 
                                const Catalog *entry_point, 
                                const bool load_final_catalog, 
                                Catalog **final_catalog);
  
  /**
   *  allocate a chunk of inodes for the given size
   *  this is done while attaching a new catalog
   *  @param size the number of inodes needed
   *  @return a structure defining a chunk of inodes to use for this catalog
   */
  InodeChunk GetInodeChunkOfSize(uint64_t size);
  
  /**
   *  this method is called if a catalog is detached
   *  which renders the associated InodeChunk invalid
   *  here you can clean caches or do some other fancy stuff
   *  @param chunk the InodeChunk to be freed
   */
  void AnnounceInvalidInodeChunk(const InodeChunk chunk) const;
  
 private:
  // TODO: this list is actually not really needed.
  //       the only point we are really using it at the moment
  //       is for searching the suited catalog for a given inode.
  //       this might be done better (currently O(n))
  //  eventually we should only safe the root catalog, representing
  //  the whole catalog tree in an implicit manor.
  CatalogList catalogs_;
  
  uint64_t current_inode_offset_;
  
  const static inode_t kInitialInodeOffset = 255;
};
  
}

#endif /* ABSTRACT_CATALOG_MANAGER_H */
