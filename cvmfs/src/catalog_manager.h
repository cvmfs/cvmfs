#ifndef CATALOG_MANAGER_H
#define CATALOG_MANAGER_H 1

#include <vector>
#include <string>

#include "catalog_class.h"
#include "hash.h"
#include "atomic.h"
#include "directory_entry.h"

namespace cvmfs {
  
class CatalogManager {
 public:
  CatalogManager(const std::string &root_url, const std::string &repo_name, const std::string &whitelist, const std::string &blacklist, const bool force_signing);
  virtual ~CatalogManager();
  
  bool Init();
  
  inline bool LookupWithoutParent(const inode_t inode, DirectoryEntry *entry) const { return Lookup(inode, entry, false); };
  inline bool LookupWithoutParent(const std::string &path, DirectoryEntry *entry) { return Lookup(path, entry, false); };
  bool Lookup(const inode_t inode, DirectoryEntry *entry, const bool with_parent = true) const;
  bool Lookup(const std::string &path, DirectoryEntry *entry, const bool with_parent = true);
  
  bool Listing(const std::string &path, DirectoryEntryList *listing);
  
  inline inode_t GetRootInode() const { return kInitialInodeOffset + 1; }
  inline uint64_t GetRevision() const { return 0; } // TODO: implement this
  inline int GetNumberOfAttachedCatalogs() const { return catalogs_.size(); }
  
  inline inode_t MangleInode(const inode_t inode) const { return (inode < kInitialInodeOffset) ? GetRootInode() : inode; }
  
 private:
  typedef enum {
     ppNotPresent,
     ppPresent,
     ppNotAttachedNestedCatalog
  } PathPresent;
  
  int FetchCatalog(const std::string &url_path, const bool no_proxy, const hash::t_md5 &mount_point,
                   std::string &cat_file, hash::t_sha1 &cat_sha1, std::string &old_file, hash::t_sha1 &old_sha1, 
                   bool &cached_copy, const hash::t_sha1 &sha1_expected, const bool dry_run = false);
                   
  /** convenience wrapper to hide all the nasty loading stuff which I do not understand */
  inline int LoadCatalogFile(const std::string &url_path, const hash::t_md5 &mount_point, 
                             std::string *catalog_file)
  {
    return LoadCatalogFile(url_path, mount_point, -1, false, hash::t_sha1(), catalog_file);
  }
  int LoadCatalogFile(const std::string &url_path, const hash::t_md5 &mount_point, 
                      const int existing_cat_id, const bool no_cache,
                      const hash::t_sha1 expected_clg, std::string *catalog_file);

  bool LoadAndAttachRootCatalog();
  bool LoadAndAttachCatalog(const std::string &mountpoint, Catalog *parent_catalog, Catalog **attached_catalog = NULL);
  bool AttachCatalog(const std::string &db_file, const std::string &url, Catalog *parent, const bool open_transaction, Catalog **attached_catalog);
  bool RefreshCatalog();
  bool DetachCatalog();
  bool DetachAllCatalogs();
  
  uint64_t GetInodeChunkOfSize(uint64_t size);

  bool IsValidCertificate(bool nocache);
  std::string MakeFilesystemKey(std::string url) const;
  
  inline Catalog* GetRootCatalog() const { return catalogs_[0]; }
  bool GetCatalogByPath(const std::string &path, const bool load_final_catalog, Catalog **catalog = NULL, DirectoryEntry *entry = NULL);
  bool GetCatalogByInode(const inode_t inode, Catalog **catalog) const;
  
  Catalog* FindBestFittingCatalogForPath(const std::string &path) const;
  bool LoadNestedCatalogForPath(const std::string &path, const Catalog *entry_point, const bool load_final_catalog, Catalog **final_catalog);
  
  inline void ReadLock() const { pthread_rwlock_rdlock((pthread_rwlock_t*)&read_write_lock_); }
  inline void WriteLock() { pthread_rwlock_wrlock(&read_write_lock_); }
  inline void ExpandLock() { Unlock(); WriteLock(); }
  inline void ShrinkLock() { Unlock(); ReadLock(); }
  inline void Unlock() const { pthread_rwlock_unlock((pthread_rwlock_t*)&read_write_lock_); }
  
 private:
  CatalogVector catalogs_;
  
  pthread_rwlock_t read_write_lock_;
  
  std::string root_url_;
  std::string repo_name_;
  std::string whitelist_;
  std::string blacklist_;
  bool force_signing_;
  
  atomic_int certificate_hits_;
  atomic_int certificate_misses_;
  
  uint64_t current_inode_offset_;
  
  const static inode_t kInitialInodeOffset = 255;
};
  
}

#endif /* CATALOG_MANAGER_H */
