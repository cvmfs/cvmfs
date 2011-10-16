#ifndef CATALOG_CLASS_H
#define CATALOG_CLASS_H 1

#include <pthread.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "catalog_queries.h"
#include "directory_entry.h"
#include "hash.h"

extern "C" {
   #include "sqlite3-duplex.h"
}

namespace cvmfs {

class Catalog;
typedef std::vector<Catalog*> CatalogVector;

class Catalog {
 public:
  Catalog(const std::string &path, Catalog *parent);
  ~Catalog();
  bool Init(const std::string &db_file, const uint64_t inode_offset);
  
 public:
  inline bool IsRoot() const { return NULL == parent_; }
  
  bool Lookup(const inode_t inode, DirectoryEntry *entry) const;
  bool Lookup(const hash::t_md5 &path_hash, DirectoryEntry *entry) const;
  inline bool Lookup(const std::string &path, DirectoryEntry *entry) const { return Lookup(hash::t_md5(path), entry); }
  
  bool Listing(const inode_t inode, DirectoryEntryList *listing) const;
  bool Listing(const hash::t_md5 &path_hash, DirectoryEntryList *listing) const;
  inline bool Listing(const std::string &path, DirectoryEntryList *listing) const { return Listing(hash::t_md5(path), listing); }
  
  inline void addChild(Catalog *child) { children_.push_back(child); }
  
  inline bool ContainsInode(const inode_t inode) const { return (inode > inode_offset_ && inode <= maximal_row_id_ + inode_offset_); }
  inline CatalogVector GetChildren() const { return children_; }
  inline std::string GetPath() const { return path_; }
  
  inode_t GetInodeFromRowIdAndHardlinkGroupId(uint64_t row_id, uint64_t hardlink_group_id) const;
  
 private:
  inline void Lock() const { pthread_mutex_lock((pthread_mutex_t *)&mutex_); }
  inline void Unlock() const { pthread_mutex_unlock((pthread_mutex_t *)&mutex_); }
  
  inline uint64_t GetRowIdFromInode(const inode_t inode) const { return inode - inode_offset_; }
  
  bool EnsureConsistencyOfDirectoryEntry(const hash::t_md5 &path_hash, DirectoryEntry *entry) const;
  
 private:
  static const uint64_t DEFAULT_TTL; ///< Default TTL for a catalog is one hour.
  static const uint64_t GROW_EPOCH;
  static const int      SQLITE_THREAD_MEM; ///< SQLite3 heap limit per thread

 private:
  sqlite3 *database_; ///< The SQLite3 database handle for this catalog

  pthread_mutex_t mutex_;
  std::string root_prefix_; ///< If we mount deep into a nested catalog, we need the full preceeding path to calculate the correct MD5 hash
  std::string path_;
  
  Catalog *parent_;
  CatalogVector children_;
  
  uint64_t inode_offset_;
  uint64_t maximal_row_id_;
  
  ListingLookupSqlStatement *listing_statement_;
  PathHashLookupSqlStatement *path_hash_lookup_statement_;
  InodeLookupSqlStatement *inode_lookup_statement_;
  FindNestedCatalogSqlStatement *find_nested_catalog_statement_;
};

}

#endif /* CATALOG_CLASS_H */
