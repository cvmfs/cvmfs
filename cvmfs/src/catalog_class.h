#ifndef CATALOG_CLASS_H
#define CATALOG_CLASS_H 1

#include <stdint.h>
#include <string>
#include <list>
#include <map>
#include <assert.h>

#include "catalog_queries.h"
#include "directory_entry.h"
#include "hash.h"
#include "thread_safe.h"

extern "C" {
   #include "sqlite3-duplex.h"
}

namespace cvmfs {

class Catalog;
typedef std::list<Catalog*> CatalogList;

class CatalogManager;

struct InodeChunk {
  uint64_t offset;
  uint64_t size;
  
  InodeChunk() {
    offset = 0;
    size = 0;
  }
  
  inline bool ContainsInode(const inode_t inode) const {
    return ((uint64_t)inode > offset && inode <= size + offset);
  }
  
  inline bool IsInitialized() const { return offset > 0 && size > 0; }
};

class Catalog : public ThreadSafeMutex {
 public:
  Catalog(const std::string &path, Catalog *parent);
  virtual ~Catalog();
  bool OpenDatabase(const std::string &db_file);
  inline bool IsInitialized() const { return inode_chunk_.IsInitialized() && max_row_id_ > 0; }
  
 public:
  inline bool IsRoot() const { return NULL == parent_; }
  
  bool Lookup(const inode_t inode, DirectoryEntry *entry, hash::t_md5 *parent_hash = NULL) const;
  bool Lookup(const hash::t_md5 &path_hash, DirectoryEntry *entry) const;
  inline bool Lookup(const std::string &path, DirectoryEntry *entry) const { return Lookup(hash::t_md5(path), entry); }
  
  bool Listing(const inode_t inode, DirectoryEntryList *listing) const;
  bool Listing(const hash::t_md5 &path_hash, DirectoryEntryList *listing) const;
  inline bool Listing(const std::string &path, DirectoryEntryList *listing) const { return Listing(hash::t_md5(path), listing); }
  
  void AddChild(Catalog *child);
  void RemoveChild(const Catalog *child);
  CatalogList GetChildrenRecursively() const;
  
  inline bool ContainsInode(const inode_t inode) const { assert(IsInitialized()); return inode_chunk_.ContainsInode(inode); }
  
  inline CatalogList children() const { return children_; }
  inline std::string path() const { return path_; }
  inline Catalog* parent() const { return parent_; }
  inline uint64_t max_row_id() const { return max_row_id_; }
  inline InodeChunk inode_chunk() const { return inode_chunk_; }
  inline void set_inode_chunk(const InodeChunk chunk) { inode_chunk_ = chunk; }
  
  inode_t GetInodeFromRowIdAndHardlinkGroupId(uint64_t row_id, uint64_t hardlink_group_id);
  
 private:
  inline uint64_t GetRowIdFromInode(const inode_t inode) const { return inode - inode_chunk_.offset; }
  
  bool EnsureCoherenceOfInodes(const hash::t_md5 &path_hash, DirectoryEntry *entry) const;
  
  void InitPreparedStatements();
  void FinalizePreparedStatements();
  
 private:
  static const uint64_t DEFAULT_TTL; ///< Default TTL for a catalog is one hour.
  static const uint64_t GROW_EPOCH;
  static const int      SQLITE_THREAD_MEM; ///< SQLite3 heap limit per thread
  
  typedef std::map<int, inode_t> HardlinkGroupIdMap;

 private:
  sqlite3 *database_; ///< The SQLite3 database handle for this catalog

  std::string root_prefix_; ///< If we mount deep into a nested catalog, we need the full preceeding path to calculate the correct MD5 hash
  std::string path_;
  
  Catalog *parent_;
  CatalogList children_;
  
  InodeChunk inode_chunk_;
  uint64_t max_row_id_;
  
  HardlinkGroupIdMap hardlink_groups_;
  
  ListingLookupSqlStatement *listing_statement_;
  PathHashLookupSqlStatement *path_hash_lookup_statement_;
  InodeLookupSqlStatement *inode_lookup_statement_;
  FindNestedCatalogSqlStatement *find_nested_catalog_statement_;
};

}

#endif /* CATALOG_CLASS_H */
