#ifndef DIRECTORY_ENTRY_H
#define DIRECTORY_ENTRY_H 1

#include <list>

#include "compat.h"

namespace cvmfs {

typedef uint64_t inode_t;

class DirectoryEntry {
 public:
  inline bool IsNestedCatalogRoot() const { /* TODO: implement this! */ return false; }
  inline bool IsNestedCatalogMountpoint() const { /* TODO: implement this! */ return false; }
  
  inline bool IsLink() const { return S_ISLNK(mode_); }
  inline bool IsDirectory() const { return S_ISDIR(mode_); }
  
  inline int catalog_id() const { return catalog_id_; }
  inline inode_t inode() const { return inode_; }
  inline inode_t parent_inode() const { return parent_inode_; }
  inline void set_parent_inode(const inode_t parent_inode) { parent_inode_ = parent_inode; } // TODO: this is only used by cvmfs_lookup for performance improvements... consider this function as friend function to restrict access to this setter
  inline std::string name() const { return name_; }
  inline std::string symlink() const { return symlink_; }
  inline hash::t_sha1 checksum() const { return checksum_; }
  inline uint64_t size() const { return size_; }
  inline unsigned int mode() const { return mode_; }
  
  inline struct stat GetStatStructure() const {
    // TODO: implement this properly
    struct stat s;
    return s;
  }
  
  inline void set_inode(const inode_t inode) { inode_ = inode; }
  
 private:
  int catalog_id_;
  int flags_;
  inode_t inode_;
  inode_t parent_inode_;
  unsigned int mode_;
  uint64_t size_;
  time_t mtime_;
  hash::t_sha1 checksum_;
  std::string name_;
  std::string symlink_;
};

typedef std::list<DirectoryEntry> DirectoryEntryList;

} // cvmfs

#endif /* DIRECTORY_ENTRY_H */
