/**
 *  A directory entry is essentially that, what it's name implies
 *  This class implements a data wrapper for single dentries in CVMFS
 *  Additionally to the normal file meta data it manages some
 *  bookkeeping data like the associated catalog.
 */

#ifndef DIRECTORY_ENTRY_H
#define DIRECTORY_ENTRY_H 1

#include <list>

#include "compat.h"
#include "util.h"

namespace cvmfs {

class Catalog;
typedef uint64_t inode_t;

class DirectoryEntry {
  friend class LookupSqlStatement;                   // simplify creation of DirectoryEntry objects
  friend class ManipulateDirectoryEntrySqlStatement; // simplify write of DirectoryEntry objects in database
  friend class WritableCatalogManager;               // simplify creation of DirectoryEntry objects for write back        
  
 public:
  const static inode_t kInvalidInode = 0;
  
  /**
   *  constructor initializes structure to a (senseless) zero value
   */
  inline DirectoryEntry() :
    catalog_(NULL),
    inode_(kInvalidInode),
    parent_inode_(kInvalidInode),
    linkcount_(0),
    mode_(0),
    size_(0),
    mtime_(0),
    hardlink_group_id_(0),
    is_nested_catalog_root_(false),
    is_nested_catalog_mountpoint_(false) {}
  
  inline bool IsNestedCatalogRoot() const { return is_nested_catalog_root_; }
  inline bool IsNestedCatalogMountpoint() const { return is_nested_catalog_mountpoint_; }
  
  inline bool IsLink() const { return S_ISLNK(mode_); }
  inline bool IsDirectory() const { return S_ISDIR(mode_); }
  inline bool IsPartOfHardlinkGroup() const { return hardlink_group_id() > 0; }
  
  inline inode_t inode() const { return inode_; }
  inline inode_t parent_inode() const { return parent_inode_; }
  inline int linkcount() const { return linkcount_; }
  inline std::string name() const { return name_; }
  inline std::string symlink() const { return symlink_; }
  inline hash::t_sha1 checksum() const { return checksum_; }
  inline uint64_t size() const { return (IsLink()) ? symlink().length() : size_; }
  inline unsigned int mode() const { return mode_; }
  inline int hardlink_group_id() const { return hardlink_group_id_; }
  
  /**
   *  returns a plain old struct stat containing all
   *  known meta data of the represented file. This can simple be used as output
   *  of a FUSE callback function.
   *  @return the struct stat for this DirectoryEntry
   */
  inline struct stat GetStatStructure() const {
    struct stat s;
    
    memset(&s, 0, sizeof(s));
    s.st_dev = 1;
    s.st_ino = inode_;
    s.st_mode = mode_;
    s.st_nlink = linkcount_;
    s.st_uid = 1; // TODO: get this globally from somewhere!!
    s.st_gid = 1; // TODO: this as well !!
    s.st_rdev = 1;
    s.st_size = size();
    s.st_blksize = 4096; /* will be ignored by Fuse */
    s.st_blocks = 1 + size() / 512;
    s.st_atime = mtime_;
    s.st_mtime = mtime_;
    s.st_ctime = mtime_;
    
    return s;
  }
  
  // these accessors are accessible from outside but should be used with extreme caution!
  // (With great powers comes huge responsibility!!)
  inline void set_inode(const inode_t inode) { inode_ = inode; }
  inline const Catalog* catalog() const { return catalog_; }
  inline void set_parent_inode(const inode_t parent_inode) { parent_inode_ = parent_inode; }
  inline void set_is_nested_catalog_mountpoint(const bool val) { is_nested_catalog_mountpoint_ = val; }
  inline void set_is_nested_catalog_root(const bool val) { is_nested_catalog_root_ = val; }
  
 private:
  // associated cvmfs catalog
  Catalog* catalog_;
  
  // general file information
  inode_t inode_;
  inode_t parent_inode_;
  int linkcount_;
  unsigned int mode_;
  uint64_t size_;
  time_t mtime_;
  hash::t_sha1 checksum_;
  std::string name_;
  std::string symlink_;
  
  // administrative data
  int hardlink_group_id_;
  bool is_nested_catalog_root_;
  bool is_nested_catalog_mountpoint_;
};

typedef std::list<DirectoryEntry> DirectoryEntryList;

} // cvmfs

#endif /* DIRECTORY_ENTRY_H */
