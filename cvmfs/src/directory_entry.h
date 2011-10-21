#ifndef DIRECTORY_ENTRY_H
#define DIRECTORY_ENTRY_H 1

#include <list>

#include "compat.h"
#include "util.h"

namespace cvmfs {

class Catalog;
typedef uint64_t inode_t;

class DirectoryEntry {
  friend class LookupSqlStatement; // simplify creation of DirectoryEntry objects
  
 public:
  const static inode_t kInvalidInode = 0;
  
  const static int kFlagDir                 = 1;
  const static int kFlagDirNestedMountpoint = 2;  /* Link in the parent catalog */
  const static int kFlagDirNestedRoot       = 32; /* Link in the child catalog */
  const static int kFlagFile                = 4;
  const static int kFlagLink                = 8;
  const static int kFlagFileStat            = 16;
  const static int kFlagFileChunk           = 64;
  const static int kFlagLinkCount_0         = 256; // 8 bit for link count of file, starting for here
  const static int kFlagLinkCount_1         = kFlagLinkCount_0 << 1;
  const static int kFlagLinkCount_2         = kFlagLinkCount_0 << 2;
  const static int kFlagLinkCount_3         = kFlagLinkCount_0 << 3;
  const static int kFlagLinkCount_4         = kFlagLinkCount_0 << 4;
  const static int kFlagLinkCount_5         = kFlagLinkCount_0 << 5;
  const static int kFlagLinkCount_6         = kFlagLinkCount_0 << 6;
  const static int kFlagLinkCount_7         = kFlagLinkCount_0 << 7;
  const static int kFlagLinkCount           = kFlagLinkCount_0 | kFlagLinkCount_1 | kFlagLinkCount_2 | kFlagLinkCount_3 | kFlagLinkCount_4 | kFlagLinkCount_5 | kFlagLinkCount_6 | kFlagLinkCount_7;
  
  inline bool IsNestedCatalogRoot() const { return flags_ & kFlagDirNestedRoot; }
  inline bool IsNestedCatalogMountpoint() const { return flags_ & kFlagDirNestedMountpoint; }
  
  inline bool IsLink() const { return S_ISLNK(mode_); }
  inline bool IsDirectory() const { return S_ISDIR(mode_); }
  inline bool IsPartOfHardlinkGroup() const { return GetHardlinkGroupId() > 0; }
  
  inline inode_t inode() const { return inode_; }
  inline inode_t parent_inode() const { return parent_inode_; }
  inline std::string name() const { return name_; }
  inline std::string symlink() const { return symlink_; }
  inline hash::t_sha1 checksum() const { return checksum_; }
  inline uint64_t size() const { return size_; }
  inline unsigned int mode() const { return mode_; }
  
  inline int GetHardlinkGroupId() const { return hardlink_group_id_; }
  inline struct stat GetStatStructure() const {
    struct stat s;
    
    memset(&s, 0, sizeof(s));
    s.st_dev = 1;
    s.st_ino = inode_;
    s.st_mode = mode_;
    s.st_nlink = GetLinkcount();
    s.st_uid = 1; // TODO: get this globally from somewhere!!
    s.st_gid = 1; // TODO: this as well !!
    s.st_rdev = 1;
    s.st_size = (IsLink()) ? expand_env(symlink_).length() : size_;
    s.st_blksize = 4096; /* will be ignored by Fuse */
    s.st_blocks = 1 + size_ / 512;
    s.st_atime = mtime_;
    s.st_mtime = mtime_;
    s.st_ctime = mtime_;
    
    return s;
  }
  
  // these accessors are accessible from outside but should be used with extreme caution!
  // (With great power comes huge resonsibility!!)
  inline void set_inode(const inode_t inode) { inode_ = inode; }
  inline const Catalog* catalog() const { return catalog_; }
  inline void set_parent_inode(const inode_t parent_inode) { parent_inode_ = parent_inode; }
  
 private:
  inline int GetLinkcount() const {
    return (flags_ & kFlagLinkCount) / kFlagLinkCount_0;
  }
  
 private:
  const Catalog* catalog_;
  int flags_;
  inode_t inode_;
  inode_t parent_inode_;
  unsigned int mode_;
  uint64_t size_;
  time_t mtime_;
  hash::t_sha1 checksum_;
  std::string name_;
  std::string symlink_;
  
  int hardlink_group_id_;
};

typedef std::list<DirectoryEntry> DirectoryEntryList;

} // cvmfs

#endif /* DIRECTORY_ENTRY_H */
