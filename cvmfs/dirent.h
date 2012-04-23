/**
 * This file is part of the CernVM File System.
 *
 * This class implements a data wrapper for single dentries in CVMFS
 * Additionally to the normal file meta data it manages some
 * bookkeeping data like the associated catalog.
 */

#ifndef CVMFS_DIRENT_H_
#define CVMFS_DIRENT_H_

#include <list>
#include <string>

#include "platform.h"
#include "util.h"
#include "hash.h"
#include "shortstring.h"
#include "globals.h"

namespace publish {
class SyncItem;
}

namespace catalog {

class Catalog;
typedef uint64_t inode_t;

enum SpecialDirents {
  kDirentNormal = 0,
  kDirentNegative,
};

class DirectoryEntry {
  friend class LookupSqlStatement;                   // simplify creation of DirectoryEntry objects
  friend class ManipulateDirectoryEntrySqlStatement; // simplify write of DirectoryEntry objects in database
  friend class publish::SyncItem;                    // simplify creation of DirectoryEntry objects for write back
  friend class WritableCatalogManager;               // TODO: remove this dependency

public:
  const static inode_t kInvalidInode = 0;

  /**
   * Zero-constructed DirectoryEntry objects are unusable as such.
   */
  inline DirectoryEntry() :
    catalog_(NULL),
    inode_(kInvalidInode),
    parent_inode_(kInvalidInode),
    linkcount_(0),
    mode_(0),
    size_(0),
    mtime_(0),
    cached_mtime_(0),
    hardlink_group_id_(0),
    is_nested_catalog_root_(false),
    is_nested_catalog_mountpoint_(false) { }

  inline explicit DirectoryEntry(SpecialDirents special_type) :
    catalog_((Catalog *)(-1)) { };

  inline SpecialDirents GetSpecial() {
    return (catalog_ == (Catalog *)(-1)) ? kDirentNegative : kDirentNormal;
  }
  inline bool IsNestedCatalogRoot() const { return is_nested_catalog_root_; }
  inline bool IsNestedCatalogMountpoint() const {
    return is_nested_catalog_mountpoint_;
  }

  inline bool IsRegular() const { return S_ISREG(mode_); }
  inline bool IsLink() const { return S_ISLNK(mode_); }
  inline bool IsDirectory() const { return S_ISDIR(mode_); }
  inline bool IsPartOfHardlinkGroup() const { return hardlink_group_id() > 0; }

  inline inode_t inode() const { return inode_; }
  inline inode_t parent_inode() const { return parent_inode_; }
  inline int linkcount() const { return linkcount_; }
  inline NameString name() const { return name_; }
  inline LinkString symlink() const { return symlink_; }
  inline hash::Any checksum() const { return checksum_; }
  inline const hash::Any *checksum_ptr() const { return &checksum_; }
  inline uint64_t size() const {
    return (IsLink()) ? symlink().GetLength() : size_;
  }
  inline time_t mtime() const { return mtime_; }
  inline time_t cached_mtime() const { return cached_mtime_; }
  inline unsigned int mode() const { return mode_; }
  inline int hardlink_group_id() const { return hardlink_group_id_; }

  /**
   * Converts to a stat struct as required by many Fuse callbacks.
   * @return the struct stat for this DirectoryEntry
   */
  inline struct stat GetStatStructure() const {
    struct stat s;

    memset(&s, 0, sizeof(s));
    s.st_dev = 1;
    s.st_ino = inode_;
    s.st_mode = mode_;
    s.st_nlink = linkcount_;
    s.st_uid = g_uid;
    s.st_gid = g_gid;
    s.st_rdev = 1;
    s.st_size = size();
    s.st_blksize = 4096;  // will be ignored by Fuse
    s.st_blocks = 1 + size() / 512;
    s.st_atime = mtime_;
    s.st_mtime = mtime_;
    s.st_ctime = mtime_;

    return s;
  }

  inline void set_cached_mtime(const time_t value) { cached_mtime_ = value; }
  inline void set_inode(const inode_t inode) { inode_ = inode; }
  inline const Catalog *catalog() const { return catalog_; }
  inline void set_parent_inode(const inode_t parent_inode) {
    parent_inode_ = parent_inode;
  }
  inline void set_is_nested_catalog_mountpoint(const bool val) {
    is_nested_catalog_mountpoint_ = val;
  }
  inline void set_is_nested_catalog_root(const bool val) {
    is_nested_catalog_root_ = val;
  }

private:
  // Associated cvmfs catalog
  Catalog* catalog_;

  // stat like information
  NameString name_;
  inode_t inode_;
  inode_t parent_inode_;
  int linkcount_;
  unsigned int mode_;
  uint64_t size_;
  time_t mtime_;
  time_t cached_mtime_;  /**< can be compared to mtime to figure out if caches
                              need to be invalidated (file has changed) */
  // TODO: unionize
  LinkString symlink_;
  hash::Any checksum_;

  // Administrative data
  int hardlink_group_id_;
  bool is_nested_catalog_root_;
  bool is_nested_catalog_mountpoint_;
};

/**
 * Saves memory for large directory listings
 */
struct StatEntry {
  NameString name;
  struct stat info;

  StatEntry() { }
  StatEntry(const NameString &n, const struct stat &i) : name(n), info(i) { }
};

typedef std::vector<DirectoryEntry> DirectoryEntryList;
typedef std::vector<StatEntry> StatEntryList;

} // namespace catalog

#endif  // CVMFS_DIRENT_H_
