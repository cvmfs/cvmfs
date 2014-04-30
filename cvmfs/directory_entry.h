/**
 * This file is part of the CernVM File System.
 *
 * This class implements a data wrapper for single dentries in CVMFS
 * Additionally to the normal file meta data it manages some
 * bookkeeping data like the associated catalog.
 */

#ifndef CVMFS_DIRECTORY_ENTRY_H_
#define CVMFS_DIRECTORY_ENTRY_H_

#include <sys/types.h>

#include <list>
#include <string>

#include <cstring>

#include "platform.h"
#include "util.h"
#include "hash.h"
#include "shortstring.h"
#include "globals.h"
#include "bigvector.h"

namespace publish {
class SyncItem;
}

namespace swissknife {
class CommandMigrate;
}

namespace catalog {

class DirectoryEntryTestFactory;

class Catalog;
typedef uint64_t inode_t;

enum SpecialDirents {
  kDirentNormal = 0,
  kDirentNegative,
};

/**
 * This class is a thin wrapper around struct dirent.
 * It only contains file system related meta data for a directory entry
 */
class DirectoryEntryBase {
  // simplify creation of DirectoryEntry objects for write back
  friend class publish::SyncItem;

  // simplify file system like _touch_ of DirectoryEntry objects
  friend class SqlDirentTouch;

 public:
  struct Difference {
    static const unsigned int kIdentical                    = 0x000; // 000000000000
    static const unsigned int kName                         = 0x001; // 000000000001
    static const unsigned int kLinkcount                    = 0x002; // 000000000010
    static const unsigned int kSize                         = 0x004; // 000000000100
    static const unsigned int kMode                         = 0x008; // 000000001000
    static const unsigned int kMtime                        = 0x010; // 000000010000
    static const unsigned int kSymlink                      = 0x020; // 000000100000
    static const unsigned int kChecksum                     = 0x040; // 000001000000
    static const unsigned int kHardlinkGroup                = 0x080; // 000010000000
    static const unsigned int kNestedCatalogTransitionFlags = 0x100; // 000100000000
    static const unsigned int kChunkedFileFlag              = 0x200; // 001000000000
  };
  typedef unsigned int Differences;

 public:
  const static inode_t kInvalidInode = 0;

  /**
   * Zero-constructed DirectoryEntry objects are unusable as such.
   */
  inline DirectoryEntryBase() :
    inode_(kInvalidInode),
    parent_inode_(kInvalidInode),
    //generation_(0),
    mode_(0),
    uid_(0),
    gid_(0),
    size_(0),
    mtime_(0),
    linkcount_(1) // generally a normal file has linkcount 1 -> default
    { }

  // accessors
  inline bool IsRegular() const                 { return S_ISREG(mode_); }
  inline bool IsLink() const                    { return S_ISLNK(mode_); }
  inline bool IsDirectory() const               { return S_ISDIR(mode_); }

  inline inode_t inode() const                  { return inode_; }
  inline inode_t parent_inode() const           { return parent_inode_; }
  //inline uint32_t generation() const           { return generation_; }
  inline uint32_t linkcount() const             { return linkcount_; }
  inline NameString name() const                { return name_; }
  inline LinkString symlink() const             { return symlink_; }

  inline time_t mtime() const                   { return mtime_; }
  inline unsigned int mode() const              { return mode_; }
  inline uid_t uid() const                      { return uid_; }
  inline gid_t gid() const                      { return gid_; }

  inline shash::Any checksum() const            { return checksum_; }
  inline const shash::Any *checksum_ptr() const { return &checksum_; }
  inline shash::Algorithms hash_algorithm() const { return checksum_.algorithm; }

  inline uint64_t size() const {
    return (IsLink()) ? symlink().GetLength() : size_;
  }

  inline std::string GetFullPath(const std::string &parent_directory) const {
    std::string file_path = parent_directory + "/";
    file_path.append(name().GetChars(), name().GetLength());
    return file_path;
  }

  // some reasonable setters
  inline void set_inode(const inode_t inode) { inode_ = inode; }
  inline void set_parent_inode(const inode_t parent_inode) {
    parent_inode_ = parent_inode;
  }

  inline void set_linkcount(const uint32_t linkcount) {
    assert(linkcount > 0);
    linkcount_ = linkcount;
  }
  inline void set_symlink(const LinkString &symlink) {
    symlink_ = symlink;
  }

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
    s.st_nlink = linkcount();
    s.st_uid = uid();
    s.st_gid = gid();
    s.st_rdev = 1;
    s.st_size = size();
    s.st_blksize = 4096;  // will be ignored by Fuse
    s.st_blocks = 1 + size() / 512;
    s.st_atime = mtime_;
    s.st_mtime = mtime_;
    s.st_ctime = mtime_;

    return s;
  }

  Differences CompareTo(const DirectoryEntryBase &other) const;
  inline bool operator==(const DirectoryEntryBase &other) const {
    return CompareTo(other) == Difference::kIdentical;
  }
  inline bool operator!=(const DirectoryEntryBase &other) const {
    return !(*this == other);
  }

 protected:

  inode_t inode_;        // inodes are generated on the fly by the cvmfs client.
  inode_t parent_inode_; // since they are file system stuff, we have them here
                         // Though, they are NOT written to any catalog.
  //uint32_t generation_;

  // stat like information
  NameString name_;
  unsigned int mode_;
  uid_t uid_;
  gid_t gid_;
  uint64_t size_;
  time_t mtime_;
  LinkString symlink_;
  uint32_t linkcount_;

  // checksum is not part of the file system intrinsics, though can be computed
  // just using the file contents... we therefore put it in this base class.
  shash::Any checksum_;
};

/**
 * DirectoryEntries might contain cvmfs-specific meta data
 * Currently these are the following things:
 *  - Pointer to the originating catalog
 *  - Markers for nested catalog transition points (mountpoint and root entry)
 *  - Transient marker storing the time of caching (Fuse page caches).
 *    This is required to invalidate caches after a catalog update
 *  - Hardlink group used to emulate hardlinks in cvmfs
 */
class DirectoryEntry : public DirectoryEntryBase {
  friend class SqlLookup;                  // simplify creation of DirectoryEntry objects
  friend class SqlDirentWrite;             // simplify write of DirectoryEntry objects in database
  friend class swissknife::CommandMigrate; // fixing DirectoryEntry glitches
  friend class WritableCatalogManager;     // TODO: remove this dependency
  friend class DirectoryEntryTestFactory;  // create DirectoryEntries for unit-test purposes

 public:
  /**
   * This is _kind of_ a copy constructor allowing us to create
   * DirectoryEntries directly from DirectoryEntryBase objects. Though we
   * make this explicit, to disallow black magic from happening. It uses the
   * copy constructor of DirectoryEntryBase and initializes the additional
   * fields of DirectoryEntry.
   */
  inline explicit DirectoryEntry(const DirectoryEntryBase& base) :
    DirectoryEntryBase(base),
    cached_mtime_(0),
    hardlink_group_(0),
    is_nested_catalog_root_(false),
    is_nested_catalog_mountpoint_(false),
    is_chunked_file_(false),
    is_negative_(false) {}

  inline DirectoryEntry() :
    cached_mtime_(0),
    hardlink_group_(0),
    is_nested_catalog_root_(false),
    is_nested_catalog_mountpoint_(false),
    is_chunked_file_(false),
    is_negative_(false) {}

  inline explicit DirectoryEntry(SpecialDirents special_type) :
    cached_mtime_(0),
    hardlink_group_(0),
    is_nested_catalog_root_(false),
    is_nested_catalog_mountpoint_(false),
    is_chunked_file_(false),
    is_negative_(true) { assert(special_type == kDirentNegative); };

  inline SpecialDirents GetSpecial() const {
    return is_negative_ ? kDirentNegative : kDirentNormal;
  }

  Differences CompareTo(const DirectoryEntry &other) const;
  inline bool operator==(const DirectoryEntry &other) const {
    return CompareTo(other) == Difference::kIdentical;
  }
  inline bool operator!=(const DirectoryEntry &other) const {
    return !(*this == other);
  }

  inline bool IsNegative() const { return is_negative_; }

  inline bool IsNestedCatalogRoot() const { return is_nested_catalog_root_; }
  inline bool IsNestedCatalogMountpoint() const {
    return is_nested_catalog_mountpoint_;
  }
  inline bool IsChunkedFile() const { return is_chunked_file_; }

  inline uint32_t hardlink_group() const { return hardlink_group_; }
  inline time_t cached_mtime() const     { return cached_mtime_; }

  inline void set_hardlink_group(const uint32_t group) { hardlink_group_ = group; }
  inline void set_cached_mtime(const time_t value)     { cached_mtime_ = value; }

  inline void set_is_nested_catalog_mountpoint(const bool val) {
    is_nested_catalog_mountpoint_ = val;
  }
  inline void set_is_nested_catalog_root(const bool val) {
    is_nested_catalog_root_ = val;
  }

  inline void set_is_chunked_file(const bool val) {
    is_chunked_file_ = val;
  }

 private:
  time_t cached_mtime_;  /**< can be compared to mtime to figure out if caches
                              need to be invalidated (file has changed) */

  // Hardlink handling is a bit unsual in CVMFS. Since inodes are allocated
  // on demand we only save hardlink relationships using a `hardlink_group`
  uint32_t hardlink_group_;

  // Administrative data
  bool is_nested_catalog_root_;
  bool is_nested_catalog_mountpoint_;
  bool is_chunked_file_;
  bool is_negative_;
};

/**
 * Saves memory for large directory listings
 */
struct StatEntry {
  NameString name;
  struct stat info;

  StatEntry() { memset(&info, 0, sizeof(info)); }
  StatEntry(const NameString &n, const struct stat &i) : name(n), info(i) { }
};

typedef std::vector<DirectoryEntry> DirectoryEntryList;         // TODO: rename!
typedef std::vector<DirectoryEntryBase> DirectoryEntryBaseList; //       these are NOT lists.
typedef BigVector<StatEntry> StatEntryList;  // TODO: use mmap for large listings

} // namespace catalog

#endif  // CVMFS_DIRECTORY_ENTRY_H_
