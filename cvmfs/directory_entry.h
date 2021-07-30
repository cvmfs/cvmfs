/**
 * This file is part of the CernVM File System.
 *
 * Data wrappers for single dentries.  In addition to the normal file meta data
 * it manages bookkeeping data specific to cvmfs such as the associated catalog.
 */

#ifndef CVMFS_DIRECTORY_ENTRY_H_
#define CVMFS_DIRECTORY_ENTRY_H_

#include <sys/types.h>

#include <cassert>
#include <cstring>
#include <string>
#include <vector>

#include "bigvector.h"
#include "compression.h"
#include "hash.h"
#include "platform.h"
#include "shortstring.h"

namespace publish {
class SyncItem;
class SyncItemNative;
class SyncItemTar;
class SyncItemDummyDir;
class SyncItemDummyCatalog;
}
namespace swissknife {
class CommandMigrate;
}

namespace catalog {

// Create DirectoryEntries for unit test purposes.
class DirectoryEntryTestFactory;

class MockCatalogManager;
class Catalog;
class WritableCatalogManager;

template <class CatalogMgrT>
class CatalogBalancer;
typedef uint64_t inode_t;

enum SpecialDirents {
  kDirentNormal = 0,
  kDirentNegative,
};

/**
 * Wrapper around struct dirent.  Only contains file system related meta data
 * for a directory entry.
 * TODO(jblomer): separation to DirectoryEntry not quite clear: this one also
 * contains hash, compression algorithm and external flag
 */
class DirectoryEntryBase {
  // For testing the catalog balancing
  friend class CatalogBalancer<MockCatalogManager>;
  // Create .cvmfscatalog and .cvmfsautocatalog files
  friend class CatalogBalancer<WritableCatalogManager>;
  // Simplify creation of DirectoryEntry objects for write back
  friend class publish::SyncItem;
  friend class publish::SyncItemNative;
  friend class publish::SyncItemTar;
  friend class publish::SyncItemDummyDir;
  friend class publish::SyncItemDummyCatalog;
  // Simplify file system like _touch_ of DirectoryEntry objects
  friend class SqlDirentTouch;
  // Allow creation of virtual directories and files
  friend class VirtualCatalog;

 public:
  static const inode_t kInvalidInode = 0;

  /**
   * Used in the swissknife for sanity checks and catalog migration.  If
   * anything is added, also adjust PrintDifferences in swissknife::CommandDiff.
   */
  struct Difference {
    static const unsigned int kIdentical                    = 0x000;
    static const unsigned int kName                         = 0x001;
    static const unsigned int kLinkcount                    = 0x002;
    static const unsigned int kSize                         = 0x004;
    static const unsigned int kMode                         = 0x008;
    static const unsigned int kMtime                        = 0x010;
    static const unsigned int kSymlink                      = 0x020;
    static const unsigned int kChecksum                     = 0x040;
    static const unsigned int kHardlinkGroup                = 0x080;
    static const unsigned int kNestedCatalogTransitionFlags = 0x100;
    static const unsigned int kChunkedFileFlag              = 0x200;
    static const unsigned int kHasXattrsFlag                = 0x400;
    static const unsigned int kExternalFileFlag             = 0x800;
    static const unsigned int kBindMountpointFlag           = 0x1000;
    static const unsigned int kHiddenFlag                   = 0x2000;
    static const unsigned int kDirectIoFlag                 = 0x4000;
  };
  typedef unsigned int Differences;

  /**
   * Zero-constructed DirectoryEntry objects are unusable as such.
   */
  inline DirectoryEntryBase()
    : inode_(kInvalidInode)
    , mode_(0)
    , uid_(0)
    , gid_(0)
    , size_(0)
    , mtime_(0)
    , linkcount_(1)  // generally a normal file has linkcount 1 -> default
    , has_xattrs_(false)
    , is_external_file_(false)
    , is_direct_io_(false)
    , compression_algorithm_(zlib::kZlibDefault)
    { }

  inline bool IsRegular() const                 { return S_ISREG(mode_); }
  inline bool IsLink() const                    { return S_ISLNK(mode_); }
  inline bool IsDirectory() const               { return S_ISDIR(mode_); }
  inline bool IsFifo() const                    { return S_ISFIFO(mode_); }
  inline bool IsSocket() const                  { return S_ISSOCK(mode_); }
  inline bool IsCharDev() const                 { return S_ISCHR(mode_); }
  inline bool IsBlockDev() const                { return S_ISBLK(mode_); }
  inline bool IsSpecial() const {
    return IsFifo() || IsSocket() || IsCharDev() || IsBlockDev();
  }
  inline bool IsExternalFile() const            { return is_external_file_; }
  inline bool IsDirectIo() const                { return is_direct_io_; }
  inline bool HasXattrs() const                 { return has_xattrs_;    }

  inline inode_t inode() const                  { return inode_; }
  inline uint32_t linkcount() const             { return linkcount_; }
  inline NameString name() const                { return name_; }
  inline LinkString symlink() const             { return symlink_; }
  inline time_t mtime() const                   { return mtime_; }
  inline unsigned int mode() const              { return mode_; }
  inline uid_t uid() const                      { return uid_; }
  inline gid_t gid() const                      { return gid_; }
  inline shash::Any checksum() const            { return checksum_; }
  inline const shash::Any *checksum_ptr() const { return &checksum_; }
  inline shash::Algorithms hash_algorithm() const {
    return checksum_.algorithm;
  }
  inline uint64_t size() const {
    if (IsLink())
      return symlink().GetLength();
    if (IsBlockDev() || IsCharDev())
      return 0;
    return size_;
  }
  inline dev_t rdev() const {
    if (IsBlockDev() || IsCharDev())
      return size_;
    return 1;
  }
  inline std::string GetFullPath(const std::string &parent_directory) const {
    std::string file_path = parent_directory + "/";
    file_path.append(name().GetChars(), name().GetLength());
    return file_path;
  }

  inline void set_inode(const inode_t inode) { inode_ = inode; }
  inline void set_linkcount(const uint32_t linkcount) {
    assert(linkcount > 0);
    linkcount_ = linkcount;
  }
  inline void set_symlink(const LinkString &symlink) {
    symlink_ = symlink;
  }
  inline void set_has_xattrs(const bool has_xattrs) {
    has_xattrs_ = has_xattrs;
  }

  inline zlib::Algorithms compression_algorithm() const {
    return compression_algorithm_;
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
    s.st_rdev = rdev();
    s.st_size = size();
    s.st_blksize = 4096;  // will be ignored by Fuse
    s.st_blocks = 1 + size() / 512;
    s.st_atime = mtime_;
    s.st_mtime = mtime_;
    s.st_ctime = mtime_;
    return s;
  }

  Differences CompareTo(const DirectoryEntryBase &other) const;
  inline bool operator ==(const DirectoryEntryBase &other) const {
    return CompareTo(other) == Difference::kIdentical;
  }
  inline bool operator !=(const DirectoryEntryBase &other) const {
    return !(*this == other);
  }

 protected:
  // Inodes are generated based on the rowid of the entry in the file catalog.
  inode_t inode_;

  // Data from struct stat
  NameString name_;
  unsigned int mode_;
  uid_t uid_;
  gid_t gid_;
  uint64_t size_;
  time_t mtime_;
  LinkString symlink_;
  uint32_t linkcount_;
  // In order to save memory, we only indicate if a directory entry has custom
  // extended attributes.  Another call to the file catalog is necessary to
  // get them.
  bool has_xattrs_;

  // The cryptographic hash is not part of the file system intrinsics, though
  // it can be computed just using the file contents.  We therefore put it in
  // this base class.
  shash::Any checksum_;

  bool is_external_file_;
  bool is_direct_io_;

  // The compression algorithm
  zlib::Algorithms compression_algorithm_;
};


/**
 * In addition to the file system meta-data covered by DirectoryEntryBase,
 * DirectoryEntries contain cvmfs-specific meta data.  Currently these are the
 * following things:
 *  - Pointer to the originating catalog
 *  - Markers for nested catalog transition points (mountpoint and root entry)
 *  - Transient marker storing the time of caching (Fuse page caches).
 *    This is required to invalidate caches after a catalog update
 *  - Hardlink group used to emulate hardlinks in cvmfs
 */
class DirectoryEntry : public DirectoryEntryBase {
  // Simplify creation of DirectoryEntry objects
  friend class SqlLookup;
  // Simplify write of DirectoryEntry objects in database
  friend class SqlDirentWrite;
  // For fixing DirectoryEntry glitches
  friend class swissknife::CommandMigrate;
  // TODO(rmeusel): remove this dependency
  friend class WritableCatalogManager;
  // Create DirectoryEntries for unit test purposes.
  friend class DirectoryEntryTestFactory;

 public:
  /**
   * This is _kind of_ a copy constructor allowing us to create
   * DirectoryEntries directly from DirectoryEntryBase objects.  We make it
   * explicit, to disallow black magic from happening.  It uses the copy
   * constructor of DirectoryEntryBase and initializes the additional fields of
   * DirectoryEntry.
   */
  inline explicit DirectoryEntry(const DirectoryEntryBase& base)
    : DirectoryEntryBase(base)
    , hardlink_group_(0)
    , is_nested_catalog_root_(false)
    , is_nested_catalog_mountpoint_(false)
    , is_bind_mountpoint_(false)
    , is_chunked_file_(false)
    , is_hidden_(false)
    , is_negative_(false) { }

  inline DirectoryEntry()
    : hardlink_group_(0)
    , is_nested_catalog_root_(false)
    , is_nested_catalog_mountpoint_(false)
    , is_bind_mountpoint_(false)
    , is_chunked_file_(false)
    , is_hidden_(false)
    , is_negative_(false) { }

  inline explicit DirectoryEntry(SpecialDirents special_type)
    : hardlink_group_(0)
    , is_nested_catalog_root_(false)
    , is_nested_catalog_mountpoint_(false)
    , is_bind_mountpoint_(false)
    , is_chunked_file_(false)
    , is_hidden_(false)
    , is_negative_(true) { assert(special_type == kDirentNegative); }

  inline SpecialDirents GetSpecial() const {
    return is_negative_ ? kDirentNegative : kDirentNormal;
  }

  Differences CompareTo(const DirectoryEntry &other) const;
  inline bool operator ==(const DirectoryEntry &other) const {
    return CompareTo(other) == Difference::kIdentical;
  }
  inline bool operator !=(const DirectoryEntry &other) const {
    return !(*this == other);
  }

  inline bool IsNegative() const { return is_negative_; }
  inline bool IsNestedCatalogRoot() const { return is_nested_catalog_root_; }
  inline bool IsNestedCatalogMountpoint() const {
    return is_nested_catalog_mountpoint_;
  }
  inline bool IsBindMountpoint() const { return is_bind_mountpoint_; }
  inline bool IsChunkedFile() const { return is_chunked_file_; }
  inline bool IsHidden() const { return is_hidden_; }
  inline uint32_t hardlink_group() const { return hardlink_group_; }

  inline void set_hardlink_group(const uint32_t group) {
    hardlink_group_ = group;
  }
  inline void set_is_nested_catalog_mountpoint(const bool val) {
    is_nested_catalog_mountpoint_ = val;
  }
  inline void set_is_nested_catalog_root(const bool val) {
    is_nested_catalog_root_ = val;
  }
  inline void set_is_bind_mountpoint(const bool val) {
    is_bind_mountpoint_ = val;
  }
  inline void set_is_chunked_file(const bool val) {
    is_chunked_file_ = val;
  }
  inline void set_is_hidden(const bool val) {
    is_hidden_ = val;
  }

 private:
  /**
   * Hardlink handling is emulated in CVMFS. Since inodes are allocated on
   * demand we save hardlink relationships using the same hardlink_group.
   */
  uint32_t hardlink_group_;

  // TODO(jblomer): transform into bitfield to save memory
  bool is_nested_catalog_root_;
  bool is_nested_catalog_mountpoint_;
  bool is_bind_mountpoint_;
  bool is_chunked_file_;
  bool is_hidden_;
  bool is_negative_;
};


/**
 * Saves memory for large directory listings.
 */
struct StatEntry {
  NameString name;
  struct stat info;

  StatEntry() { memset(&info, 0, sizeof(info)); }
  StatEntry(const NameString &n, const struct stat &i) : name(n), info(i) { }
};


typedef std::vector<DirectoryEntry> DirectoryEntryList;
typedef std::vector<DirectoryEntryBase> DirectoryEntryBaseList;
// TODO(jblomer): use mmap for large listings
typedef BigVector<StatEntry> StatEntryList;

}  // namespace catalog

#endif  // CVMFS_DIRECTORY_ENTRY_H_
