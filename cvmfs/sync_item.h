/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_H_
#define CVMFS_SYNC_ITEM_H_

#include <cstring>
#include <map>
#include <string>

#include "directory_entry.h"
#include "hash.h"
#include "platform.h"
#include "sync_union.h"
#include "util.h"

namespace publish {

enum SyncItemType {
  kItemDir,
  kItemFile,
  kItemSymlink,
  kItemNew
};


/**
 * Every directory entry emitted by the FileSystemTraversal is wrapped in a
 * SyncItem structure.
 * This class represents potentially three concrete files:
 *   - <read-only path>/<filename>
 *   - <scratch (read-write) branch>/<filename>
 *   - <union volume path>/<filename>
 * The main purpose of this class is to cache stat calls to the underlying
 * files in the different locations as well as hiding some implementation
 * details.
 */
class SyncItem {
 public:
  /**
   *  create a new SyncItem (is normally not required for normal usage
   *                         as the RecursionEngine provides you with DirEntries)
   *  @param dirPath the RELATIVE path to the file
   *  @param filename the name of the file ;-)
   *  @param entryType well...
   */
  SyncItem();  // TODO(rmeusel): Remove
  SyncItem(const std::string &relative_parent_path,
           const std::string &filename,
           const SyncItemType entry_type,
           const SyncUnion *union_engine);

  inline bool IsDirectory()     const { return scratch_type_ == kItemDir;     }
  inline bool WasDirectory()    const { return rdonly_type_  == kItemDir;     }
  inline bool IsRegularFile()   const { return scratch_type_ == kItemFile;    }
  inline bool WasRegularFile()  const { return rdonly_type_  == kItemFile;    }
  inline bool IsSymlink()       const { return scratch_type_ == kItemSymlink; }
  inline bool WasSymlink()      const { return rdonly_type_  == kItemSymlink; }
  inline bool IsWhiteout()      const { return whiteout_;                     }
  inline bool IsCatalogMarker() const { return filename_ == ".cvmfscatalog";  }
  bool IsOpaqueDirectory() const;
  bool IsNew() const { return rdonly_type_ == kItemNew; }

  inline shash::Any GetContentHash() const { return content_hash_; }
  inline void SetContentHash(const shash::Any &hash) { content_hash_ = hash; }
  inline bool HasContentHash() const { return !content_hash_.IsNull(); }

  catalog::DirectoryEntryBase CreateBasicCatalogDirent() const;

  inline std::string GetRelativePath() const {
    return (relative_parent_path_.empty()) ?
      filename_                            :
      relative_parent_path_ + (filename_.empty() ? "" : ("/" + filename_));
  }

  std::string GetRdOnlyPath() const;
  std::string GetUnionPath() const;
  std::string GetScratchPath() const;

  void MarkAsWhiteout(const std::string &actual_filename);

  unsigned int GetRdOnlyLinkcount() const;
  uint64_t GetRdOnlyInode() const;
  unsigned int GetUnionLinkcount() const;
  uint64_t GetUnionInode() const;
  inline platform_stat64 GetUnionStat() const {
    StatUnion();
    return union_stat_.stat;
  }

  inline std::string filename() const { return filename_; }
  inline std::string relative_parent_path() const {
    return relative_parent_path_;
  }

  bool operator==(const SyncItem &other) const {
    return ((relative_parent_path_ == other.relative_parent_path_) &&
            (filename_ == other.filename_));
  }

 protected:
  SyncItemType GetRdOnlyFiletype() const;

 private:
  /**
   * Structure to cache stat calls to the different file locations.
   */
  struct EntryStat {
    EntryStat() : obtained(false), error_code(0) {
      memset(&stat, 0, sizeof(stat));
    }

    bool obtained;   /**< false at the beginning, true after first stat call */
    int error_code;  /**< errno value of the stat call */
    platform_stat64 stat;
  };

  const SyncUnion *union_engine_;

  mutable EntryStat rdonly_stat_;
  mutable EntryStat union_stat_;
  mutable EntryStat scratch_stat_;

  bool whiteout_;
  std::string relative_parent_path_;
  std::string filename_;

  SyncItemType scratch_type_;
  SyncItemType rdonly_type_;

  // The hash of regular file's content
  shash::Any content_hash_;

  // Lazy evaluation and caching of results of file stats
  inline void StatRdOnly(const bool refresh = false) const {
    StatGeneric(GetRdOnlyPath(), &rdonly_stat_, refresh);
  }
  inline void StatUnion(const bool refresh = false) const {
    StatGeneric(GetUnionPath(), &union_stat_, refresh);
  }
  inline void StatOverlay(const bool refresh = false) const {
    StatGeneric(GetScratchPath(), &scratch_stat_, refresh);
  }
  static void StatGeneric(const std::string  &path,
                          EntryStat          *info,
                          const bool          refresh);
};

typedef std::map<std::string, SyncItem> SyncItemList;

}  // namespace publish

#endif  // CVMFS_SYNC_ITEM_H_
