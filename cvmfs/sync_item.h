/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_H_
#define CVMFS_SYNC_ITEM_H_

#include <string>

#include "platform.h"
#include "hash.h"
#include "dirent.h"
#include "sync_union.h"

namespace publish {

enum SyncItemType {
	kItemDir,
	kItemFile,
	kItemSymlink
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
	SyncItem() { };  // TODO: Remove
  SyncItem(const std::string &relative_parent_path,
	         const std::string &filename,
	         const SyncItemType entry_type,
	         const SyncUnion *union_engine);

	inline bool IsDirectory() const { return type_ == kItemDir; }
	inline bool IsRegularFile() const { return type_ == kItemFile; }
	inline bool IsSymlink() const { return type_ == kItemSymlink; }
	inline bool IsWhiteout() const { return whiteout_; }
	inline bool IsCatalogMarker() const { return filename_ == ".cvmfscatalog"; }
	bool IsOpaqueDirectory() const;
  bool IsNew() const;

	inline hash::Any GetContentHash() const { return content_hash_; }
	inline void SetContentHash(const hash::Any &hash) { content_hash_ = hash; }
	inline bool HasContentHash() { return !content_hash_.IsNull(); }

  catalog::DirectoryEntry CreateCatalogDirent() const;

	inline std::string GetRelativePath() const {
    return (relative_parent_path_.empty()) ? filename_ :
      relative_parent_path_ + "/" + filename_;
  }

  std::string GetRdOnlyPath() const;
  std::string GetUnionPath() const;
  std::string GetScratchPath() const;

  void MarkAsWhiteout(const std::string &actual_filename);

	unsigned int GetUnionLinkcount() const;
	uint64_t GetUnionInode() const;
	inline platform_stat64 GetUnionStat() const {
    StatUnion();
    return union_stat_.stat;
  };

  inline std::string filename() const { return filename_; }
	inline std::string relative_parent_path() const {
    return relative_parent_path_;
  }

  bool operator==(const SyncItem &other) const {
    return ((relative_parent_path_ == other.relative_parent_path_) &&
            (filename_ == other.filename_));
  }

 private:
	/**
	 * Structure to cache stat calls to the different file locations.
	 */
	struct EntryStat {
	  EntryStat() : obtained(false), error_code(0) { }

		bool obtained;  /**< false at the beginning, true after first stat call */
		int error_code;  /**< errno value of the stat call */
		platform_stat64 stat;
	};

  SyncItemType type_;
	bool whiteout_;
	std::string relative_parent_path_;
	std::string filename_;
	// The hash of regular file's content
	hash::Any content_hash_;
  const SyncUnion *union_engine_;

  mutable EntryStat rdonly_stat_;
	mutable EntryStat union_stat_;
	mutable EntryStat scratch_stat_;

  // Lazy evaluation and caching of results of file stats
	inline void StatRdOnly() const {
    if (rdonly_stat_.obtained) return;
    StatGeneric(GetRdOnlyPath(), &rdonly_stat_);
  }
	inline void StatUnion() const {
    if (union_stat_.obtained) return;
    StatGeneric(GetUnionPath(), &union_stat_);
  }
	inline void StatOverlay() const {
    if (scratch_stat_.obtained) return;
    StatGeneric(GetScratchPath(), &scratch_stat_);
  }
	static void StatGeneric(const std::string &path, EntryStat *info);
};

typedef std::map<std::string, SyncItem> SyncItemList;

}  // namespace publish

#endif  // CVMFS_SYNC_ITEM_H_
