#ifndef SYNC_ITEM_H
#define SYNC_ITEM_H 1

#include <string>

#include "hash.h"
#include "platform.h"

#include "DirectoryEntry.h"
#include "SyncUnion.h"

namespace cvmfs {

enum SyncItemType {
	DE_DIR,
	DE_FILE,
	DE_SYMLINK
};

/**
 *  any directory entry emitted by the RecursionEngine is wrapped in a
 *  convenient SyncItem structure
 *  This class represents potentially three concrete files:
 *    - <repository path>/<filename>
 *    - <read-write branch>/<filename>
 *    - <union volume path>/<filename>
 *  The main purpose of this class is to cache stat calls to the underlying
 *  files in the different locations as well as hiding some implementation details
 */
class SyncItem {
 private:
	SyncItemType mType;

	/**
	 *  structure to cache stat calls to the different file locations
	 *  obtained: false at the beginning, after first stat call it is true
	 *  errorCode: the errno value after the stat call responded != 0
	 *  stat: the actual stat structure
	 */
	struct EntryStat {
	  EntryStat() :
	    obtained(false),
	    errorCode(0) {}

		bool obtained;
		int errorCode;
		platform_stat64 stat;
	};

	mutable EntryStat mRepositoryStat;
	mutable EntryStat mUnionStat;
	mutable EntryStat mOverlayStat;

	bool mWhiteout;

	std::string mRelativeParentPath;
	std::string mFilename;

	/** the hash of the file's content (computed by the SyncMediator) */
	hash::t_sha1 mContentHash;

  const SyncUnion *mUnionEngine;

 public:
	/**
	 *  create a new SyncItem (is normally not required for normal usage
	 *                         as the RecursionEngine provides you with DirEntries)
	 *  @param dirPath the RELATIVE path to the file
	 *  @param filename the name of the file ;-)
	 *  @param entryType well...
	 */
	SyncItem(const std::string &dirPath,
	         const std::string &filename,
	         const SyncItemType entryType,
	         const SyncUnion *syncUnionEngine);
	virtual ~SyncItem();

	inline bool IsDirectory() const { return mType == DE_DIR; }
	inline bool IsRegularFile() const { return mType == DE_FILE; }
	inline bool IsSymlink() const { return mType == DE_SYMLINK; }
	inline bool IsWhiteout() const { return mWhiteout; }
	inline bool IsCatalogRequestFile() const { return mFilename == ".cvmfscatalog"; }
	bool IsOpaqueDirectory() const;

	inline hash::t_sha1 GetContentHash() const { return mContentHash; }
	inline void SetContentHash(hash::t_sha1 &hash) { mContentHash = hash; }
	inline bool HasContentHash() { return mContentHash != hash::t_sha1(); }

	inline std::string GetFilename() const { return mFilename; }
	inline std::string GetParentPath() const { return mRelativeParentPath; }
  DirectoryEntry CreateDirectoryEntry() const;

	inline std::string GetRelativePath() const { return (mRelativeParentPath.empty()) ? mFilename : mRelativeParentPath + "/" + mFilename; }

  std::string GetRepositoryPath() const;
  std::string GetUnionPath() const;
  std::string GetOverlayPath() const;

  void MarkAsWhiteout(const std::string &actual_filename);

	unsigned int GetUnionLinkcount() const;
	uint64_t GetUnionInode() const;
	inline platform_stat64 GetUnionStat() const { StatUnion(); return mUnionStat.stat; };
	bool IsNew() const;

  bool operator==(const SyncItem &otherEntry) const { return (GetRelativePath() == otherEntry.GetRelativePath()); }

 private:
	// lazy evaluation and caching of results of file stats
	inline void StatRepository() const { if (mRepositoryStat.obtained) return; StatGeneric(GetRepositoryPath(), &mRepositoryStat); }
	inline void StatUnion() const { if (mUnionStat.obtained) return; StatGeneric(GetUnionPath(), &mUnionStat); }
	inline void StatOverlay() const { if (mOverlayStat.obtained) return; StatGeneric(GetOverlayPath(), &mOverlayStat); }
	void StatGeneric(const std::string &path, EntryStat *statStructure) const;
};

typedef std::list<SyncItem> SyncItemList;

} // namespace cvmfs

#endif /* SYNC_ITEM_H */
