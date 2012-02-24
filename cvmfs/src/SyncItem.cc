#include "SyncItem.h"

#include <sstream>
#include <errno.h>

#include "SyncMediator.h"

using namespace std;

namespace cvmfs {

SyncItem::SyncItem(const string &dirPath,
                   const string &filename,
                   const SyncItemType entryType,
                   const SyncUnion *syncUnionEngine) :
  mType(entryType),
  mWhiteout(false),
  mRelativeParentPath(dirPath),
  mFilename(filename),
  mUnionEngine(syncUnionEngine) {}

SyncItem::~SyncItem() {
}

bool SyncItem::IsNew() const {
	StatRepository();
	return (mRepositoryStat.errorCode == ENOENT);
}

void SyncItem::MarkAsWhiteout(const std::string &actual_filename) {
	// mark the file as whiteout entry and strip the whiteout prefix
	mWhiteout = true;
	mFilename = actual_filename;

	// find the entry in the repository
	StatRepository();
	if (mRepositoryStat.errorCode != 0) {
		stringstream ss;
		ss << "'" << GetRelativePath() << "' should be deleted, but was not found in repository.";
		printWarning(ss.str());
		return;
	}

	// what are we actually deleting?
	if (S_ISDIR(mRepositoryStat.stat.st_mode)) mType = DE_DIR;
	else if (S_ISREG(mRepositoryStat.stat.st_mode)) mType = DE_FILE;
	else if (S_ISLNK(mRepositoryStat.stat.st_mode)) mType = DE_SYMLINK;
}

unsigned int SyncItem::GetUnionLinkcount() const {
	StatUnion();
	return mUnionStat.stat.st_nlink;
}

uint64_t SyncItem::GetUnionInode() const {
	StatUnion();
	return mUnionStat.stat.st_ino;
}

void SyncItem::StatGeneric(const string &path, EntryStat *statStructure) const {
	if (platform_lstat(path.c_str(), &statStructure->stat) != 0) {
		statStructure->errorCode = errno;
	}
	statStructure->obtained = true;
}

bool SyncItem::IsOpaqueDirectory() const {
	if (not IsDirectory()) {
		return false;
	}

	return mUnionEngine->IsOpaqueDirectory(this);
}

DirectoryEntry SyncItem::CreateDirectoryEntry() const {
  DirectoryEntry dEntry;
  dEntry.inode_             = DirectoryEntry::kInvalidInode; // inode is determined at runtime of client
  dEntry.parent_inode_      = DirectoryEntry::kInvalidInode; // ... dito
  dEntry.mode_              = this->GetUnionStat().st_mode;
  dEntry.size_              = this->GetUnionStat().st_size;
  dEntry.mtime_             = this->GetUnionStat().st_mtime;
  dEntry.checksum_          = this->GetContentHash();
  dEntry.name_              = this->GetFilename();

  if (this->IsSymlink()) {
    char slnk[PATH_MAX+1];
		ssize_t l = readlink((this->GetUnionPath()).c_str(), slnk, PATH_MAX);
		if (l >= 0) {
			slnk[l] = '\0';
			dEntry.symlink_ = slnk;
		}
  }

  dEntry.linkcount_         = this->GetUnionLinkcount();

  return dEntry;
}

std::string SyncItem::GetRepositoryPath() const {
 return mUnionEngine->GetRepositoryPath() + "/" + GetRelativePath();
}

std::string SyncItem::GetUnionPath() const {
 return mUnionEngine->GetUnionPath() + "/" + GetRelativePath();
}

std::string SyncItem::GetOverlayPath() const {
 return mUnionEngine->GetOverlayPath() + "/" + GetRelativePath();
}

} // namespace cvmfs
