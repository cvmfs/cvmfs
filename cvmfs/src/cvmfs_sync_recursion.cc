#include "cvmfs_sync_recursion.h"
#include "cvmfs_sync_aufs.h"

#include <dirent.h>
#include <errno.h>
#include <sstream>

#include <iostream> // TODO: remove later!

using namespace std;
using namespace cvmfs;

SyncItem::SyncItem(const string &dirPath, const string &filename, const SyncItemType entryType) :
  mType(entryType),
  mWhiteout(false),
  mRelativeParentPath(dirPath),
  mFilename(filename)
{
	// init stat structures
	mRepositoryStat.obtained = false;
	mRepositoryStat.errorCode = 0;
	mUnionStat.obtained = false;
	mUnionStat.errorCode = 0;
	mOverlayStat.obtained = false;
	mOverlayStat.errorCode = 0;
}

SyncItem::~SyncItem() {
}

bool SyncItem::isNew() {
	statRepository();
	return (mRepositoryStat.errorCode == ENOENT);
}

void SyncItem::markAsWhiteout() {
	// mark the file as whiteout entry and strip the whiteout prefix
	mWhiteout = true;
	mFilename = UnionSync::sharedInstance()->unwindWhiteoutFilename(getFilename());

	// find the entry in the repository
	statRepository();
	if (mRepositoryStat.errorCode != 0) {
		stringstream ss;
		ss << "'" << getRelativePath() << "' should be deleted, but was not found in repository.";
		printWarning(ss.str());
		return;
	}

	// what are we actually deleting?
	if (S_ISDIR(mRepositoryStat.stat.st_mode)) mType = DE_DIR;
	else if (S_ISREG(mRepositoryStat.stat.st_mode)) mType = DE_FILE;
	else if (S_ISLNK(mRepositoryStat.stat.st_mode)) mType = DE_SYMLINK;
}

unsigned int SyncItem::getUnionLinkcount() {
	statUnion();
	return mUnionStat.stat.st_nlink;
}

uint64_t SyncItem::getUnionInode() {
	statUnion();
	return mUnionStat.stat.st_ino;
}

void SyncItem::statGeneric(const string &path, EntryStat *statStructure) {
	if (portableLinkStat64(path.c_str(), &statStructure->stat) != 0) {
		statStructure->errorCode = errno;
	}
	statStructure->obtained = true;
}

string SyncItem::getRepositoryPath() const {
	return UnionSync::sharedInstance()->getRepositoryPath() + "/" + getRelativePath();
}

string SyncItem::getUnionPath() const {
	return UnionSync::sharedInstance()->getUnionPath() + "/" + getRelativePath();
}

string SyncItem::getOverlayPath() const {
	return UnionSync::sharedInstance()->getOverlayPath() + "/" + getRelativePath();
}

bool SyncItem::isOpaqueDirectory() const {
	if (!isDirectory()) {
		return false;
	}
	
	return UnionSync::sharedInstance()->isOpaqueDirectory(this);
}
