#include "cvmfs_sync_recursion.h"

#include <dirent.h>
#include <errno.h>
#include <sstream>

#include <iostream> // TODO: remove later!

using namespace std;
using namespace cvmfs;

DirEntry::DirEntry(const string &dirPath, const string &filename, const DirEntryType entryType) {
	mRelativeParentPath = dirPath;
	mFilename = filename;
	mType = entryType;
	mWhiteout = false;

	// init stat structures
	mRepositoryStat.obtained = false;
	mRepositoryStat.errorCode = 0;
	mUnionStat.obtained = false;
	mUnionStat.errorCode = 0;
	mOverlayStat.obtained = false;
	mOverlayStat.errorCode = 0;
}

DirEntry::~DirEntry() {}

bool DirEntry::isNew() {
	statRepository();
	return (mRepositoryStat.errorCode == ENOENT);
}

void DirEntry::markAsWhiteout() {
	// mark the file as whiteout entry and strip the whiteout prefix
	mWhiteout = true;
	mFilename = mFilename.substr(UnionFilesystemSync::sharedInstance()->getWhiteoutPrefix().length());

	// find the entry in the repository
	statRepository();
	if (mRepositoryStat.errorCode != 0) {
		stringstream ss;
		ss << "'" << getRelativePath() << "' should be deleted, but was not found in repository.";
		UnionFilesystemSync::sharedInstance()->printWarning(ss.str());
		return;
	}

	// what are we actually deleting?
	if (S_ISDIR(mRepositoryStat.stat.st_mode)) mType = DE_DIR;
	else if (S_ISREG(mRepositoryStat.stat.st_mode)) mType = DE_FILE;
	else if (S_ISLNK(mRepositoryStat.stat.st_mode)) mType = DE_SYMLINK;
}

unsigned int DirEntry::getUnionLinkcount() {
	statUnion();
	return mUnionStat.stat.st_nlink;
}

uint64_t DirEntry::getUnionInode() {
	statUnion();
	return mUnionStat.stat.st_ino;
}

void DirEntry::statGeneric(const string &path, EntryStat *statStructure) {
	if (portableLinkStat64(path.c_str(), &statStructure->stat) != 0) {
		statStructure->errorCode = errno;
	}
	statStructure->obtained = true;
}
