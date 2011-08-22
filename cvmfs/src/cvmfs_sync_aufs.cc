#include "cvmfs_sync_aufs.h"

#include "util.h"
#include "cvmfs_sync_recursion.h"

#include <iostream> // remove later
#include <dirent.h>
#include <stdio.h>
#include <unistd.h>
#include <sstream>

using namespace cvmfs;
using namespace std;

UnionFilesystemSync* UnionFilesystemSync::mInstance = NULL;

UnionFilesystemSync* UnionFilesystemSync::sharedInstance() {
	if (mInstance == NULL) {
		UnionFilesystemSync::printError("Sync framework not initialized");
		exit(1);
	}
	
	return mInstance;
}

void SyncAufs1::initialize(const std::string &repositoryPath, const std::string &unionPath, const std::string &aufsPath) {
	if (mInstance != NULL) {
		UnionFilesystemSync::printError("Sync framework initialized twice");
		exit(1);
	}
	
	mInstance = new SyncAufs1(repositoryPath, unionPath, aufsPath);
}

SyncAufs1::SyncAufs1(const string &repositoryPath, const std::string &unionPath, const std::string &aufsPath) :
	UnionFilesystemSync(repositoryPath, unionPath, aufsPath){
	// init ignored filenames
	mIgnoredFilenames.insert(".wh..wh..tmp");
	mIgnoredFilenames.insert(".wh..wh.plnk");
	mIgnoredFilenames.insert(".wh..wh.aufs");
	
	// set the whiteout prefix AUFS preceeds for every whiteout file
	mWhiteoutPrefix = ".wh.";
}

SyncAufs1::~SyncAufs1() {
	
}

bool SyncAufs1::goGetIt() {
	RecursionEngine<SyncAufs1> recursion(this, mOverlayPath);
	
	recursion.foundRegularFile = &SyncAufs1::processFoundRegularFile;
	recursion.foundDirectory = &SyncAufs1::processFoundDirectory;
	recursion.foundSymlink = &SyncAufs1::processFoundSymlink;
	recursion.caresAbout = &SyncAufs1::isInterestingFilename;
	recursion.enteringDirectory = &SyncAufs1::enteringDirectory;
	recursion.leavingDirectory = &SyncAufs1::leavingDirectory;
	
	recursion.recurse(mOverlayPath);
	
	return true;
}

bool SyncAufs1::isEditedItem(const string &dirPath, const string &filename) const {
	return false; // at the moment there is no way to distinguish between overwritten
	              // and edited files... we are assuming every file to be overwritten
	              // this breaks inode persistency, but we have to live with that atm
}
/*
void SyncAufs1::copyUpHardlinks(const std::string &dirPath, const std::string &filename) {
	// get inode of file
	uint64_t inode = statFileInUnionVolume(dirPath, filename).st_ino;
	
	// if inode already exists, we don't have to do this hassle again
	if (hardlinkGroupWithUnionInodeExists(inode)) {
		return;
	}
	
	// go through directory and search for the same inode
	string pathToDirectory = getPathToUnionFile(dirPath, "");
	DIR *dip;
	PortableDirent *dit;
	string filenameInDirectory;
	uint64_t inodeInDirectory;
	if ((dip = opendir(pathToDirectory.c_str())) == NULL) {
		return;
	}
	
	// create a hardlink group which is created at once in the end
	HardlinkGroup hardlinks;
	hardlinks.masterFile = getPathToUnionFile(dirPath, filename);
	
	while ((dit = portableReaddir(dip)) != NULL) {
		filenameInDirectory = dit->d_name;
		inodeInDirectory = statFileInUnionVolume(dirPath, filenameInDirectory).st_ino;
		
		if (inodeInDirectory == inode) {
			// the complete group of hardlinks will be replaced
			// old ones are deleted and afterwards the complete group is recreated
			if (not isNewItem(dirPath, filenameInDirectory)) {
				mMediator->deleteRegularFile(dirPath, filenameInDirectory);
			}
			hardlinks.hardlinks.push_back(getPathToUnionFile(dirPath, filenameInDirectory));
		}
	}
	
	closedir(dip);
	
	// the hardlink group is built up and will be recreated
	addHardlinkGroup(hardlinks, inode);
}
*/

bool SyncAufs1::isWhiteoutEntry(const DirEntry *entry) const {
	return (entry->getFilename().substr(0,mWhiteoutPrefix.length()) == mWhiteoutPrefix);
}

UnionFilesystemSync::UnionFilesystemSync(const string &repositoryPath, const std::string &unionPath, const string &overlayPath) {
	mRepositoryPath = canonical_path(repositoryPath);
	mUnionPath = canonical_path(unionPath);
	mOverlayPath = canonical_path(overlayPath);
	mCheckSymlinks = false;
	
	mMediator = new SyncMediator();
}

UnionFilesystemSync::~UnionFilesystemSync() {
	delete mMediator;
}

bool UnionFilesystemSync::processFoundDirectory(DirEntry *entry) {
	if (entry->isNew()) {
		mMediator->add(entry);
		return false;
	} else {
		// directory already exists... was just touched, go on with recursion -> return true
		mMediator->touch(entry);
		return true;
	}
}

void UnionFilesystemSync::processFoundRegularFile(DirEntry *entry) {
	// process whiteout prefix
	if (isWhiteoutEntry(entry)) {
		entry->markAsWhiteout();
		mMediator->remove(entry);
		
	// process normal file
	} else {
		if (entry->isNew()) {
			mMediator->add(entry);
		} else {
			mMediator->replace(entry);
		}
	} 
}

void UnionFilesystemSync::processFoundSymlink(DirEntry *entry) {
	// normally symlinks are just files:
	processFoundRegularFile(entry);
}

void UnionFilesystemSync::enteringDirectory(DirEntry *entry) {
	mMediator->enterDirectory(entry);
}

void UnionFilesystemSync::leavingDirectory(DirEntry *entry) {
	mMediator->leaveDirectory(entry);
}

void UnionFilesystemSync::printError(const string &errorMessage) {
	cerr << "ERROR: " << errorMessage << endl;
}

void UnionFilesystemSync::printWarning(const string &warningMessage) {
	cerr << "Warning: " << warningMessage << endl;
}
