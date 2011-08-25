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
		printError("Sync framework not initialized");
		exit(1);
	}
	
	return mInstance;
}

void SyncAufs1::initialize(const std::string &repositoryPath, const std::string &unionPath, const std::string &aufsPath, SyncMediator *mediator) {
	if (mInstance != NULL) {
		printError("Sync framework initialized twice");
		exit(1);
	}
	
	mInstance = new SyncAufs1(repositoryPath, unionPath, aufsPath, mediator);
}

SyncAufs1::SyncAufs1(const string &repositoryPath, const std::string &unionPath, const std::string &aufsPath, SyncMediator *mediator) :
	UnionFilesystemSync(repositoryPath, unionPath, aufsPath, mediator) {
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
	
	mMediator->commit();
	
	return true;
}

bool SyncAufs1::isEditedItem(const string &dirPath, const string &filename) const {
	return false; // at the moment there is no way to distinguish between overwritten
	              // and edited files... we are assuming every file to be overwritten
	              // this breaks inode persistency, but we have to live with that atm
}

bool SyncAufs1::isWhiteoutEntry(const DirEntry *entry) const {
	return (entry->getFilename().substr(0,mWhiteoutPrefix.length()) == mWhiteoutPrefix);
}

UnionFilesystemSync::UnionFilesystemSync(const string &repositoryPath, const std::string &unionPath, const string &overlayPath, SyncMediator *mediator) {
	mRepositoryPath = canonical_path(repositoryPath);
	mUnionPath = canonical_path(unionPath);
	mOverlayPath = canonical_path(overlayPath);
	mCheckSymlinks = false;
	
	mMediator = mediator;
}

UnionFilesystemSync::~UnionFilesystemSync() {
}

void UnionFilesystemSync::fini() {
	delete UnionFilesystemSync::sharedInstance();
	mInstance = NULL;
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
