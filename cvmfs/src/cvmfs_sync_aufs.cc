#include "cvmfs_sync_aufs.h"

#include "util.h"
#include "cvmfs_sync_recursion.h"

#include <iostream> // remove later
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sstream>

using namespace cvmfs;
using namespace std;

UnionSync* UnionSync::mInstance = NULL;

UnionSync* UnionSync::sharedInstance() {
	if (mInstance == NULL) {
		printError("Sync framework not initialized");
		exit(1);
	}
	
	return mInstance;
}

void SyncAufs1::initialize(SyncMediator *mediator, const SyncParameters *parameters) {
	if (mInstance != NULL) {
		printError("Sync framework initialized twice");
		exit(1);
	}
	
	mInstance = new SyncAufs1(mediator, parameters);
}

SyncAufs1::SyncAufs1(SyncMediator *mediator, const SyncParameters *parameters) :
	UnionSync(mediator, parameters) {
		
	// init ignored filenames
	mIgnoredFilenames.insert(".wh..wh..tmp");
	mIgnoredFilenames.insert(".wh..wh.plnk");
	mIgnoredFilenames.insert(".wh..wh.aufs");
	mIgnoredFilenames.insert(".wh..wh..opq");
	
	// set the whiteout prefix AUFS preceeds for every whiteout file
	mWhiteoutPrefix = ".wh.";
}

SyncAufs1::~SyncAufs1() {
	
}

bool SyncAufs1::doYourMagic() {
	RecursionEngine<SyncAufs1> recursion(this, mOverlayPath, mIgnoredFilenames);
	
	recursion.foundRegularFile = &SyncAufs1::processFoundRegularFile;
	recursion.foundDirectory = &SyncAufs1::processFoundDirectory;
	recursion.foundSymlink = &SyncAufs1::processFoundSymlink;
	recursion.enteringDirectory = &SyncAufs1::enteringDirectory;
	recursion.leavingDirectory = &SyncAufs1::leavingDirectory;
	
	recursion.recurse(mOverlayPath);
	
	mMediator->commit();
	
	return true;
}

UnionSync::UnionSync(SyncMediator *mediator, const SyncParameters *parameters) {
	mRepositoryPath = canonical_path(parameters->dir_cvmfs);
	mUnionPath      = canonical_path(parameters->dir_shadow);
	mOverlayPath    = canonical_path(parameters->dir_overlay);
	
	mMediator = mediator;
}

UnionSync::~UnionSync() {
}

void UnionSync::fini() {
	delete UnionSync::sharedInstance();
	mInstance = NULL;
}

RecursionPolicy UnionSync::processFoundDirectory(SyncItem *entry) {
	if (entry->isNew()) {
		mMediator->add(entry);
		return RP_DONT_RECURSE;
	} else {
		// directory already exists... 
		if (entry->isOpaqueDirectory()) { // was directory completely overwritten?
			mMediator->replace(entry);
			return RP_DONT_RECURSE;
		} else {                          // no, all okay... just touch it and recurse into it
			mMediator->touch(entry);
			return RP_RECURSE;
		}
	}
}

void UnionSync::processFoundRegularFile(SyncItem *entry) {
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

void UnionSync::processFoundSymlink(SyncItem *entry) {
	// symlinks are just files in this sense:
	processFoundRegularFile(entry);
}

void UnionSync::enteringDirectory(SyncItem *entry) {
	mMediator->enterDirectory(entry);
}

void UnionSync::leavingDirectory(SyncItem *entry) {
	mMediator->leaveDirectory(entry);
}
