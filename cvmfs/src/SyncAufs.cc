#include "SyncUnionAufs.h"

#include "util.h"
#include "cvmfs_sync_recursion.h"
/*
#include <iostream> // remove later
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sstream>
*/

using namespace cvmfs;
using namespace std;

SyncAufs::SyncAufs(SyncMediator *mediator, const SyncParameters *parameters) :
	UnionSync(mediator, parameters) {
		
	// init ignored filenames
	mIgnoredFilenames.insert(".wh..wh..tmp");
	mIgnoredFilenames.insert(".wh..wh.plnk");
	mIgnoredFilenames.insert(".wh..wh.aufs");
	mIgnoredFilenames.insert(".wh..wh..opq");
	
	// set the whiteout prefix AUFS preceeds for every whiteout file
	mWhiteoutPrefix = ".wh.";
}

bool SyncAufs::DoYourMagic() {
	RecursionEngine<SyncAufs> recursion(this, mOverlayPath, mIgnoredFilenames);
	
	recursion.foundRegularFile = &SyncAufs::processFoundRegularFile;
	recursion.foundDirectory = &SyncAufs::processFoundDirectory;
	recursion.foundSymlink = &SyncAufs::processFoundSymlink;
	recursion.enteringDirectory = &SyncAufs::enteringDirectory;
	recursion.leavingDirectory = &SyncAufs::leavingDirectory;
	
	recursion.recurse(mOverlayPath);
	
	mMediator->commit();
	
	return true;
}
