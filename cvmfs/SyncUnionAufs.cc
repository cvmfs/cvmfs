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

SyncUnionAufs::SyncUnionAufs(SyncMediator *mediator,
                             const std::string &repository_path,
                             const std::string &union_path,
                             const std::string &overlay_path) :
	SyncUnion(mediator, repository_path, union_path, overlay_path) {

	// init ignored filenames
	mIgnoredFilenames.insert(".wh..wh..tmp");
	mIgnoredFilenames.insert(".wh..wh.plnk");
	mIgnoredFilenames.insert(".wh..wh.aufs");
	mIgnoredFilenames.insert(".wh..wh..opq");

	// set the whiteout prefix AUFS preceeds for every whiteout file
	mWhiteoutPrefix = ".wh.";
}

bool SyncUnionAufs::DoYourMagic() {
	RecursionEngine<SyncUnionAufs> recursion(this,
	                                         mOverlayPath,
	                                         true,
	                                         mIgnoredFilenames);

	recursion.foundRegularFile = &SyncUnionAufs::ProcessFoundRegularFile;
	recursion.foundDirectory = &SyncUnionAufs::ProcessFoundDirectory;
	recursion.foundSymlink = &SyncUnionAufs::ProcessFoundSymlink;
	recursion.enteringDirectory = &SyncUnionAufs::EnteringDirectory;
	recursion.leavingDirectory = &SyncUnionAufs::LeavingDirectory;

	recursion.Recurse(mOverlayPath);

	mMediator->Commit();

	return true;
}
