#include "SyncUnionAufs.h"

#include "util.h"
#include "fs_traversal.h"
/*
#include <iostream> // remove later
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sstream>
*/

using namespace std;  // NOLINT

namespace publish {

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

bool SyncUnionAufs::Traverse() {
	FileSystemTraversal<SyncUnionAufs> traversal(this,
	                                         scratch_path(),
	                                         true,
	                                         mIgnoredFilenames);

	traversal.foundRegularFile = &SyncUnionAufs::ProcessRegularFile;
	traversal.foundDirectory = &SyncUnionAufs::ProcessDirectory;
	traversal.foundSymlink = &SyncUnionAufs::ProcessSymlink;
	traversal.enteringDirectory = &SyncUnionAufs::EnterDirectory;
	traversal.leavingDirectory = &SyncUnionAufs::LeaveDirectory;

	traversal.Recurse(scratch_path());

	mediator_->Commit();

	return true;
}

}  // namespace sync
