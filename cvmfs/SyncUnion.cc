#include "SyncUnion.h"

#include "util.h"
#include "cvmfs_sync_recursion.h"

#include "SyncMediator.h"
#include "SyncItem.h"

/*
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sstream>
*/

using namespace cvmfs;
using namespace std;

SyncUnion::SyncUnion(SyncMediator *mediator,
                     const std::string &repository_path,
                     const std::string &union_path,
                     const std::string &overlay_path) :
  mRepositoryPath(repository_path),
  mOverlayPath(overlay_path),
  mUnionPath(union_path),
  mMediator(mediator) {
    mMediator->RegisterSyncUnionEngine(this);
  }

bool SyncUnion::ProcessFoundDirectory(const std::string &parent_dir,
                                      const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, DE_DIR, this);

	if (entry.IsNew()) {
		mMediator->Add(entry);
		return false; // <-- recursion will stop here... all content of new directory
		              //     will be added later on by the SyncMediator

	} else { // directory already existed...

		if (entry.IsOpaqueDirectory()) { // was directory completely overwritten?
			mMediator->Replace(entry);
			return false; // <-- replace does not need any further recursion

		} else { // directory was just changed internally... only touch needed
			mMediator->Touch(entry);
			return true;
		}
	}
}

void SyncUnion::ProcessFoundRegularFile(const std::string &parent_dir,
                                        const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, DE_FILE, this);
  ProcessFoundFile(entry);
}

void SyncUnion::ProcessFoundSymlink(const std::string &parent_dir,
                                    const std::string &link_name) {
  SyncItem entry(parent_dir, link_name, DE_SYMLINK, this);
	ProcessFoundFile(entry);
}

void SyncUnion::ProcessFoundFile(SyncItem &entry) {
	// process whiteout prefix
	if (IsWhiteoutEntry(entry)) {
    string actual_filename = UnwindWhiteoutFilename(entry.GetFilename());
		entry.MarkAsWhiteout(actual_filename);
		mMediator->Remove(entry);

	// process normal file
	} else {
		if (entry.IsNew()) {
			mMediator->Add(entry);

		} else {
			mMediator->Replace(entry);
		}
	}
}

void SyncUnion::EnteringDirectory(const std::string &parent_dir,
                                  const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, DE_DIR, this);
  mMediator->EnterDirectory(entry);
}

void SyncUnion::LeavingDirectory(const std::string &parent_dir,
                                 const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, DE_DIR, this);
	mMediator->LeaveDirectory(entry);
}
