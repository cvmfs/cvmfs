/**
 * This file is part of the CernVM File System
 */

#include "sync_union.h"

#include "util.h"
#include "SyncMediator.h"
#include "SyncItem.h"

using namespace std;

namespace publish {

SyncUnion::SyncUnion(SyncMediator *mediator,
                     const std::string &rdonly_path,
                     const std::string &union_path,
                     const std::string &scratch_path) :
  rdonly_path_(rdonly_path),
  scratch_path_(scratch_path),
  union_path_(union_path),
  mediator_(mediator)
{
  mediator_->RegisterSyncUnionEngine(this);
}


bool SyncUnion::ProcessDirectory(const string &parent_dir,
                                 const string &dir_name)
{
  SyncItem entry(parent_dir, dir_name, DE_DIR, this);

	if (entry.IsNew()) {
		mediator_->Add(entry);
		return false; // <-- recursion will stop here... all content of new directory
		              //     will be added later on by the SyncMediator

	} else { // directory already existed...
		if (entry.IsOpaqueDirectory()) { // was directory completely overwritten?
			mediator_->Replace(entry);
			return false; // <-- replace does not need any further recursion

		} else { // directory was just changed internally... only touch needed
			mediator_->Touch(entry);
			return true;
		}
	}
}


void SyncUnion::ProcessRegularFile(const string &parent_dir,
                                   const string &filename)
{
  SyncItem entry(parent_dir, filename, DE_FILE, this);
  ProcessFile(entry);
}


void SyncUnion::ProcessSymlink(const string &parent_dir,
                               const string &link_name)
{
  SyncItem entry(parent_dir, link_name, DE_SYMLINK, this);
	ProcessFile(entry);
}


void SyncUnion::ProcessFile(SyncItem &entry) {
	// Process whiteout prefix
	if (IsWhiteoutEntry(entry)) {
    string actual_filename = UnwindWhiteoutFilename(entry.GetFilename());
		entry.MarkAsWhiteout(actual_filename);
		mediator_->Remove(entry);

	// Process normal file
	} else {
		if (entry.IsNew()) {
			mediator_->Add(entry);
		} else {
			mediator_->Replace(entry);
		}
	}
}


void SyncUnion::EnterDirectory(const string &parent_dir,
                               const string &dir_name)
{
  SyncItem entry(parent_dir, dir_name, DE_DIR, this);
  mediator_->EnterDirectory(entry);
}


void SyncUnion::LeaveDirectory(const string &parent_dir,
                               const string &dir_name)
{
  SyncItem entry(parent_dir, dir_name, DE_DIR, this);
	mediator_->LeaveDirectory(entry);
}

}  // namespace sync
