/**
 * This file is part of the CernVM File System.
 */

#include "sync_mediator.h"

#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cassert>

#include <sstream>
#include <iostream> // TODO: remove that!

#include "compression.h"
#include "smalloc.h"
#include "hash.h"
#include "fs_traversal.h"
#include "util.h"

using namespace std;  // NOLINT

namespace publish {


PublishFilesCallback::PublishFilesCallback(SyncMediator *mediator) {
  assert(mediator);
  mediator_ = mediator;
}


void PublishFilesCallback::Callback(const std::string &path, int retval,
                                    const std::string &digest)
{
  LogCvmfs(kLogCvmfs, kLogStdout, "callback for %s, digest %s, retval %d", path.c_str(), digest.c_str(), retval);
  if (retval != 0) {
    LogCvmfs(kLogCvmfs, kLogStdout, "FAILURE!");
    abort();
  }
  hash::Any hash(hash::kSha1, hash::HexPtr(digest));

  pthread_mutex_lock(&mediator_->lock_file_queue_);
  SyncItemList::iterator itr = mediator_->file_queue_.find(path);
  assert(itr != mediator_->file_queue_.end());
  itr->second.SetContentHash(hash);
  pthread_mutex_unlock(&mediator_->lock_file_queue_);
  mediator_->catalog_manager_->AddFile(itr->second.CreateCatalogDirent(),
                                      itr->second.relative_parent_path());
}


SyncMediator::SyncMediator(catalog::WritableCatalogManager *catalogManager,
                           const SyncParameters *params) :
  catalog_manager_(catalogManager),
  params_(params)
{
  int retval = pthread_mutex_init(&lock_file_queue_, NULL);
  assert(retval == 0);

  params->spooler->SetCallback(new PublishFilesCallback(this));
  LogCvmfs(kLogCvmfs, kLogStdout, "processing changes...");
}


/**
 * Add an entry to the repository.
 * Added directories will be traversed in order to add the complete subtree.
 */
void SyncMediator::Add(SyncItem &entry) {
  if (entry.IsDirectory()) {
		AddDirectoryRecursively(entry);
	}

	else if (entry.IsRegularFile() || entry.IsSymlink()) {
		// create a nested catalog if we find a NEW request file
		if (entry.IsCatalogMarker() && entry.IsNew()) {
			CreateNestedCatalog(entry);
		}

		// a file is a hard link if the link count is greater than 1
		if (entry.GetUnionLinkcount() > 1) {
			InsertHardlink(entry);
		} else {
			AddFile(entry);
		}
	}

	else {
		stringstream ss;
		ss << "'" << entry.GetRelativePath() << "' cannot be added. Unregcognized file format.";
		PrintWarning(ss.str());
	}
}


/**
 * Touch an entry in the repository.
 */
void SyncMediator::Touch(SyncItem &entry) {
  PrintWarning("TOUCH entry:" + entry.GetRelativePath());
	if (entry.IsDirectory()) {
		TouchDirectory(entry);
	}

	else if (entry.IsRegularFile() || entry.IsSymlink()) {
		Replace(entry);
	}

	else {
		stringstream ss;
		ss << "'" << entry.GetRelativePath() << "' cannot be touched. Unregcognized file format.";
		PrintWarning(ss.str());
	}
}


/**
 * Remove an entry from the repository. Directories will be recursively removed.
 */
void SyncMediator::Remove(SyncItem &entry) {
  PrintWarning("REMOVE entry:" + entry.GetRelativePath());
	if (entry.IsDirectory()) {
    PrintWarning("REMOVE entire directory:" + entry.GetRelativePath());
		RemoveDirectoryRecursively(entry);
	} else if (entry.IsRegularFile() || entry.IsSymlink()) {
		// first remove the file...
		RemoveFile(entry);

		// ... then the nested catalog (if needed)
		if (entry.IsCatalogMarker() && not entry.IsNew()) {
			RemoveNestedCatalog(entry);
		}
	}

	else {
		stringstream ss;
		ss << "'" << entry.GetRelativePath() << "' cannot be deleted. Unregcognized file format.";
		PrintWarning(ss.str());
	}
}


/**
 * Remove the old entry and add the new one.
 */
void SyncMediator::Replace(SyncItem &entry) {
  PrintWarning("REPLACE entry:" + entry.GetRelativePath());
	// an entry is just a representation of a filename
	// replacing it is as easy as that:
	Remove(entry);
	Add(entry);
}


void SyncMediator::EnterDirectory(SyncItem &entry) {
	HardlinkGroupMap newMap;
	hardlink_stack_.push(newMap);
}


void SyncMediator::LeaveDirectory(SyncItem &entry)
{
  CompleteHardlinks(entry);
	AddHardlinkGroups(GetHardlinkMap());
	hardlink_stack_.pop();
}


/**
 * Do any pending processing and commit all changes to the catalogs.
 * To be called after change set traversal is finished.
 */
Manifest *SyncMediator::Commit() {
  LogCvmfs(kLogCatalog, kLogStdout, "Wait for upload of files");
  while (!params_->spooler->IsIdle())
    sleep(1);
  params_->spooler->UnsetCallback();
  if (params_->spooler->num_errors() > 0) {
    LogCvmfs(kLogCatalog, kLogStderr, "failed to commit files");
    return NULL;
  }

	catalog_manager_->PrecalculateListings();
	return catalog_manager_->Commit();
}


/**** EXPERIMENTAL *****/


/*void SyncMediator::AddFileQueueToCatalogs() {
	// don't do things you could regret later on
	if (params_->dry_run) {
		return;
	}

	// add singular files
	SyncItemList::const_iterator i, iend;
	for (i = mFileQueue.begin(), iend = mFileQueue.end(); i != iend; ++i) {
		catalog_manager_->AddFile(i->second.CreateCatalogDirent(), i->second.relative_parent_path());
	}

	// add hardlink groups
	HardlinkGroupList::const_iterator j, jend;
	for (j = mHardlinkQueue.begin(), jend = mHardlinkQueue.end(); j != jend; ++j) {
    AddHardlinkGroup(*j);
	}
}*/


void SyncMediator::InsertHardlink(SyncItem &entry) {
	uint64_t inode = entry.GetUnionInode();

	// find the hard link group in the lists
	HardlinkGroupMap::iterator hardlinkGroup = GetHardlinkMap().find(inode);

	if (hardlinkGroup == GetHardlinkMap().end()) {
		// create a new hardlink group (sorry for this creepy construction :o) )
		GetHardlinkMap().insert(HardlinkGroupMap::value_type(inode, HardlinkGroup(entry)));
	} else {
		// append the file to the appropriate hardlink group
		hardlinkGroup->second.AddHardlink(entry);
	}
}

void SyncMediator::InsertExistingHardlink(SyncItem &entry) {
	// check if found file has hardlinks (nlink > 1)
	// as we are looking through all files in one directory here, there might be
	// completely untouched hardlink groups, which we can safely skip
	// finally we have to see, if the hardlink is already part of this group

	// check if we have a hard link here
	if (entry.GetUnionLinkcount() <= 1) {
		return;
	}

	uint64_t inode = entry.GetUnionInode();
	HardlinkGroupMap::iterator hlGroup;
	hlGroup = GetHardlinkMap().find(inode);

	if (hlGroup != GetHardlinkMap().end()) { // touched hardlinks in this group?
		SyncItemList::const_iterator i,end;
		bool alreadyThere = false;

		// search for the entry in this group
		for (i = hlGroup->second.hardlinks.begin(), end = hlGroup->second.hardlinks.end(); i != end; ++i) {
			if (i->second == entry) {
				alreadyThere = true;
				break;
			}
		}

		if (not alreadyThere) { // hardlink already in the group?
			// if one element of a hardlink group is edited, all elements must be replaced
			// here we remove an untouched hardlink and add it to its hardlink group for re-adding later
			Remove(entry);
			hlGroup->second.AddHardlink(entry);
		}
	}
}

// -----------------------------------------------------------------------------

void SyncMediator::CompleteHardlinks(SyncItem &entry) {
	// create a recursion engine which DOES NOT recurse into directories by default.
	// it basically goes through the current directory (in the union volume) and
	// searches for legacy hardlinks which has to be connected to the new (edited) ones

	// if no hardlink in this directory was changed, we can skip this
	if (GetHardlinkMap().size() == 0) {
		return;
	}

	// look for legacy hardlinks
	FileSystemTraversal<SyncMediator> traversal(this, union_engine_->union_path(), false);
	traversal.foundRegularFile = &SyncMediator::InsertExistingHardlinkFileCallback;
	traversal.foundSymlink = &SyncMediator::InsertExistingHardlinkSymlinkCallback;
	traversal.Recurse(entry.GetUnionPath());
}

void SyncMediator::InsertExistingHardlinkFileCallback(const std::string &parent_dir,
                                                      const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, kItemFile, union_engine_);
  InsertExistingHardlink(entry);
}

void SyncMediator::InsertExistingHardlinkSymlinkCallback(const std::string &parent_dir,
                                                         const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, kItemSymlink, union_engine_);
  InsertExistingHardlink(entry);
}

// -----------------------------------------------------------------------------

void SyncMediator::AddDirectoryRecursively(SyncItem &entry) {
	AddDirectory(entry);

	// create a recursion engine, which recursively adds all entries in a newly created directory
	FileSystemTraversal<SyncMediator> traversal(this, union_engine_->scratch_path(), true, union_engine_->GetIgnoreFilenames());
	traversal.enteringDirectory = &SyncMediator::EnterAddedDirectoryCallback;
	traversal.leavingDirectory = &SyncMediator::LeaveAddedDirectoryCallback;
	traversal.foundRegularFile = &SyncMediator::AddFileCallback;
	traversal.foundDirectory = &SyncMediator::AddDirectoryCallback;
	traversal.foundSymlink = &SyncMediator::AddSymlinkCallback;
	traversal.Recurse(entry.GetScratchPath());
}

bool SyncMediator::AddDirectoryCallback(const std::string &parent_dir,
                                        const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, kItemDir, union_engine_);
	AddDirectory(entry);
	return true; // <-- tells the recursion engine to recurse further into this directory
}

void SyncMediator::AddFileCallback(const std::string &parent_dir,
                                   const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, kItemFile, union_engine_);
  Add(entry);
}

void SyncMediator::AddSymlinkCallback(const std::string &parent_dir,
                                      const std::string &link_name) {
  SyncItem entry(parent_dir, link_name, kItemSymlink, union_engine_);
  Add(entry);
}

void SyncMediator::EnterAddedDirectoryCallback(const std::string &parent_dir,
                                               const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, kItemDir, union_engine_);
  EnterDirectory(entry);
}

void SyncMediator::LeaveAddedDirectoryCallback(const std::string &parent_dir,
                                               const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, kItemDir, union_engine_);
  LeaveDirectory(entry);
}

// -----------------------------------------------------------------------------

void SyncMediator::RemoveDirectoryRecursively(SyncItem &entry) {
	// delete a directory AFTER it was emptied here,
	// because it would start up another recursion

	FileSystemTraversal<SyncMediator> traversal(this, union_engine_->rdonly_path());
	traversal.foundRegularFile = &SyncMediator::RemoveFileCallback;
	traversal.foundDirectoryAfterRecursion = &SyncMediator::RemoveDirectoryCallback;
	traversal.foundSymlink = &SyncMediator::RemoveSymlinkCallback;
	traversal.Recurse(entry.GetRdOnlyPath());

	// the given directory was emptied recursively and can now itself be deleted
	RemoveDirectory(entry);
}

void SyncMediator::RemoveFileCallback(const std::string &parent_dir,
                                      const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, kItemFile, union_engine_);
  Remove(entry);
}

void SyncMediator::RemoveSymlinkCallback(const std::string &parent_dir,
                                         const std::string &link_name) {
  SyncItem entry(parent_dir, link_name, kItemSymlink, union_engine_);
  Remove(entry);
}

void SyncMediator::RemoveDirectoryCallback(const std::string &parent_dir,
                                           const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, kItemDir, union_engine_);
  RemoveDirectory(entry);
}

// -----------------------------------------------------------------------------

void SyncMediator::CreateNestedCatalog(SyncItem &requestFile) {
	if (params_->print_changeset) cout << "[add] NESTED CATALOG" << endl;
	if (not params_->dry_run)     catalog_manager_->CreateNestedCatalog(requestFile.relative_parent_path());
}

void SyncMediator::RemoveNestedCatalog(SyncItem &requestFile) {
	if (params_->print_changeset) cout << "[rem] NESTED CATALOG" << endl;
	if (not params_->dry_run)     catalog_manager_->RemoveNestedCatalog(requestFile.relative_parent_path());
}

void SyncMediator::AddFile(SyncItem &entry) {
	if (params_->print_changeset) cout << "[add] " << entry.GetRdOnlyPath() << endl;

	if (entry.IsSymlink() && not params_->dry_run) {
	  // symlinks have no 'actual' file content, which would have to be compressed...
		catalog_manager_->AddFile(entry.CreateCatalogDirent(), entry.relative_parent_path());
	} else {
	  // Push the file in the queue for later post-processing
    pthread_mutex_lock(&lock_file_queue_);
		file_queue_[entry.GetUnionPath()] = entry;
    pthread_mutex_unlock(&lock_file_queue_);
    // Spool the file
    params_->spooler->SpoolProcess(
      params_->dir_union + "/" + entry.GetRelativePath(), "data", "");
	}
}

void SyncMediator::RemoveFile(SyncItem &entry) {
	if (params_->print_changeset) cout << "[rem] " << entry.GetRdOnlyPath() << endl;
	if (not params_->dry_run)     catalog_manager_->RemoveFile(entry.GetRelativePath());
}

void SyncMediator::TouchFile(SyncItem &entry) {
	if (params_->print_changeset) cout << "[tou] " << entry.GetRdOnlyPath() << endl;
	if (not params_->dry_run)     catalog_manager_->TouchFile(entry.CreateCatalogDirent(),
	                                                entry.GetRelativePath());
}

void SyncMediator::AddDirectory(SyncItem &entry) {
	if (params_->print_changeset) cout << "[add] " << entry.GetRdOnlyPath() << endl;
	if (not params_->dry_run)     catalog_manager_->AddDirectory(entry.CreateCatalogDirent(),
	                                                   entry.relative_parent_path());
}

void SyncMediator::RemoveDirectory(SyncItem &entry) {
	if (params_->print_changeset) cout << "[rem] " << entry.GetRdOnlyPath() << endl;
	if (not params_->dry_run)     catalog_manager_->RemoveDirectory(entry.GetRelativePath());
}

void SyncMediator::TouchDirectory(SyncItem &entry) {
	if (params_->print_changeset) cout << "[tou] " << entry.GetRdOnlyPath() << endl;
	if (not params_->dry_run)     catalog_manager_->TouchDirectory(entry.CreateCatalogDirent(),
	                                                     entry.GetRelativePath());
}

void SyncMediator::AddHardlinkGroups(const HardlinkGroupMap &hardlinks) {
  HardlinkGroupMap::const_iterator i,end;
	for (i = hardlinks.begin(), end = hardlinks.end(); i != end; ++i) {
	   // currentHardlinkGroup = i->second; --> just to remind you

		if (params_->print_changeset) {
			cout << "[add] hardlink group around: " << i->second.master.GetRdOnlyPath() << "( ";
			SyncItemList::const_iterator j,jend;
			for (j = i->second.hardlinks.begin(), jend = i->second.hardlinks.end(); j != jend; ++j) {
				cout << j->second.filename() << " ";
			}
			cout << ")" << endl;
		}

		if (i->second.master.IsSymlink() && not params_->dry_run) {
		  // symlink hardlinks just end up in the database
		  // (see SyncMediator::addFile() same semantics here)
      AddHardlinkGroup(i->second);

		} else {
		  // yeah... just see SyncMediator::addFile()
			mHardlinkQueue.push_back(i->second);
		}
	}
}

void SyncMediator::AddHardlinkGroup(const HardlinkGroup &group) {
  // create a DirectoryEntry list out of the hardlinks
  catalog::DirectoryEntryList hardlinks;
  SyncItemList::const_iterator k, kend;
  for (k    = group.hardlinks.begin(),
       kend = group.hardlinks.end(); k != kend; ++k) {
    // EXP
    //hardlinks[k->CreateDirectoryEntry().GetRelativePath()] = k->CreateDirectoryEntry();
  }
	catalog_manager_->AddHardlinkGroup(hardlinks, group.master.relative_parent_path());
}

}  // namespace publish
