#include "SyncMediator.h"

#include <sstream>
#include <iostream> // TODO: remove that!

extern "C" {
   #include "compression.h"
   #include "smalloc.h"
}

#include "hash.h"
#include "cvmfs_sync_recursion.h"

using namespace cvmfs;
using namespace std;

SyncMediator::SyncMediator(WritableCatalogManager *catalogManager,
                           const string &data_directory,
                           const bool dry_run,
                           const bool print_changeset) :
  mCatalogManager(catalogManager),
  mDataDirectory(data_directory),
  mDryRun(dry_run),
  mPrintChangeset(print_changeset) {}

void SyncMediator::Add(SyncItem &entry) {
  if (entry.IsDirectory()) {
		AddDirectoryRecursively(entry);
	}
	
	else if (entry.IsRegularFile() || entry.IsSymlink()) {

		// create a nested catalog if we find a NEW request file
		if (entry.IsCatalogRequestFile() && entry.IsNew()) {
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
		printWarning(ss.str());
	}
}

void SyncMediator::Touch(SyncItem &entry) {
	if (entry.IsDirectory()) {
		TouchDirectory(entry);
	}
	
	else if (entry.IsRegularFile() || entry.IsSymlink()) {
		Replace(entry);
	}

	else {
		stringstream ss;
		ss << "'" << entry.GetRelativePath() << "' cannot be touched. Unregcognized file format.";
		printWarning(ss.str());
	}
}

void SyncMediator::Remove(SyncItem &entry) {
	if (entry.IsDirectory()) {
		RemoveDirectoryRecursively(entry);
	}
	
	else if (entry.IsRegularFile() || entry.IsSymlink()) {
		// first remove the file...
		RemoveFile(entry);
		
		// ... then the nested catalog (if needed)
		if (entry.IsCatalogRequestFile() && not entry.IsNew()) {
			RemoveNestedCatalog(entry);
		}
	}

	else {
		stringstream ss;
		ss << "'" << entry.GetRelativePath() << "' cannot be deleted. Unregcognized file format.";
		printWarning(ss.str());
	}
}

void SyncMediator::Replace(SyncItem &entry) {
	// an entry is just a representation of a filename
	// replacing it is as easy as that:
	Remove(entry);
	Add(entry);
}

void SyncMediator::EnterDirectory(SyncItem &entry) {
	HardlinkGroupMap newMap;
	mHardlinkStack.push(newMap);
}

void SyncMediator::LeaveDirectory(SyncItem &entry, 
                                  const bool complete_hardlinks) {
	if (complete_hardlinks) {
	  CompleteHardlinks(entry);
  }
	AddHardlinkGroups(GetHardlinkMap());
	mHardlinkStack.pop();
}

void SyncMediator::Commit() {
	CompressAndHashFileQueue();
	AddFileQueueToCatalogs();
	mCatalogManager->PrecalculateListings();
	mCatalogManager->Commit();
}

void SyncMediator::CompressAndHashFileQueue() {
	// compressing and hashing files
	// TODO: parallelize this!
	SyncItemList::iterator i;
	SyncItemList::const_iterator iend;
	for (i = mFileQueue.begin(), iend = mFileQueue.end(); i != iend; ++i) {
		hash::t_sha1 hash;
		AddFileToDatastore(*i, hash);
		i->SetContentHash(hash);
	}

	// compressing and hashing files in hardlink groups
	// (hardlinks point to the same "data", therefore we only have to compress it once)
	HardlinkGroupList::iterator j;
	HardlinkGroupList::const_iterator jend;
	SyncItemList::iterator k;
	SyncItemList::const_iterator kend;
	for (j = mHardlinkQueue.begin(), jend = mHardlinkQueue.end(); j != jend; ++j) {
		// hardlinks to anything else (mostly symlinks) do not have to be compressed
		if (not j->masterFile.IsRegularFile()) {
			continue;
		}
		
		// compress the master file
		hash::t_sha1 hash;
		AddFileToDatastore(j->masterFile, hash);
		
		// distribute the obtained hash for every hardlink
		for (k = j->hardlinks.begin(), kend = j->hardlinks.end(); k != kend; ++k) {
			k->SetContentHash(hash);
		}
	}
}

void SyncMediator::AddFileQueueToCatalogs() {
	// don't do things you could regret later on
	if (mDryRun) {
		return;
	}
	
	// add singular files
	SyncItemList::const_iterator i, iend;
	for (i = mFileQueue.begin(), iend = mFileQueue.end(); i != iend; ++i) {
		mCatalogManager->AddFile(i->CreateDirectoryEntry(), i->GetParentPath());
	}
	
	// add hardlink groups
	HardlinkGroupList::const_iterator j, jend;
	for (j = mHardlinkQueue.begin(), jend = mHardlinkQueue.end(); j != jend; ++j) {
    AddHardlinkGroup(*j);
	}
}

bool SyncMediator::AddFileToDatastore(SyncItem &entry, const std::string &suffix, hash::t_sha1 &hash) {
	// don't do that, would change something!
	if (mDryRun) {
		return true;
	}
	
	bool result = false;
	/* Create temporary file */
	const string templ = mDataDirectory + "/txn/compressing.XXXXXX";
	char *tmp_path = (char *)smalloc(templ.length() + 1);
	strncpy(tmp_path, templ.c_str(), templ.length() + 1);
	int fd_dst = mkstemp(tmp_path);

	if ((fd_dst >= 0) && (fchmod(fd_dst, plain_file_mode) == 0)) {
		/* Compress and calculate SHA1 */
		FILE *fsrc = NULL, *fdst = NULL;
		if ( (fsrc = fopen(entry.GetOverlayPath().c_str(), "r")) && 
		     (fdst = fdopen(fd_dst, "w")) && 
		     (compress_file_fp_sha1(fsrc, fdst, hash.digest) == 0) )
		{
			const string sha1str = hash.to_string();
			const string cache_path = mDataDirectory + "/" + sha1str.substr(0, 2) + "/" + 
			sha1str.substr(2) + suffix;
			fflush(fdst);
			if (rename(tmp_path, cache_path.c_str()) == 0) {
				result = true;
			} else {	
				unlink(tmp_path);
				stringstream ss;
				ss << "could not rename " << tmp_path << " to " << cache_path;
				printWarning(ss.str());
			}
		} else {	
			stringstream ss;
			ss << "could not compress " << entry.GetOverlayPath();
			printWarning(ss.str());
		}
		if (fsrc) fclose(fsrc);
		if (fdst) fclose(fdst);
	} else {
		stringstream ss;
		ss << "could not create temporary file " << templ;
		printWarning(ss.str());
		result = false;
	}
	free(tmp_path);

	return result;
}

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
			if (*i == entry) {
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
	RecursionEngine<SyncMediator> recursion(this, mUnionEngine->GetUnionPath(), false);
	recursion.foundRegularFile = &SyncMediator::InsertExistingHardlinkFileCallback;
	recursion.foundSymlink = &SyncMediator::InsertExistingHardlinkSymlinkCallback;
	recursion.Recurse(entry.GetUnionPath());
}

void SyncMediator::InsertExistingHardlinkFileCallback(const std::string &parent_dir,
                                                      const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, DE_FILE, mUnionEngine);
  InsertExistingHardlink(entry);
}

void SyncMediator::InsertExistingHardlinkSymlinkCallback(const std::string &parent_dir,
                                                         const std::string &file_name) {                                   
  SyncItem entry(parent_dir, file_name, DE_SYMLINK, mUnionEngine);
  InsertExistingHardlink(entry);
}

// -----------------------------------------------------------------------------

void SyncMediator::AddDirectoryRecursively(SyncItem &entry) {
	AddDirectory(entry);
	
	// create a recursion engine, which recursively adds all entries in a newly created directory
	RecursionEngine<SyncMediator> recursion(this, mUnionEngine->GetOverlayPath(), true, mUnionEngine->GetIgnoredFilenames());
	recursion.enteringDirectory = &SyncMediator::EnterAddedDirectoryCallback;
	recursion.leavingDirectory = &SyncMediator::LeaveAddedDirectoryCallback;
	recursion.foundRegularFile = &SyncMediator::AddFileCallback;
	recursion.foundDirectory = &SyncMediator::AddDirectoryCallback;
	recursion.foundSymlink = &SyncMediator::AddSymlinkCallback;
	recursion.Recurse(entry.GetOverlayPath());
}

bool SyncMediator::AddDirectoryCallback(const std::string &parent_dir,
                                        const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, DE_DIR, mUnionEngine);
	AddDirectory(entry);
	return true; // <-- tells the recursion engine to recurse further into this directory
}

void SyncMediator::AddFileCallback(const std::string &parent_dir,
                                   const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, DE_FILE, mUnionEngine);
  Add(entry);
}

void SyncMediator::AddSymlinkCallback(const std::string &parent_dir,
                                      const std::string &link_name) {
  SyncItem entry(parent_dir, link_name, DE_SYMLINK, mUnionEngine);
  Add(entry);
}

void SyncMediator::EnterAddedDirectoryCallback(const std::string &parent_dir,
                                               const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, DE_DIR, mUnionEngine);
  EnterDirectory(entry);
}

void SyncMediator::LeaveAddedDirectoryCallback(const std::string &parent_dir,
                                               const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, DE_DIR, mUnionEngine);
  bool complete_hardlinks = false; // <-- this was a NEW directory...
                                   //     there are no 'unchanged' hardlinks
  LeaveDirectory(entry, complete_hardlinks);
}

// -----------------------------------------------------------------------------

void SyncMediator::RemoveDirectoryRecursively(SyncItem &entry) {
	// delete a directory AFTER it was emptied here,
	// because it would start up another recursion
	
	RecursionEngine<SyncMediator> recursion(this, mUnionEngine->GetRepositoryPath());
	recursion.foundRegularFile = &SyncMediator::RemoveFileCallback;
	recursion.foundDirectoryAfterRecursion = &SyncMediator::RemoveDirectoryCallback;
	recursion.foundSymlink = &SyncMediator::RemoveSymlinkCallback;
	recursion.Recurse(entry.GetRepositoryPath());
	
	// the given directory was emptied recursively and can now itself be deleted
	RemoveDirectory(entry);
}

void SyncMediator::RemoveFileCallback(const std::string &parent_dir,
                                      const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, DE_FILE, mUnionEngine);
  Remove(entry);
}

void SyncMediator::RemoveSymlinkCallback(const std::string &parent_dir,
                                         const std::string &link_name) {
  SyncItem entry(parent_dir, link_name, DE_SYMLINK, mUnionEngine);
  Remove(entry);
}
                           
void SyncMediator::RemoveDirectoryCallback(const std::string &parent_dir,
                                           const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, DE_DIR, mUnionEngine);
  RemoveDirectory(entry);
}

// -----------------------------------------------------------------------------

void SyncMediator::CreateNestedCatalog(SyncItem &requestFile) {
	if (mPrintChangeset) cout << "[add] NESTED CATALOG" << endl;
	if (not mDryRun)     mCatalogManager->CreateNestedCatalog(requestFile.GetParentPath());
}

void SyncMediator::RemoveNestedCatalog(SyncItem &requestFile) {
	if (mPrintChangeset) cout << "[rem] NESTED CATALOG" << endl;
	if (not mDryRun)     mCatalogManager->RemoveNestedCatalog(requestFile.GetParentPath());
}

void SyncMediator::AddFile(SyncItem &entry) {
	if (mPrintChangeset) cout << "[add] " << entry.GetRepositoryPath() << endl;
	
	if (entry.IsSymlink() && not mDryRun) {
	  // symlinks have no 'actual' file content, which would have to be compressed...
		mCatalogManager->AddFile(entry.CreateDirectoryEntry(), entry.GetParentPath());
	} else {
	  // push the file in the queue for later post-processing
		mFileQueue.push_back(entry);
	}
}

void SyncMediator::RemoveFile(SyncItem &entry) {
	if (mPrintChangeset) cout << "[rem] " << entry.GetRepositoryPath() << endl;
	if (not mDryRun)     mCatalogManager->RemoveFile(entry.GetRelativePath());
}

void SyncMediator::TouchFile(SyncItem &entry) {
	if (mPrintChangeset) cout << "[tou] " << entry.GetRepositoryPath() << endl;
	if (not mDryRun)     mCatalogManager->TouchFile(entry.CreateDirectoryEntry(),
	                                                entry.GetParentPath());
}

void SyncMediator::AddDirectory(SyncItem &entry) {
	if (mPrintChangeset) cout << "[add] " << entry.GetRepositoryPath() << endl;
	if (not mDryRun)     mCatalogManager->AddDirectory(entry.CreateDirectoryEntry(),
	                                                   entry.GetParentPath());
}

void SyncMediator::RemoveDirectory(SyncItem &entry) {
	if (mPrintChangeset) cout << "[rem] " << entry.GetRepositoryPath() << endl;
	if (not mDryRun)     mCatalogManager->RemoveDirectory(entry.GetRelativePath());
}

void SyncMediator::TouchDirectory(SyncItem &entry) {
	if (mPrintChangeset) cout << "[tou] " << entry.GetRepositoryPath() << endl;
	if (not mDryRun)     mCatalogManager->TouchDirectory(entry.CreateDirectoryEntry(),
	                                                     entry.GetParentPath());
}

void SyncMediator::AddHardlinkGroups(const HardlinkGroupMap &hardlinks) {
  HardlinkGroupMap::const_iterator i,end;
	for (i = hardlinks.begin(), end = hardlinks.end(); i != end; ++i) {
	   // currentHardlinkGroup = i->second; --> just to remind you
	   
		if (mPrintChangeset) {
			cout << "[add] hardlink group around: " << i->second.masterFile.GetRepositoryPath() << "( ";	
			SyncItemList::const_iterator j,jend;
			for (j = i->second.hardlinks.begin(), jend = i->second.hardlinks.end(); j != jend; ++j) {
				cout << j->GetFilename() << " ";
			}
			cout << ")" << endl;
		}
		
		if (i->second.masterFile.IsSymlink() && not mDryRun) {
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
  DirectoryEntryList hardlinks;
  SyncItemList::const_iterator k, kend;
  for (k    = group.hardlinks.begin(), 
       kend = group.hardlinks.end(); k != kend; ++k) {
    hardlinks.push_back(k->CreateDirectoryEntry());
  }
	mCatalogManager->AddHardlinkGroup(hardlinks, group.masterFile.GetParentPath());
}
