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

void *MainReceive(void *data) {
  SyncMediator *mediator = (SyncMediator *)data;

  FILE *fpipe_hashes = fdopen(mediator->pipe_hashes_, "r");
  char c;
  string tmp;
  string path;
  int error_code = -5000;
  hash::Any hash(hash::kSha1);
  while ((c = fgetc(fpipe_hashes)) != EOF) {
    switch (c) {
      case '\0':
        if (error_code == -5000) {
          error_code = 0;
        } else {
          path = tmp.substr(mediator->params_->dir_union.length()+1);
        }
        tmp = "";
        break;
      case '\n': {
        hash = hash::Any(hash::kSha1, hash::HexPtr(tmp));
        cout << "received: " << path << ", " << hash.ToString() << endl;
        pthread_mutex_lock(&mediator->lock_file_queue_);
        SyncItemList::iterator itr = mediator->mFileQueue.find(path);
        if (itr == mediator->mFileQueue.end()) {
          cout << path << endl;
          for (SyncItemList::const_iterator i = mediator->mFileQueue.begin(); i != mediator->mFileQueue.end(); ++i)
            cout << i->first << endl;
        }
        assert(itr != mediator->mFileQueue.end());
        itr->second.SetContentHash(hash);
        mediator->num_files_process--;
        pthread_mutex_unlock(&mediator->lock_file_queue_);
        tmp = "";
        error_code = -5000;
        break;
      } default:
        tmp.append(1, c);
    }
  }
  fclose(fpipe_hashes);

  return NULL;
}

SyncMediator::SyncMediator(catalog::WritableCatalogManager *catalogManager,
                           const SyncParameters *params) :
  mCatalogManager(catalogManager),
  params_(params)
{
  int retval = pthread_mutex_init(&lock_file_queue_, NULL);
  assert(retval == 0);
  num_files_process = 0;

  if (params_->process_locally) {
    if (fork() == 0) {
      FILE *pipe_paths = fopen(params_->paths_out.c_str(), "r");
      int pipe_hashes = open(params_->hashes_in.c_str(), O_WRONLY);
      char c;
      string path;
      while ((c = fgetc(pipe_paths)) != EOF) {
        if (c == '\n') {
          string path_compressed;
          FILE *file_compressed =
          CreateTempFile(params_->dir_temp + "/compressing",
                         kDefaultFileMode, "w", &path_compressed);
          assert(file_compressed);

          LogCvmfs(kLogCvmfs, kLogStdout, "compressing and hashing %s", path.c_str());
          FILE *file_source = fopen(path.c_str(), "r");
          assert(file_source);
          hash::Any hash(hash::kSha1);
          int retval = zlib::CompressFile2File(file_source, file_compressed,
                                               &hash);
          assert(retval);
          fclose(file_compressed);
          fclose(file_source);

          const string path_dest = "/data" + hash.MakePath(1, 2);
          retval = params->forklift->Move(path_compressed, path_dest);
          assert(retval);
          string to_pipe = "0";
          to_pipe.push_back('\0');
          to_pipe.append(path);
          to_pipe.push_back('\0');
          to_pipe.append(hash.ToString());
          to_pipe.push_back('\n');
          WritePipe(pipe_hashes, to_pipe.data(), to_pipe.length());

          path = "";
        } else {
          path.append(1, c);
        }
      }
      fclose(pipe_paths);
      close(pipe_hashes);
      _exit(0);
    }
  }

  LogCvmfs(kLogCvmfs, kLogStdout, "connecting to %s",
           params->paths_out.c_str());
  pipe_fanout_ = open(params_->paths_out.c_str(), O_WRONLY);
  LogCvmfs(kLogCvmfs, kLogStdout, "connecting to %s",
           params->hashes_in.c_str());
  pipe_hashes_ = open(params_->hashes_in.c_str(), O_RDONLY);
  LogCvmfs(kLogCvmfs, kLogStdout, "starting receiver thread");
  retval = pthread_create(&thread_receive_, NULL, MainReceive, (void *)this);
  assert(retval == 0);

  LogCvmfs(kLogCvmfs, kLogStdout, "processing changes...");
}


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

void SyncMediator::Replace(SyncItem &entry) {
  PrintWarning("REPLACE entry:" + entry.GetRelativePath());
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


/**** EXPERIMENTAL *****/

void SyncMediator::CompressAndHashFileQueue() {

  close(pipe_fanout_);
  pthread_join(thread_receive_, NULL);

  return;
}

/*	// compressing and hashing files
	// TODO: parallelize this!
	SyncItemList::iterator i;
	SyncItemList::const_iterator iend;
	for (i = mFileQueue.begin(), iend = mFileQueue.end(); i != iend; ++i) {
		hash::Any hash(hash::kSha1);
		//EXP AddFileToDatastore(*i, hash);
		//EXP i->SetContentHash(hash);
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
		hash::Any hash(hash::kSha1);
		AddFileToDatastore(j->masterFile, hash);

		// distribute the obtained hash for every hardlink
		for (k = j->hardlinks.begin(), kend = j->hardlinks.end(); k != kend; ++k) {
			//EXP k->SetContentHash(hash);
		}
	}
}*/

void SyncMediator::AddFileQueueToCatalogs() {
	// don't do things you could regret later on
	if (params_->dry_run) {
		return;
	}

	// add singular files
	SyncItemList::const_iterator i, iend;
	for (i = mFileQueue.begin(), iend = mFileQueue.end(); i != iend; ++i) {
		mCatalogManager->AddFile(i->second.CreateCatalogDirent(), i->second.relative_parent_path());
	}

	// add hardlink groups
	HardlinkGroupList::const_iterator j, jend;
	for (j = mHardlinkQueue.begin(), jend = mHardlinkQueue.end(); j != jend; ++j) {
    AddHardlinkGroup(*j);
	}
}

bool SyncMediator::AddFileToDatastore(SyncItem &entry, const std::string &suffix, hash::Any &hash) {
	// don't do that, would change something!
	if (params_->dry_run) {
		return true;
	}

	bool result = false;
	/* Create temporary file */
	const string templ = params_->dir_temp + "/compressing.XXXXXX";
	char *tmp_path = (char *)smalloc(templ.length() + 1);
	strncpy(tmp_path, templ.c_str(), templ.length() + 1);
	int fd_dst = mkstemp(tmp_path);

	if ((fd_dst >= 0) && (fchmod(fd_dst, kDefaultFileMode) == 0)) {
		/* Compress and calculate SHA1 */
		FILE *fsrc = NULL, *fdst = NULL;
		if ( (fsrc = fopen(entry.GetScratchPath().c_str(), "r")) &&
		     (fdst = fdopen(fd_dst, "w")) &&
         zlib::CompressFile2File(fsrc, fdst, &hash) )
		{
			const string sha1str = hash.ToString();
			const string cache_path = "TODO - DATA DIRECTORY/" + sha1str.substr(0, 2) + "/" +
			sha1str.substr(2) + suffix;
			fflush(fdst);
			if (rename(tmp_path, cache_path.c_str()) == 0) {
				result = true;
			} else {
				unlink(tmp_path);
				stringstream ss;
				ss << "could not rename " << tmp_path << " to " << cache_path;
				PrintWarning(ss.str());
			}
		} else {
			stringstream ss;
			ss << "could not compress " << entry.GetScratchPath();
			PrintWarning(ss.str());
		}
		if (fsrc) fclose(fsrc);
		if (fdst) fclose(fdst);
	} else {
		stringstream ss;
		ss << "could not create temporary file " << templ;
		PrintWarning(ss.str());
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
	FileSystemTraversal<SyncMediator> traversal(this, mUnionEngine->union_path(), false);
	traversal.foundRegularFile = &SyncMediator::InsertExistingHardlinkFileCallback;
	traversal.foundSymlink = &SyncMediator::InsertExistingHardlinkSymlinkCallback;
	traversal.Recurse(entry.GetUnionPath());
}

void SyncMediator::InsertExistingHardlinkFileCallback(const std::string &parent_dir,
                                                      const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, kItemFile, mUnionEngine);
  InsertExistingHardlink(entry);
}

void SyncMediator::InsertExistingHardlinkSymlinkCallback(const std::string &parent_dir,
                                                         const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, kItemSymlink, mUnionEngine);
  InsertExistingHardlink(entry);
}

// -----------------------------------------------------------------------------

void SyncMediator::AddDirectoryRecursively(SyncItem &entry) {
	AddDirectory(entry);

	// create a recursion engine, which recursively adds all entries in a newly created directory
	FileSystemTraversal<SyncMediator> traversal(this, mUnionEngine->scratch_path(), true, mUnionEngine->GetIgnoreFilenames());
	traversal.enteringDirectory = &SyncMediator::EnterAddedDirectoryCallback;
	traversal.leavingDirectory = &SyncMediator::LeaveAddedDirectoryCallback;
	traversal.foundRegularFile = &SyncMediator::AddFileCallback;
	traversal.foundDirectory = &SyncMediator::AddDirectoryCallback;
	traversal.foundSymlink = &SyncMediator::AddSymlinkCallback;
	traversal.Recurse(entry.GetScratchPath());
}

bool SyncMediator::AddDirectoryCallback(const std::string &parent_dir,
                                        const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, kItemDir, mUnionEngine);
	AddDirectory(entry);
	return true; // <-- tells the recursion engine to recurse further into this directory
}

void SyncMediator::AddFileCallback(const std::string &parent_dir,
                                   const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, kItemFile, mUnionEngine);
  Add(entry);
}

void SyncMediator::AddSymlinkCallback(const std::string &parent_dir,
                                      const std::string &link_name) {
  SyncItem entry(parent_dir, link_name, kItemSymlink, mUnionEngine);
  Add(entry);
}

void SyncMediator::EnterAddedDirectoryCallback(const std::string &parent_dir,
                                               const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, kItemDir, mUnionEngine);
  EnterDirectory(entry);
}

void SyncMediator::LeaveAddedDirectoryCallback(const std::string &parent_dir,
                                               const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, kItemDir, mUnionEngine);
  bool complete_hardlinks = false; // <-- this was a NEW directory...
                                   //     there are no 'unchanged' hardlinks
  LeaveDirectory(entry, complete_hardlinks);
}

// -----------------------------------------------------------------------------

void SyncMediator::RemoveDirectoryRecursively(SyncItem &entry) {
	// delete a directory AFTER it was emptied here,
	// because it would start up another recursion

	FileSystemTraversal<SyncMediator> traversal(this, mUnionEngine->rdonly_path());
	traversal.foundRegularFile = &SyncMediator::RemoveFileCallback;
	traversal.foundDirectoryAfterRecursion = &SyncMediator::RemoveDirectoryCallback;
	traversal.foundSymlink = &SyncMediator::RemoveSymlinkCallback;
	traversal.Recurse(entry.GetRdOnlyPath());

	// the given directory was emptied recursively and can now itself be deleted
	RemoveDirectory(entry);
}

void SyncMediator::RemoveFileCallback(const std::string &parent_dir,
                                      const std::string &file_name) {
  SyncItem entry(parent_dir, file_name, kItemFile, mUnionEngine);
  Remove(entry);
}

void SyncMediator::RemoveSymlinkCallback(const std::string &parent_dir,
                                         const std::string &link_name) {
  SyncItem entry(parent_dir, link_name, kItemSymlink, mUnionEngine);
  Remove(entry);
}

void SyncMediator::RemoveDirectoryCallback(const std::string &parent_dir,
                                           const std::string &dir_name) {
  SyncItem entry(parent_dir, dir_name, kItemDir, mUnionEngine);
  RemoveDirectory(entry);
}

// -----------------------------------------------------------------------------

void SyncMediator::CreateNestedCatalog(SyncItem &requestFile) {
	if (params_->print_changeset) cout << "[add] NESTED CATALOG" << endl;
	if (not params_->dry_run)     mCatalogManager->CreateNestedCatalog(requestFile.relative_parent_path());
}

void SyncMediator::RemoveNestedCatalog(SyncItem &requestFile) {
	if (params_->print_changeset) cout << "[rem] NESTED CATALOG" << endl;
	if (not params_->dry_run)     mCatalogManager->RemoveNestedCatalog(requestFile.relative_parent_path());
}

void SyncMediator::AddFile(SyncItem &entry) {
	if (params_->print_changeset) cout << "[add] " << entry.GetRdOnlyPath() << endl;

	if (entry.IsSymlink() && not params_->dry_run) {
	  // symlinks have no 'actual' file content, which would have to be compressed...
		mCatalogManager->AddFile(entry.CreateCatalogDirent(), entry.relative_parent_path());
	} else {
	  // push the file in the queue for later post-processing
    pthread_mutex_lock(&lock_file_queue_);
    num_files_process++;
		mFileQueue[entry.GetRelativePath()] = entry;
    pthread_mutex_unlock(&lock_file_queue_);
    const string full_path = params_->dir_union + "/" + entry.GetRelativePath() + "\n";
    WritePipe(pipe_fanout_, full_path.data(), full_path.length());
	}
}

void SyncMediator::RemoveFile(SyncItem &entry) {
	if (params_->print_changeset) cout << "[rem] " << entry.GetRdOnlyPath() << endl;
	if (not params_->dry_run)     mCatalogManager->RemoveFile(entry.GetRelativePath());
}

void SyncMediator::TouchFile(SyncItem &entry) {
	if (params_->print_changeset) cout << "[tou] " << entry.GetRdOnlyPath() << endl;
	if (not params_->dry_run)     mCatalogManager->TouchFile(entry.CreateCatalogDirent(),
	                                                entry.GetRelativePath());
}

void SyncMediator::AddDirectory(SyncItem &entry) {
	if (params_->print_changeset) cout << "[add] " << entry.GetRdOnlyPath() << endl;
	if (not params_->dry_run)     mCatalogManager->AddDirectory(entry.CreateCatalogDirent(),
	                                                   entry.relative_parent_path());
}

void SyncMediator::RemoveDirectory(SyncItem &entry) {
	if (params_->print_changeset) cout << "[rem] " << entry.GetRdOnlyPath() << endl;
	if (not params_->dry_run)     mCatalogManager->RemoveDirectory(entry.GetRelativePath());
}

void SyncMediator::TouchDirectory(SyncItem &entry) {
	if (params_->print_changeset) cout << "[tou] " << entry.GetRdOnlyPath() << endl;
	if (not params_->dry_run)     mCatalogManager->TouchDirectory(entry.CreateCatalogDirent(),
	                                                     entry.GetRelativePath());
}

void SyncMediator::AddHardlinkGroups(const HardlinkGroupMap &hardlinks) {
  HardlinkGroupMap::const_iterator i,end;
	for (i = hardlinks.begin(), end = hardlinks.end(); i != end; ++i) {
	   // currentHardlinkGroup = i->second; --> just to remind you

		if (params_->print_changeset) {
			cout << "[add] hardlink group around: " << i->second.masterFile.GetRdOnlyPath() << "( ";
			SyncItemList::const_iterator j,jend;
			for (j = i->second.hardlinks.begin(), jend = i->second.hardlinks.end(); j != jend; ++j) {
				cout << j->second.filename() << " ";
			}
			cout << ")" << endl;
		}

		if (i->second.masterFile.IsSymlink() && not params_->dry_run) {
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
	mCatalogManager->AddHardlinkGroup(hardlinks, group.masterFile.relative_parent_path());
}

}  // namespace publish
