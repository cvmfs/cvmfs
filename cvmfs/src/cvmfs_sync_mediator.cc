#include "cvmfs_sync_mediator.h"

extern "C" {
   #include "compression.h"
   #include "smalloc.h"
}

#include "hash.h"
#include "cvmfs_sync_aufs.h"
#include "cvmfs_sync_recursion.h"

#include <sstream>
#include <iostream> // TODO: remove that!

using namespace cvmfs;
using namespace std;

SyncMediator::SyncMediator(CatalogHandler *catalogHandler, string dataDirectory) {
	mCatalogHandler = catalogHandler;
	mDataDirectory = dataDirectory;
}

SyncMediator::~SyncMediator() {
	
}

void SyncMediator::add(DirEntry *entry) {
	if (entry->isDirectory()) {
		addDirectoryRecursively(entry);
	}
	
	else if (entry->isRegularFile() || entry->isSymlink()) {
		if (entry->isCatalogRequestFile() && entry->isNew()) {
			createNestedCatalog(entry);
		}
		
		if (entry->getUnionLinkcount() > 1) {
			insertHardlink(entry);
		} else {
			addFile(entry);
		}
	}
	
	else {
		stringstream ss;
		ss << "'" << entry->getRelativePath() << "' cannot be added. Unregcognized file format.";
		printWarning(ss.str());
	}
}

void SyncMediator::touch(DirEntry *entry) {
	if (entry->isDirectory()) {
		touchDirectory(entry);
	}
	
	else if (entry->isRegularFile() || entry->isSymlink()) {
		replace(entry);
	}

	else {
		stringstream ss;
		ss << "'" << entry->getRelativePath() << "' cannot be touched. Unregcognized file format.";
		printWarning(ss.str());
	}
}

void SyncMediator::remove(DirEntry *entry) {
	if (entry->isDirectory()) {
		removeDirectoryRecursively(entry);
	}
	
	else if (entry->isRegularFile() || entry->isSymlink()) {
		// first remove the file...
		removeFile(entry);
		
		// ... then the nested catalog (if needed)
		if (entry->isCatalogRequestFile()) {
			removeNestedCatalog(entry);
		}
	}

	else {
		stringstream ss;
		ss << "'" << entry->getRelativePath() << "' cannot be deleted. Unregcognized file format.";
		printWarning(ss.str());
	}
}

void SyncMediator::replace(DirEntry *entry) {
	// an entry is just a representation of a filename
	// replacing it is as easy as that:
	remove(entry);
	add(entry);
}

void SyncMediator::enterDirectory(DirEntry *entry) {
	HardlinkGroupMap newMap;
	mHardlinkStack.push(newMap);
}

void SyncMediator::leaveDirectory(DirEntry *entry) {
	completeHardlinks(entry);
	addHardlinkGroup(getHardlinkMap());
	mHardlinkStack.pop();
}

void SyncMediator::leaveAddedDirectory(DirEntry *entry) {
	addHardlinkGroup(getHardlinkMap());
	mHardlinkStack.pop();
}

void SyncMediator::commit() {
	compressAndHashFileQueue();
	addFileQueueToCatalogs();
	mCatalogHandler->precalculateListings();
	mCatalogHandler->commit();
}

void SyncMediator::compressAndHashFileQueue() {
	// compressing and hashing files
	DirEntryList::iterator i;
	DirEntryList::const_iterator iend;
	for (i = mFileQueue.begin(), iend = mFileQueue.end(); i != iend; ++i) {
		hash::t_sha1 hash;
		addFileToDatastore(*i, hash);
		(*i)->setContentHash(hash);
	}

	// compressing and hashing files in hardlink groups
	// (hardlinks point to the same "data", therefore we only have to compress it once)
	HardlinkGroupList::iterator j;
	HardlinkGroupList::const_iterator jend;
	DirEntryList::iterator k;
	DirEntryList::const_iterator kend;
	for (j = mHardlinkQueue.begin(), jend = mHardlinkQueue.end(); j != jend; ++j) {
		// hardlinks to anything else (mostly symlinks) do not have to be compressed
		if (not j->masterFile->isRegularFile()) {
			continue;
		}
		
		// compress the master file
		hash::t_sha1 hash;
		addFileToDatastore(j->masterFile, hash);
		
		// distribute the obtained hash for every hardlink
		for (k = j->hardlinks.begin(), kend = j->hardlinks.end(); k != kend; ++k) {
			(*k)->setContentHash(hash);
		}
	}
}

void SyncMediator::addFileQueueToCatalogs() {
	// add singular files
	DirEntryList::iterator i;
	DirEntryList::const_iterator iend;
	for (i = mFileQueue.begin(), iend = mFileQueue.end(); i != iend; ++i) {
		mCatalogHandler->addFile(*i);
	}
	
	// add hardlink groups
	HardlinkGroupList::iterator j;
	HardlinkGroupList::const_iterator jend;
	for (j = mHardlinkQueue.begin(), jend = mHardlinkQueue.end(); j != jend; ++j) {
		mCatalogHandler->addHardlinkGroup(j->hardlinks);
	}
}

bool SyncMediator::addFileToDatastore(DirEntry *entry, const std::string &suffix, hash::t_sha1 &hash) {
	bool result = false;

	/* Create temporary file */
	const string templ = mDataDirectory + "/txn/compressing.XXXXXX";
	char *tmp_path = (char *)smalloc(templ.length() + 1);
	strncpy(tmp_path, templ.c_str(), templ.length() + 1);
	int fd_dst = mkstemp(tmp_path);

	if ((fd_dst >= 0) && (fchmod(fd_dst, plain_file_mode) == 0)) {
		/* Compress and calculate SHA1 */
		FILE *fsrc = NULL, *fdst = NULL;
		if ( (fsrc = fopen(entry->getOverlayPath().c_str(), "r")) && 
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
			ss << "could not compress " << entry->getOverlayPath();
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

void SyncMediator::insertHardlink(DirEntry *entry) {
	uint64_t inode = entry->getUnionInode();
	HardlinkGroupMap::iterator hardlinkGroup = getHardlinkMap().find(inode);

	if (hardlinkGroup == getHardlinkMap().end()) {
		// create a new hardlink group
		HardlinkGroup newGroup;
		newGroup.masterFile = entry;
		newGroup.hardlinks.push_back(entry);
		getHardlinkMap()[inode] = newGroup;
	} else {
		// append the file to the appropriate hardlink group
		hardlinkGroup->second.hardlinks.push_back(entry);
	}
}

void SyncMediator::insertExistingHardlink(DirEntry *entry) {
	// check if found file has hardlinks (nlink > 1)
	// as we are looking through all files in one directory here, there might be
	// completely untouched hardlink groups, which we can safely skip
	// finally we have to see, if the hardlink is already part of this group
	HardlinkGroupMap::iterator hlGroup;
	if (entry->getUnionLinkcount() > 1) { // has hardlinks?
		hlGroup = getHardlinkMap().find(entry->getUnionInode());

		if (hlGroup != getHardlinkMap().end()) { // touched hardlinks in this group?
			DirEntryList::const_iterator i,end;
			bool alreadyThere = false;
			for (i = hlGroup->second.hardlinks.begin(), end = hlGroup->second.hardlinks.end(); i != end; ++i) {
				if ((*i)->isEqualTo(entry)) {
					alreadyThere = true;
					break;
				}
			}
			
			if (not alreadyThere) { // hardlink already in the group?
				// if one element of a hardlink group is edited, all elements must be replaced
				remove(entry);
				hlGroup->second.hardlinks.push_back(entry);
			}
		}
	}
}

void SyncMediator::completeHardlinks(DirEntry *entry) {
	// create a recursion engine which DOES NOT recurse into directories by default.
	// it basically goes through the current directory (in the union volume) and
	// searches for already existing hardlinks which has to be connected to the new ones
	// if there was no changed hardlink found, we can skip this
	if (getHardlinkMap().size() == 0) {
		return;
	}
	
	RecursionEngine<SyncMediator> recursion(this, UnionSync::sharedInstance()->getUnionPath(), UnionSync::sharedInstance()->getIgnoredFilenames(), false);
	recursion.foundRegularFile = &SyncMediator::insertExistingHardlink;
	recursion.foundSymlink = &SyncMediator::insertExistingHardlink;
	recursion.recurse(entry->getUnionPath());
}

void SyncMediator::addDirectoryRecursively(DirEntry *entry) {
	addDirectory(entry);
	
	RecursionEngine<SyncMediator> recursion(this, UnionSync::sharedInstance()->getOverlayPath(), UnionSync::sharedInstance()->getIgnoredFilenames());
	recursion.enteringDirectory = &SyncMediator::enterDirectory;
	recursion.leavingDirectory = &SyncMediator::leaveAddedDirectory;
	recursion.foundRegularFile = &SyncMediator::add;
	recursion.foundDirectory = &SyncMediator::addDirectoryCallback;
	recursion.foundSymlink = &SyncMediator::add;
	recursion.recurse(entry->getOverlayPath());
}

void SyncMediator::removeDirectoryRecursively(DirEntry *entry) {
	RecursionEngine<SyncMediator> recursion(this, UnionSync::sharedInstance()->getRepositoryPath(), UnionSync::sharedInstance()->getIgnoredFilenames());
	recursion.foundRegularFile = &SyncMediator::remove;
	// delete a directory AFTER it was emptied (we cannot use the generic SyncMediator::remove() here, because it would start up another recursion)
	recursion.foundDirectoryAfterRecursion = &SyncMediator::removeDirectory;
	recursion.foundSymlink = &SyncMediator::remove;
	recursion.recurse(entry->getRepositoryPath());
	
	removeDirectory(entry);
}

RecursionPolicy SyncMediator::addDirectoryCallback(DirEntry *entry) {
	addDirectory(entry);
	return RP_RECURSE; // <-- tells the recursion engine to recurse further into this directory
}

void SyncMediator::createNestedCatalog(DirEntry *requestFile) {
	cout << "[add] NESTED CATALOG" << endl;
	mCatalogHandler->createNestedCatalog(requestFile->getParentPath());
}

void SyncMediator::removeNestedCatalog(DirEntry *requestFile) {
	cout << "[rem] NESTED CATALOG" << endl;
	mCatalogHandler->removeNestedCatalog(requestFile->getParentPath());
}

void SyncMediator::addFile(DirEntry *entry) {
	cout << "[add] " << entry->getRepositoryPath() << endl;
	
	if (entry->isSymlink()) {
		mCatalogHandler->addFile(entry);
	} else {
		mFileQueue.push_back(entry);
	}
}

void SyncMediator::removeFile(DirEntry *entry) {
	cout << "[rem] " << entry->getRepositoryPath() << endl;
	mCatalogHandler->removeFile(entry);
}

void SyncMediator::touchFile(DirEntry *entry) {
	cout << "[tou] " << entry->getRepositoryPath() << endl;
	// nothing... will never happen
}

void SyncMediator::addDirectory(DirEntry *entry) {
	cout << "[add] " << entry->getRepositoryPath() << endl;
	mCatalogHandler->addDirectory(entry);
}

void SyncMediator::removeDirectory(DirEntry *entry) {
	cout << "[rem] " << entry->getRepositoryPath() << endl;
	mCatalogHandler->removeDirectory(entry);
}

void SyncMediator::touchDirectory(DirEntry *entry) {
	cout << "[tou] " << entry->getRepositoryPath() << endl;
}

void SyncMediator::addHardlinkGroup(const HardlinkGroupMap &hardlinks) {
	HardlinkGroupMap::const_iterator i,end;
	for (i = hardlinks.begin(), end = hardlinks.end(); i != end; ++i) {
		cout << "[add] hardlink group around: " << i->second.masterFile->getRepositoryPath() << endl;
		if (i->second.masterFile->isSymlink()) {
			mCatalogHandler->addHardlinkGroup(i->second.hardlinks);
		} else {
			mHardlinkQueue.push_back(i->second);				
		}		
	}
}
