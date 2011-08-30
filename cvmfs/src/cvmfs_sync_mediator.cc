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

SyncMediator::SyncMediator(CatalogHandler *catalogHandler, const SyncParameters *parameters) {
	mCatalogHandler = catalogHandler;
	mDataDirectory = canonical_path(parameters->dir_data);
	mDryRun = parameters->dry_run;
	mPrintChangeset = parameters->print_changeset;
	mGuessHardlinks = true;
}

SyncMediator::~SyncMediator() {
	
}

void SyncMediator::add(DirEntry *entry) {
	if (entry->isDirectory()) {
		addDirectoryRecursively(entry);
	}
	
	else if (entry->isRegularFile() || entry->isSymlink()) {

		// create a nested catalog if we find a NEW request file
		if (entry->isCatalogRequestFile() && entry->isNew()) {
			createNestedCatalog(entry);
		}
		
		// a file is a hard link if the link count is greater than 1 OR if the file has an associated hard link group in the catalog
		if (entry->getUnionLinkcount() > 1 || (mGuessHardlinks && not entry->isNew() && mCatalogHandler->isPartOfHardlinkGroup(entry))) {
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
		if (entry->isCatalogRequestFile() && not entry->isNew()) {
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
	// don't do things you could regret later on
	if (mDryRun) {
		return;
	}
	
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

uint64_t SyncMediator::getTemporaryHardlinkGroupNumber(DirEntry *entry) const {
	uint64_t hardlinkGroupNumber = 0;
	
	if (mGuessHardlinks) {
		// if we have to guess the hard link relations there is some more stuff to do here:
		// we are asserting, that the repository was mounted with -o hide_hardlinks otherwise this will fail
		if (entry->getUnionLinkcount() > 1) {
			// if the union linkcount is bigger than one
			// there must have been a new hard link created --> we need to recreate this mapping later on
			// until now we only collect this 'subgroup'
			hardlinkGroupNumber = entry->getUnionInode();
			hardlinkGroupNumber = hardlinkGroupNumber << 10; // this is a hack to avoid collisions (hopefully nobody will have more than 2^10 hard link groups in one catalog context and hopefully the inode numbers don't get bigger than 56 bit)
		} else {
			// if we have a link count of one 
			hardlinkGroupNumber = mCatalogHandler->getHardlinkGroup(entry);
		}
	} else {
		hardlinkGroupNumber = entry->getUnionInode();
	}
	
	return hardlinkGroupNumber;
}

void SyncMediator::insertHardlink(DirEntry *entry) {
	uint64_t hardlinkGroupNumber = getTemporaryHardlinkGroupNumber(entry);
	
	// find the hard link group in the lists
	HardlinkGroupMap::iterator hardlinkGroup = getHardlinkMap().find(hardlinkGroupNumber);

	if (hardlinkGroup == getHardlinkMap().end()) {
		// create a new hardlink group
		HardlinkGroup newGroup;
		newGroup.masterFile = entry;
		newGroup.hardlinks.push_back(entry);
		getHardlinkMap()[hardlinkGroupNumber] = newGroup;
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
	
	// check if we have a hard link here
	if (entry->getUnionLinkcount() <= 1 && (mGuessHardlinks && not mCatalogHandler->isPartOfHardlinkGroup(entry))) {
		return;
	}
	HardlinkGroupMap::iterator hlGroup;
	uint64_t hardlinkGroupNumber = getTemporaryHardlinkGroupNumber(entry);
	hlGroup = getHardlinkMap().find(hardlinkGroupNumber);

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
	if (mPrintChangeset) cout << "[add] NESTED CATALOG" << endl;
	if (not mDryRun)     mCatalogHandler->createNestedCatalog(requestFile->getParentPath());
}

void SyncMediator::removeNestedCatalog(DirEntry *requestFile) {
	if (mPrintChangeset) cout << "[rem] NESTED CATALOG" << endl;
	if (not mDryRun)     mCatalogHandler->removeNestedCatalog(requestFile->getParentPath());
}

void SyncMediator::addFile(DirEntry *entry) {
	if (mPrintChangeset) cout << "[add] " << entry->getRepositoryPath() << endl;
	
	if (entry->isSymlink() && not mDryRun) {
		mCatalogHandler->addFile(entry);
	} else {
		mFileQueue.push_back(entry);
	}
}

void SyncMediator::removeFile(DirEntry *entry) {
	if (mPrintChangeset) cout << "[rem] " << entry->getRepositoryPath() << endl;
	if (not mDryRun)     mCatalogHandler->removeFile(entry);
}

void SyncMediator::touchFile(DirEntry *entry) {
	if (mPrintChangeset) cout << "[tou] " << entry->getRepositoryPath() << endl;
	if (not mDryRun)     mCatalogHandler->touchFile(entry);
}

void SyncMediator::addDirectory(DirEntry *entry) {
	if (mPrintChangeset) cout << "[add] " << entry->getRepositoryPath() << endl;
	if (not mDryRun)     mCatalogHandler->addDirectory(entry);
}

void SyncMediator::removeDirectory(DirEntry *entry) {
	if (mPrintChangeset) cout << "[rem] " << entry->getRepositoryPath() << endl;
	if (not mDryRun)     mCatalogHandler->removeDirectory(entry);
}

void SyncMediator::touchDirectory(DirEntry *entry) {
	if (mPrintChangeset) cout << "[tou] " << entry->getRepositoryPath() << endl;
	if (not mDryRun)     mCatalogHandler->touchDirectory(entry);
}

void SyncMediator::addHardlinkGroup(const HardlinkGroupMap &hardlinks) {
	HardlinkGroupMap::const_iterator i,end;
	for (i = hardlinks.begin(), end = hardlinks.end(); i != end; ++i) {
		if (mPrintChangeset) {
			cout << "[add] hardlink group around: " << i->second.masterFile->getRepositoryPath() << "( ";	
			DirEntryList::const_iterator j,jend;
			for (j = i->second.hardlinks.begin(), jend = i->second.hardlinks.end(); j != jend; ++j) {
				cout << (*j)->getFilename() << " ";
			}
			cout << ")" << endl;
		}
		
		if (i->second.masterFile->isSymlink() && not mDryRun) {
			mCatalogHandler->addHardlinkGroup(i->second.hardlinks);
		} else {
			mHardlinkQueue.push_back(i->second);
		}
	}
}
