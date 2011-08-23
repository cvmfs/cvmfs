#include "cvmfs_sync_mediator.h"

#include "cvmfs_sync_recursion.h"

#include <sstream>
#include <iostream> // TODO: remove that!

using namespace cvmfs;
using namespace std;

SyncMediator::SyncMediator() {
	mCatalogs = new CatalogHandler();
}

SyncMediator::~SyncMediator() {
	
}

void SyncMediator::add(DirEntry *entry) {
	if (entry->isDirectory()) {
		addDirectoryRecursively(entry);
	}
	
	else if (entry->isCatalogRequestFile()) {
		createNestedCatalog(entry);
	}
	
	else if (entry->isRegularFile() || entry->isSymlink()) {
		if (entry->getUnionLinkcount() > 1) {
			insertHardlink(entry);
		} else {
			addFile(entry);
		}
	}
	
	else {
		stringstream ss;
		ss << "'" << entry->getRelativePath() << "' cannot be added. Unregcognized file format.";
		UnionFilesystemSync::sharedInstance()->printWarning(ss.str());
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
		UnionFilesystemSync::sharedInstance()->printWarning(ss.str());
	}
}

void SyncMediator::remove(DirEntry *entry) {
	if (entry->isDirectory()) {
		removeDirectoryRecursively(entry);
	}

	else if (entry->isCatalogRequestFile()) {
		removeNestedCatalog(entry);
	}
	
	else if (entry->isRegularFile() || entry->isSymlink()) {
		removeFile(entry);
	}

	else {
		stringstream ss;
		ss << "'" << entry->getRelativePath() << "' cannot be deleted. Unregcognized file format.";
		UnionFilesystemSync::sharedInstance()->printWarning(ss.str());
	}
}

void SyncMediator::replace(DirEntry *entry) {
	// an entry is just a representation of a filename
	// replacing it is as easy as that:
	remove(entry);
	add(entry);
}

void SyncMediator::enterDirectory(DirEntry *entry) {
	cout << "-> enter directory " << entry->getRepositoryPath() << endl;
	
	HardlinkGroupMap newMap;
	mHardlinkStack.push(newMap);
}

void SyncMediator::leaveDirectory(DirEntry *entry) {
	cout << "<- leave directory " << entry->getRepositoryPath() << endl;
	
	completeHardlinks(entry);
	addHardlinkGroup(getHardlinkMap());
	mHardlinkStack.pop();
}

void SyncMediator::leaveAddedDirectory(DirEntry *entry) {
	cout << "<- leave directory " << entry->getRepositoryPath() << endl;
	addHardlinkGroup(getHardlinkMap());
	mHardlinkStack.pop();
}

void SyncMediator::insertHardlink(DirEntry *entry) {
	uint64_t inode = entry->getUnionInode();
	HardlinkGroupMap::iterator hardlinkGroup = getHardlinkMap().find(inode);

	cout << "inserting hardlink... " << entry->getFilename();
	
	if (hardlinkGroup == getHardlinkMap().end()) {
		// create a new hardlink group
		HardlinkGroup newGroup;
		newGroup.masterFile = entry;
		newGroup.hardlinks.push_back(entry);
		getHardlinkMap()[inode] = newGroup;
		
		cout << "... new" << endl;
	} else {
		// append the file to the appropriate hardlink group
		hardlinkGroup->second.hardlinks.push_back(entry);
		
		cout << "... appended (" << hardlinkGroup->second.hardlinks.size() << ")" << endl;
	}
}

void SyncMediator::insertExistingHardlink(DirEntry *entry) {
	// check if found file has hardlinks (nlink > 2)
	// as we are looking through all files in one directory here, there might be
	// completely untouched hardlink groups, which we can safely skip
	// finally we have to see, if the hardlink is already part of this group
	HardlinkGroupMap::iterator hlGroup;
	if (entry->getUnionLinkcount() > 2) { // has hardlinks?
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
				hlGroup->second.hardlinks.push_back(entry);
				cout << "inserting existing hardlink... " << entry->getFilename() << endl;
			}
		}
	}
}

void SyncMediator::completeHardlinks(DirEntry *entry) {
	// create a recursion engine which DOES NOT recurse into directories by default.
	// it basically goes through the current directory (in the union volume) and
	// searches for already existing hardlinks which has to be connected to the new ones
	// if there is no hardlink in the current change set, we can skip this
	if (getHardlinkMap().size() == 0) {
		return;
	}

	cout << "--> start looking for existing hardlinks in " << entry->getRelativePath() << endl;
	
	RecursionEngine<SyncMediator> recursion(this, UnionFilesystemSync::sharedInstance()->getUnionPath(), false);
	recursion.foundRegularFile = &SyncMediator::insertExistingHardlink;
	recursion.foundSymlink = &SyncMediator::insertExistingHardlink;
	recursion.recurse(entry->getUnionPath());
	
	cout << "--> ready " << endl;
}

void SyncMediator::addDirectoryRecursively(DirEntry *entry) {
	addDirectory(entry);
	
	RecursionEngine<SyncMediator> recursion(this, UnionFilesystemSync::sharedInstance()->getOverlayPath());
	recursion.enteringDirectory = &SyncMediator::enterDirectory;
	recursion.leavingDirectory = &SyncMediator::leaveAddedDirectory;
	recursion.foundRegularFile = &SyncMediator::add;
	recursion.foundDirectory = &SyncMediator::addDirectoryCallback;
	recursion.foundSymlink = &SyncMediator::add;
	recursion.recurse(entry->getOverlayPath());
}

void SyncMediator::removeDirectoryRecursively(DirEntry *entry) {
	RecursionEngine<SyncMediator> recursion(this, UnionFilesystemSync::sharedInstance()->getOverlayPath());
	recursion.foundRegularFile = &SyncMediator::remove;
	recursion.foundDirectoryAfterRecursion = &SyncMediator::removeDirectory; // delete a directory AFTER it was emptied
	recursion.foundSymlink = &SyncMediator::remove;
	recursion.recurse(entry->getOverlayPath());
	
	removeDirectory(entry);
}

bool SyncMediator::addDirectoryCallback(DirEntry *entry) {
	addDirectory(entry);
	return true; // <-- tells the recursion engine to recurse further into this directory
}

void SyncMediator::createNestedCatalog(DirEntry *requestFile) {
	cout << "[add] NESTED CATALOG" << endl;
//	mChangeset.addedNestedCatalogs.push_back(requestFile->getParentPath());
}

void SyncMediator::removeNestedCatalog(DirEntry *requestFile) {
	cout << "[rem] NESTED CATALOG" << endl;
//	mChangeset.removedNestedCatalogs.push_back(requestFile->getParentPath());
}

void SyncMediator::addFile(DirEntry *entry) {
	cout << "[add] " << entry->getRepositoryPath() << endl;
	list<string> filename;
	filename.push_back(entry->getRelativePath());
	mChangeset.addedFiles.push_back(filename);
}

void SyncMediator::removeFile(DirEntry *entry) {
	cout << "[rem] " << entry->getRepositoryPath() << endl;
	mChangeset.removedFiles.push_back(entry->getRelativePath());
}

void SyncMediator::touchFile(DirEntry *entry) {
	cout << "[tou] " << entry->getRepositoryPath() << endl;
	// nothing... will never happen
}

void SyncMediator::addDirectory(DirEntry *entry) {
	cout << "[add] " << entry->getRepositoryPath() << endl;
	mChangeset.addedDirectories.push_back(entry->getRelativePath());
}

void SyncMediator::removeDirectory(DirEntry *entry) {
	cout << "[rem] " << entry->getRepositoryPath() << endl;
	mChangeset.removedDirectories.push_back(entry->getRelativePath());
}

void SyncMediator::touchDirectory(DirEntry *entry) {
	cout << "[tou] " << entry->getRepositoryPath() << endl;
	mChangeset.touchedDirectories.push_back(entry->getRelativePath());
}

void SyncMediator::addHardlinkGroup(const HardlinkGroupMap &hardlinks) {
	cout << "adding hardlink groups... " << endl;
	
	HardlinkGroupMap::const_iterator i,end;
	for (i = hardlinks.begin(), end = hardlinks.end(); i != end; ++i) {
		cout << "master file: " << i->second.masterFile->getRepositoryPath() << endl;
		
		list<string> filenames;
		DirEntryList::const_iterator j, send;
		for (j = i->second.hardlinks.begin(), send = i->second.hardlinks.end(); j != send; ++j) {
			filenames.push_back((*i)->getRelativePath());
		}
		mChangeset.addedFiles.push_back(filenames);
	}
}
