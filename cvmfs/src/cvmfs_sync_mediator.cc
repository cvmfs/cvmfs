#include "cvmfs_sync_mediator.h"

#include "cvmfs_sync_recursion.h"

#include <sstream>
#include <iostream> // TODO: remove that!

using namespace cvmfs;
using namespace std;

SyncMediator::SyncMediator() {
	
}

SyncMediator::~SyncMediator() {
	
}

void SyncMediator::add(DirEntry *entry) {
	if (entry->isDirectory()) {
		addDirectoryRecursively(entry);
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
		touchFile(entry);
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

void SyncMediator::commit() {
	// hardlinks has to be treated somehow
}

void SyncMediator::enterDirectory(DirEntry *entry) {
	HardlinkGroupMap newMap;
	mHardlinkStack.push(newMap);
}

void SyncMediator::leaveDirectory(DirEntry *entry) {
	cout << "!/!?!? leave" << endl;
	completeHardlinks(entry);
	addHardlinkGroup(mHardlinkStack.top());
	mHardlinkStack.pop();
}

void SyncMediator::insertHardlink(DirEntry *entry) {
	uint64_t inode = entry->getUnionInode();
	HardlinkGroupMap::iterator hardlinkGroup = mHardlinkStack.top().find(inode);
	
	if (hardlinkGroup == mHardlinkStack.top().end()) {
		// create a new hardlink group
		HardlinkGroup newGroup;
		newGroup.masterFile = entry;
		newGroup.hardlinks.push_back(entry);
		mHardlinkStack.top()[inode] = newGroup;
	} else {
		// append the file to the appropriate hardlink group
		hardlinkGroup->second.hardlinks.push_back(entry);
	}

	cout << "inserting hardlink... " << entry->getFilename() << endl;
}

void SyncMediator::insertExistingHardlink(DirEntry *entry) {
	// check if found file has hardlinks (nlink > 2)
	// as we are looking through all files in one directory here, there might be
	// completely untouched hardlink groups, which we can safely skip
	HardlinkGroupMap::iterator hlGroup;
	if (entry->getUnionLinkcount() > 2) { // has hardlinks?
		hlGroup = mHardlinkStack.top().find(entry->getUnionInode());
		if (hlGroup != mHardlinkStack.top().end()) { // touched hardlinks in this group?
			hlGroup->second.hardlinks.push_back(entry);
			cout << "inserting existing hardlink... " << entry->getFilename() << endl;
		}
	}
}

void SyncMediator::completeHardlinks(DirEntry *entry) {
	// create a recursion engine which DOES NOT recurse into directories by default.
	// it basically goes through the current directory (in the union volume) and
	// searches for already existing hardlinks which has to be connected to the new ones
	// if there is no hardlink in the current change set, we can skip this
	if (mHardlinkStack.top().size() == 0) {
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
	recursion.leavingDirectory = &SyncMediator::leaveDirectory;
	recursion.foundRegularFile = &SyncMediator::add;
	recursion.foundDirectory = &SyncMediator::addDirectoryCallback;
	recursion.foundSymlink = &SyncMediator::add;
	recursion.recurse(entry->getOverlayPath());
}

void SyncMediator::removeDirectoryRecursively(DirEntry *entry) {
	RecursionEngine<SyncMediator> recursion(this, UnionFilesystemSync::sharedInstance()->getOverlayPath());
	recursion.foundRegularFile = &SyncMediator::remove;
	recursion.foundDirectory = &SyncMediator::removeDirectoryCallback;
	recursion.foundSymlink = &SyncMediator::remove;
	recursion.recurse(entry->getOverlayPath());
	
	removeDirectory(entry);
}

bool SyncMediator::addDirectoryCallback(DirEntry *entry) {
	addDirectory(entry);
	return true; // <-- tells the recursion engine to recurse further into this directory
}

bool SyncMediator::removeDirectoryCallback(DirEntry *entry) {
	removeDirectory(entry);
	return true; // <-- tells the recursion engine to recurse further into this directory
}

void SyncMediator::addFile(DirEntry *entry) {
	cout << "adding file... " << entry->getFilename() << endl;
}

void SyncMediator::removeFile(DirEntry *entry) {
	cout << "removing file... " << entry->getFilename() << endl;
}

void SyncMediator::touchFile(DirEntry *entry) {
	cout << "touching file... " << entry->getFilename() << endl;
}

void SyncMediator::addDirectory(DirEntry *entry) {
	cout << "adding directory... " << entry->getFilename() << endl;
}

void SyncMediator::removeDirectory(DirEntry *entry) {
	cout << "removing directory... " << entry->getFilename() << endl;
}

void SyncMediator::touchDirectory(DirEntry *entry) {
	cout << "touching directory... " << entry->getFilename() << endl;
}

void SyncMediator::addHardlinkGroup(const HardlinkGroupMap &hardlinks) {
	cout << "adding hardlink groups... " << endl;
}
