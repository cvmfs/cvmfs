#include "cvmfs_sync_aufs.h"

#include "util.h"
#include "compat.h"

#include <iostream> // remove later
#include <dirent.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>
#include <unistd.h>

using namespace cvmfs;
using namespace std;

SyncAufs1::SyncAufs1(const string &repositoryPath, const std::string &unionPath, const std::string &aufsPath) :
	UnionFilesystemSync(repositoryPath, unionPath, aufsPath){
	// init ignored filenames
	mIgnoredFilenames.insert(".wh..wh..tmp");
	mIgnoredFilenames.insert(".wh..wh.plnk");
	mIgnoredFilenames.insert(".wh..wh.aufs");
	
	// set the whiteout prefix AUFS preceeds for every whiteout file
	mWhiteoutPrefix = ".wh.";
}

SyncAufs1::~SyncAufs1() {
	
}

bool SyncAufs1::goGetIt() {
	RecursionEngine<SyncAufs1> recursion(this, mOverlayPath);
	
	recursion.foundRegularFile = &SyncAufs1::processFoundRegularFile;
	recursion.foundDirectory = &SyncAufs1::processFoundDirectory;
	recursion.foundLink = &SyncAufs1::processFoundLink;
	recursion.caresAbout = &SyncAufs1::isInterestingFilename;
	
	recursion.recurse(mOverlayPath);
	
	return true;
}

bool SyncAufs1::isEditedItem(const string &dirPath, const string &filename) const {
	return true;

	// to be implemented...
	
	// // this is a very creepy approach, but just for testing purposes...
	// string overlayPath = getPathToOverlayFile(dirPath, filename);
	// string unionPath = getPathToUnionFile(dirPath, filename);
	// 
	// // read the inode 
	// PortableStat64 info;
	// if (portableLinkStat64(unionPath.c_str(), &info) != 0) {
	// 	return false; // file seems to be inaccessible and therefore not editable
	// }
	// ino_t inode_before = info.st_ino;
	// 
	// cout << "inode before: " << inode_before << endl;
	// 
	// // rename the file, so that AUFS gives us the underlying file
	// // from the repository
	// rename(overlayPath.c_str(), (overlayPath + ".tmp.aufs.trick.hastenichtgesehn").c_str());
	// 
	// // read the inode again to see if it changed
	// if (portableLinkStat64(unionPath.c_str(), &info) != 0) {
	// 	cout << errno << endl;
	// 	return false; // file seems to be inaccessible now and was therefore newly created (even if this should be impossible here)
	// }
	// ino_t inode_after = info.st_ino;
	// 
	// cout << "inode after: " << inode_after << endl;
	// 
	// // rename back the file to reset the state to the normal one
	// // from the repository
	// rename((overlayPath + ".tmp.aufs.trick.hastenichtgesehn").c_str(), overlayPath.c_str());
	// 
	// if (inode_before == inode_after) cout << "file was edited" << endl; else cout << "file was replaced" << endl;
	// 
	// return (inode_before == inode_after);
}

UnionFilesystemSync::UnionFilesystemSync(const string &repositoryPath, const std::string &unionPath, const string &overlayPath) {
	mRepositoryPath = canonical_path(repositoryPath);
	mUnionPath = canonical_path(unionPath);
	mOverlayPath = canonical_path(overlayPath);
}

UnionFilesystemSync::~UnionFilesystemSync() {}

bool UnionFilesystemSync::processFoundDirectory(const string &dirPath, const string &filename) {
	if (isNewItem(dirPath, filename)) {
		// everything in a new directory is supposed to be new and can be added without
		// lots of lookups in the repository --> return false to stop seeking recursion
		// in this directory
		addDirectoryRecursively(dirPath, filename);
		return false;
	} else {
		// directory already exists... was just touched, go on with recursion -> return true
		touchDirectory(dirPath, filename);
		return true;
	}
}

void UnionFilesystemSync::processFoundRegularFile(const string &dirPath, const string &filename) {
	// process whiteout prefix
	if (isWhiteoutFilename(filename)) {
		processWhiteoutEntry(dirPath, filename);
	} else if (isNewItem(dirPath, filename)) {
		addRegularFile(dirPath, filename);
	} else {
		// if a file is overwritten by another file it's inodes will change
		// on the other hand the an edited file just changes it's contents
		if (isEditedItem(dirPath, filename)) {
			touchRegularFile(dirPath, filename);
		} else {
			deleteRegularFile(dirPath, filename);
			addRegularFile(dirPath, filename);
		}
	} 
}

void UnionFilesystemSync::processFoundLink(const string &dirPath, const string &filename) {
	if (not checkSymlink(dirPath, filename)) {
		printWarning("found symbolic link pointing outside of repository... skipping that");
		return;
	}
	
	if (isNewItem(dirPath, filename)) {
		addLink(dirPath, filename);
	} else {
		touchRegularFile(dirPath, filename);
	}
}

void UnionFilesystemSync::processWhiteoutEntry(const string &dirPath, const string &filename) {
	// get the name of the file to be deleted and check its state in the repository
	string actualFilename = getFilenameFromWhiteout(filename);
	FileType filetype = getFiletypeInRepository(dirPath, actualFilename);

	switch (filetype) {
		case FT_DIR:
			deleteDirectoryRecursively(dirPath, actualFilename);
			break;
		case FT_SYM:
			deleteSymlink(dirPath, actualFilename);
			break;
		case FT_REG:
			deleteRegularFile(dirPath, actualFilename);
			break;
		default:
			printError("cannot process whiteout entry in AUFS overlay volume");
	}
}

bool UnionFilesystemSync::isNewItem(const string &dirPath, const string &filename) const {
	string fullPath = getPathToRepositoryFile(dirPath, filename);
	PortableStat64 info;
	return (portableFileStat64(fullPath.c_str(), &info) != 0 && errno == ENOENT);
}

void UnionFilesystemSync::deleteDirectoryRecursively(const string &dirPath, const string &filename) {
	RecursionEngine<UnionFilesystemSync> recursion(this, mRepositoryPath);
	
	recursion.foundRegularFile = &UnionFilesystemSync::deleteRegularFile;
	recursion.foundDirectory = &UnionFilesystemSync::deleteDirectory;
	recursion.foundLink = &UnionFilesystemSync::deleteSymlink;
	recursion.caresAbout = &UnionFilesystemSync::isInterestingFilename;
	
	recursion.recurse(getPathToRepositoryFile(dirPath, filename));
	
	deleteDirectory(dirPath, filename);
}

bool UnionFilesystemSync::deleteDirectory(const string &dirPath, const string &filename) {
	mChangeset.dir_rem.insert(getPathToUnionFile(dirPath, filename));
	return true;
}

void UnionFilesystemSync::deleteRegularFile(const string &dirPath, const string &filename) {
	mChangeset.fil_rem.insert(getPathToUnionFile(dirPath, filename));
}

void UnionFilesystemSync::deleteSymlink(const string &dirPath, const string &filename) {
	deleteRegularFile(dirPath, filename); // indistinguishable
}

void UnionFilesystemSync::addDirectoryRecursively(const string &dirPath, const string &filename) {
	addDirectory(dirPath, filename);
	
	RecursionEngine<UnionFilesystemSync> recursion(this, mOverlayPath);
	
	recursion.foundRegularFile = &UnionFilesystemSync::addRegularFile;
	recursion.foundDirectory = &UnionFilesystemSync::addDirectory;
	recursion.foundLink = &UnionFilesystemSync::addLink;
	recursion.caresAbout = &UnionFilesystemSync::isInterestingFilename;
	
	recursion.recurse(getPathToOverlayFile(dirPath, filename));
}

bool UnionFilesystemSync::addDirectory(const string &dirPath, const string &filename) {
	mChangeset.dir_add.insert(getPathToUnionFile(dirPath, filename));
	return true;
}

void UnionFilesystemSync::touchDirectory(const std::string &dirPath, const std::string &filename) {
	mChangeset.dir_touch.insert(getPathToUnionFile(dirPath, filename));
}

void UnionFilesystemSync::addRegularFile(const string &dirPath, const string &filename) {
	mChangeset.reg_add.insert(getPathToUnionFile(dirPath, filename));
}

void UnionFilesystemSync::touchRegularFile(const string &dirPath, const string &filename) {
	mChangeset.reg_touch.insert(getPathToUnionFile(dirPath, filename));
}

void UnionFilesystemSync::addLink(const string &dirPath, const string &filename) {
	mChangeset.sym_add.insert(getPathToUnionFile(dirPath, filename));
}

string UnionFilesystemSync::getPathToRepositoryFile(const string &dirPath, const string &filename) const { 
	return (dirPath.empty()) ? mRepositoryPath + "/" + filename : mRepositoryPath + "/" + dirPath + "/" + filename;
}

string UnionFilesystemSync::getPathToOverlayFile(const string &dirPath, const string &filename) const {
	return (dirPath.empty()) ? mOverlayPath + "/" + filename : mOverlayPath + "/" + dirPath + "/" + filename;
}

string UnionFilesystemSync::getPathToUnionFile(const string &dirPath, const string &filename) const {
	return (dirPath.empty()) ? mUnionPath + "/" + filename : mUnionPath + "/" + dirPath + "/" + filename;
}

FileType UnionFilesystemSync::getFiletypeInRepository(const string &dirPath, const string &filename) const {
	// find the file in the mounted repository and get its file type
	// TODO: replace this file system stat by a catalog lookup
	string path = getPathToRepositoryFile(dirPath, filename);
	return getFileType(path);
}

FileType UnionFilesystemSync::getFileType(const string &path) const {
	PortableStat64 info;
	if (portableLinkStat64(path.c_str(), &info) != 0)
		return FT_ERR;

	if (S_ISDIR(info.st_mode)) return FT_DIR;
	else if (S_ISREG(info.st_mode)) return FT_REG;
	else if (S_ISLNK(info.st_mode)) return FT_SYM;

	return FT_ERR;
}

bool UnionFilesystemSync::checkSymlink(const string &dirPath, const string &filename) {
	string path = getPathToUnionFile(dirPath, filename);
	string basePath = getPathToUnionFile(dirPath, "");
	char buf1[255] = {0};
	char buf2[255] = {0};
	string absPath;
	
	// check if link points to something...
	PortableStat64 info;
	if (portableLinkStat64(path.c_str(), &info) != 0) {
		return false;
	}
	
	// find out if link points inside the repository
	readlink(path.c_str(), buf1, sizeof(buf1));
	rel2abs(buf1, basePath.c_str(), buf2, sizeof(buf2));
	absPath = buf2;
	if (absPath.substr(0, mUnionPath.length()) != mUnionPath) {
		return false;
	}
	
	// if symlink is absolute, make it relative...
	if (*buf1 == '/') {
		memset(buf2, 0, sizeof(buf2));
		abs2rel(absPath.c_str(), basePath.c_str(), buf2, sizeof(buf2));
		
		unlink(path.c_str());
		symlink(buf2, path.c_str());
	}
	
	return true;
}

void UnionFilesystemSync::printError(const string &errorMessage) {
	cerr << "ERROR: " << errorMessage << endl;
}

void UnionFilesystemSync::printWarning(const string &warningMessage) {
	cerr << "Warning: " << warningMessage << endl;
}

template <class T>
RecursionEngine<T>::RecursionEngine(T *delegate, const string &relativeToDirectory) {
	mDelegate = delegate;
	mRelativeToDirectory = canonical_path(relativeToDirectory);
	
	enteringDirectory = NULL;
	leavingDirectory = NULL;
	caresAbout = NULL;
	foundRegularFile = NULL;
	foundDirectory = NULL;
	foundLink = NULL;
}

template <class T>
void RecursionEngine<T>::recurse(const string &dirPath) const {
	assert(enteringDirectory != NULL || leavingDirectory != NULL || caresAbout != NULL || foundRegularFile != NULL || foundDirectory != NULL || foundLink != NULL);
	assert(mRelativeToDirectory.length() == 0 || dirPath.substr(0, mRelativeToDirectory.length()) == mRelativeToDirectory);
	
	doRecursion(dirPath);
}

template <class T>
void RecursionEngine<T>::doRecursion(const string &dirPath) const {
	DIR *dip;
	PortableDirent *dit;
	string filename, relativePath;
	
	// obtain the relative path by cutting away the absolute part
	relativePath = getRelativePath(dirPath);
	
	// get into directory and notify the user
	if ((dip = opendir(dirPath.c_str())) == NULL) {
		return;
	}
	if (enteringDirectory != NULL) (mDelegate->*enteringDirectory)(relativePath);

	while ((dit = portableReaddir(dip)) != NULL) {
		// skip "virtual" directories
		if (strcmp(dit->d_name, ".") == 0 || strcmp(dit->d_name, "..") == 0) {
			continue;
		}
		
		filename = dit->d_name;
		
		// check if user cares about knowing something more
		if (caresAbout != NULL && not (mDelegate->*caresAbout)(filename)) {
			continue;
		}

		// notify user about found directory entries
		switch (dit->d_type) {
			case DT_DIR: // recursion takes place either if "foundDirectory" is NULL or returns true
				if (foundDirectory == NULL || (mDelegate->*foundDirectory)(relativePath, filename))
					doRecursion(dirPath + "/" + filename);
				break;
			case DT_REG:
				if (foundRegularFile != NULL) (mDelegate->*foundRegularFile)(relativePath, filename);
				break;
			case DT_LNK:
				if (foundLink != NULL) (mDelegate->*foundLink)(relativePath, filename);
				break;
		}
	}
	
	// close directory and notify user
	if (closedir(dip) != 0) {
		return;
	}
	if (leavingDirectory != NULL) (mDelegate->*leavingDirectory)(relativePath);
}

template <class T>
string RecursionEngine<T>::getRelativePath(const string &absolutePath) const {
	// be careful of trailing '/' --> ( +1 if needed)
	return (absolutePath.length() == mRelativeToDirectory.length()) ? "" : absolutePath.substr(mRelativeToDirectory.length() + 1);
}
