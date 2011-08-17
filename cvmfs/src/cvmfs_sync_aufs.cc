#include "cvmfs_sync_aufs.h"

#include "util.h"

#include <iostream> // remove later
#include <dirent.h>
#include <errno.h>
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <sstream>

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
	recursion.foundSymlink = &SyncAufs1::processFoundSymlink;
	recursion.caresAbout = &SyncAufs1::isInterestingFilename;
	
	recursion.recurse(mOverlayPath);
	
	return true;
}

bool SyncAufs1::isEditedItem(const string &dirPath, const string &filename) const {
	return false; // at the moment there is no way to distinguish between overwritten
	              // and edited files... we are assuming every file to be overwritten
	              // this breaks inode persistency, but we have to live with that atm
}

void SyncAufs1::copyUpHardlinks(const std::string &dirPath, const std::string &filename) {
	// get inode of file
	unsigned int inode = statFileInUnionVolume(dirPath, filename).st_ino;
	
	// go through directory and search for the same inode
	string pathToDirectory = getPathToUnionFile(dirPath, "");
	DIR *dip;
	PortableDirent *dit;
	string filenameInDirectory;
	unsigned int inodeInDirectory;
	if ((dip = opendir(pathToDirectory.c_str())) == NULL) {
		return;
	}
	
	// create a hardlink group which is created at once in the end
	HardlinkGroup hardlinks;
	hardlinks.masterFile = getPathToUnionFile(dirPath, filename);
	
	while ((dit = portableReaddir(dip)) != NULL) {
		filenameInDirectory = dit->d_name;
		inodeInDirectory = statFileInUnionVolume(dirPath, filenameInDirectory).st_ino;
		
		if (inodeInDirectory == inode) {
			// the complete group of hardlinks will be replaced
			// old ones are deleted and afterwards the complete group is recreated
			if (not isNewItem(dirPath, filenameInDirectory)) {
				deleteRegularFile(dirPath, filenameInDirectory);
			}
			hardlinks.hardlinks.push_back(getPathToUnionFile(dirPath, filenameInDirectory));
		}
	}
	
	closedir(dip);
	
	// the hardlink group is built up and will be recreated
	addHardlinkGroup(hardlinks);
}

UnionFilesystemSync::UnionFilesystemSync(const string &repositoryPath, const std::string &unionPath, const string &overlayPath) {
	mRepositoryPath = canonical_path(repositoryPath);
	mUnionPath = canonical_path(unionPath);
	mOverlayPath = canonical_path(overlayPath);
	mCheckSymlinks = false;
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
		
	// process hardlink
	} else if (statFileInUnionVolume(dirPath, filename).st_nlink > 1) {
		// there must be hard links which has to be updated as well
		// (currently hardlinks are only supported in the same directory)
		copyUpHardlinks(dirPath, filename);
		
	// process normal file
	} else {
		if (isNewItem(dirPath, filename)) {
			addRegularFile(dirPath, filename);
		} else {
			// if a file is overwritten by another file it's inodes will change
			// on the other hand an edited file just changes it's contents
			if (isEditedItem(dirPath, filename)) {
				touchRegularFile(dirPath, filename);
			} else {
				deleteRegularFile(dirPath, filename);
				addRegularFile(dirPath, filename);
			}
		}
	} 
}

void UnionFilesystemSync::processFoundSymlink(const string &dirPath, const string &filename) {
	if (not checkSymlink(dirPath, filename)) {
		printWarning("found symbolic link pointing outside of repository... skipping that");
		return;
	}
	
	if (isNewItem(dirPath, filename)) {
		addSymlink(dirPath, filename);
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

PortableStat64 UnionFilesystemSync::statFileInUnionVolume(const std::string &dirPath, const std::string filename) const {
	string path = getPathToUnionFile(dirPath, filename);

	PortableStat64 info;
	if (portableFileStat64(path.c_str(), &info) != 0) {
		stringstream ss;
		ss << "could not stat file " << path;
		printWarning(ss.str());
		return info;
	}

	return info;
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
	recursion.foundSymlink = &UnionFilesystemSync::deleteSymlink;
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
	recursion.foundSymlink = &UnionFilesystemSync::addSymlink;
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

void UnionFilesystemSync::addSymlink(const string &dirPath, const string &filename) {
	mChangeset.sym_add.insert(getPathToUnionFile(dirPath, filename));
}

void UnionFilesystemSync::touchSymlink(const string &dirPath, const string &filename) {
	touchRegularFile(dirPath, filename); // indistinguishable
}

void UnionFilesystemSync::addHardlinkGroup(const HardlinkGroup hardlinks) {
	cout << "Hardlink Group added" << endl;
	cout << "master file: " << hardlinks.masterFile << endl;
	cout << "hardlinks: " << endl;
	
	list<string>::const_iterator begin, end;
	begin = hardlinks.hardlinks.begin();
	end = hardlinks.hardlinks.end();
	for (; begin != end; begin++) {
		cout << *begin << endl;
	}
	
	mChangeset.hardlink_add.push_back(hardlinks);
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
	if (not mCheckSymlinks) {
		return true;
	}
	
	// checking...
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

void UnionFilesystemSync::printError(const string &errorMessage) const {
	cerr << "ERROR: " << errorMessage << endl;
}

void UnionFilesystemSync::printWarning(const string &warningMessage) const {
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
	foundSymlink = NULL;
}

template <class T>
void RecursionEngine<T>::recurse(const string &dirPath) const {
	assert(enteringDirectory != NULL || leavingDirectory != NULL || caresAbout != NULL || foundRegularFile != NULL || foundDirectory != NULL || foundSymlink != NULL);
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
				if (foundSymlink != NULL) (mDelegate->*foundSymlink)(relativePath, filename);
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
