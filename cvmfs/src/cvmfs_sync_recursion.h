#ifndef CVMFS_SYNC_RECURSION_H
#define CVMFS_SYNC_RECURSION_H 1

#include <string>
#include <assert.h>
#include <list>
#include <set>
#include <stdint.h>

#include <iostream> // TODO: remove me

#include "compat.h"
#include "util.h"
#include "hash.h"
#include "DirectoryEntry.h"

namespace cvmfs {

class UnionSync;

enum SyncItemType {
	DE_DIR,
	DE_FILE,
	DE_SYMLINK
};

/**
 *  This base class implements a reference counting scheme
 *  inspired by NSObjects in Objective-C by Apple.
 *  After object creation, these objects have a reference count of 1
 *  retain() increments release() decrements this counter.
 *  If the reference counter hits 0 the object will be deleted.
 *
 *  This class ONLY works for objects created by 'new' and therefore reside on the heap!!
 *
 *  BE CAREFUL: This is not thread safe at the moment!
 *
 *  Best practices: If you save an RefCntObject in your class, retain it.
 *                  When the object is just passed through don't retain it.
 *                  Everytime you loose the reference to the object release it.
 *                  Take special care of setters, they have to release the old object and retain the new one
 */
class RefCntObject {
private:
   unsigned int mReferenceCount;

public:
   inline RefCntObject() { mReferenceCount = 1; }
   virtual ~RefCntObject() {};
   
   inline void retain() { ++mReferenceCount; }
   inline void release() { --mReferenceCount; if (mReferenceCount == 0) delete this; }
};

/**
 *  any directory entry emitted by the RecursionEngine is wrapped in a 
 *  convenient SyncItem structure
 *  This class represents potentially three concrete files:
 *    - <repository path>/<filename>
 *    - <read-write branch>/<filename>
 *    - <union volume path>/<filename>
 *  The main purpose of this class is to cache stat calls to the underlying
 *  files in the different locations as well as hiding some implementation details
 */
class SyncItem :
   public RefCntObject {
private:
	SyncItemType mType;

	/**
	 *  structure to cache stat calls to the different file locations
	 *  obtained: false at the beginning, after first stat call it is true
	 *  errorCode: the errno value after the stat call responded != 0
	 *  stat: the actual stat structure
	 */
	typedef struct {
		bool obtained;
		int errorCode;
		PortableStat64 stat;
	} EntryStat;

	mutable EntryStat mRepositoryStat;
	mutable EntryStat mUnionStat;
	mutable EntryStat mOverlayStat;
	
	bool mWhiteout;

	std::string mRelativeParentPath;
	std::string mFilename;
	
	/** the hash of the file's content (computed by the SyncMediator) */
	hash::t_sha1 mContentHash;

public:
	/**
	 *  create a new SyncItem (is normally not required for normal usage
	 *                         as the RecursionEngine provides you with DirEntries)
	 *  @param dirPath the RELATIVE path to the file
	 *  @param filename the name of the file ;-)
	 *  @param entryType well...
	 */
	SyncItem(const std::string &dirPath, const std::string &filename, const SyncItemType entryType);
	virtual ~SyncItem();

	inline bool isDirectory() const { return mType == DE_DIR; }
	inline bool isRegularFile() const { return mType == DE_FILE; }
	inline bool isSymlink() const { return mType == DE_SYMLINK; }
	inline bool isWhiteout() const { return mWhiteout; }
	inline bool isCatalogRequestFile() const { return mFilename == ".cvmfscatalog"; }
	bool isOpaqueDirectory() const;
	
	inline hash::t_sha1 getContentHash() const { return mContentHash; }
	inline void setContentHash(hash::t_sha1 &hash) { mContentHash = hash; }
	inline bool hasContentHash() { return mContentHash != hash::t_sha1(); }
	
	inline std::string getFilename() const { return mFilename; }
	inline std::string getParentPath() const { return mRelativeParentPath; }
  DirectoryEntry createDirectoryEntry() const;

	inline std::string getRelativePath() const { return (mRelativeParentPath.empty()) ? mFilename : mRelativeParentPath + "/" + mFilename; }
	std::string getRepositoryPath() const;
	std::string getUnionPath() const;
	std::string getOverlayPath() const;

	void markAsWhiteout();
	
	unsigned int getUnionLinkcount() const;
	uint64_t getUnionInode() const;
	inline PortableStat64 getUnionStat() const { statUnion(); return mUnionStat.stat; };
	bool isNew() const;
	
	inline bool isEqualTo(const SyncItem *otherEntry) const { return (getRelativePath() == otherEntry->getRelativePath()); }

private:
	// lazy evaluation and caching of results of file stats
	inline void statRepository() const { if (mRepositoryStat.obtained) return; statGeneric(getRepositoryPath(), &mRepositoryStat); } 
	inline void statUnion() const { if (mUnionStat.obtained) return; statGeneric(getUnionPath(), &mUnionStat); } 
	inline void statOverlay() const { if (mOverlayStat.obtained) return; statGeneric(getOverlayPath(), &mOverlayStat); } 
	void statGeneric(const std::string &path, EntryStat *statStructure) const;
};

typedef std::list<SyncItem*> SyncItemList;

/**
 *  the foundDirectory-Callback can decide if the recursion engine should recurse into
 *  the recently found directory. It has to communicate it's decision by returning one
 *  of these enum elements
 */
enum RecursionPolicy {
	RP_RECURSE,
	RP_DONT_RECURSE
};

/**
 *  @brief a simple recursion engine to abstract the recursion of directories.
 *  It provides several callback hooks to instrument and control the recursion.
 *  Hooks will be called on the provided delegate object which has to be of type T
 *
 *  Found directory entries are sent back to the delegate object as a pointer to a
 *  SyncItem structure. SyncItem objects are reference counted and the recursion
 *  engine calls release() on them, after sending the callback.
 *  !! If you want to store a SyncItem for longer than the processing of the call-
 *     back, you have to retain() it. After that you are responsible for freeing
 *     your retained SyncItem by calling release().
 */
template <class T>
class RecursionEngine {
private:
	/** the delegate all hooks are called on */
	T *mDelegate;

	/** dirPath in callbacks will be relative to this directory */
	std::string mRelativeToDirectory;
	bool mRecurse;
	
	/** if one of these files are found somewhere they are completely ignored */
	std::set<std::string> mIgnoredFiles;

public:
	/** callback if a directory is entered by the recursion */
	void (T::*enteringDirectory)(SyncItem *entry);

	/** callback if a directory is left by the recursion */
	void (T::*leavingDirectory)(SyncItem *entry);

	/** callback if a file was found */
	void (T::*foundRegularFile)(SyncItem *entry);

	/**
	 *  callback if a directory was found
	 *  depending on the response of the callback, the recursion will continue in the found directory
	 *  if this callback is not specified, it will recurse by default!
	 */
	RecursionPolicy (T::*foundDirectory)(SyncItem *entry);
	
	/**
	 *  callback for a found directory after it was already recursed
	 *  e.g. for deletion of directories (first delete content, then the directory itself)
	 */
	void (T::*foundDirectoryAfterRecursion)(SyncItem *entry);

	/** callback if a symlink was found */
	void (T::*foundSymlink)(SyncItem *entry);

public:
	/**
	 *  create a new recursion engine
	 *  @param delegate the object which will receive the callbacks
	 *  @param relativeToDirectory the DirEntries will be created relative to this directory
	 *  @param ignoredFiles a list of files which the delegate DOES NOT care about (no callback calls or recursion for them)
	 */
	RecursionEngine(T *delegate, const std::string &relativeToDirectory, std::set<std::string> ignoredFiles);
	
	/**
	 *  create a new recursion engine
	 *  @param delegate the object which will receive the callbacks
	 *  @param relativeToDirectory the DirEntries will be created relative to this directory
	 *  @param ignoredFiles a list of files which the delegate DOES NOT care about (no callback calls or recursion for them)
	 *  @param recurse should the recursion engine recurse at all? (if not, it basically just traverses the given directory)
	 */
	RecursionEngine(T *delegate, const std::string &relativeToDirectory, std::set<std::string> ignoredFiles, bool recurse);

	/**
	 *  start the recursion
	 *  @param dirPath the directory to start the recursion at
	 */
	void recurse(const std::string &dirPath) const;

private:
	void doRecursion(SyncItem *entry) const;

	bool notifyForDirectory(SyncItem *entry) const;
	void notifyForDirectoryAfterRecursion(SyncItem *entry) const;
	void notifyForRegularFile(const std::string &dirPath, const std::string &filename) const;
	void notifyForSymlink(const std::string &dirPath, const std::string &filename) const;
	
	void init(T *delegate, const std::string &relativeToDirectory, std::set<std::string> ignoredFiles, bool recurse);

	std::string getRelativePath(const std::string &absolutePath) const;
};

/**********************************
 ** Template implementation
 **********************************/

template <class T>
RecursionEngine<T>::RecursionEngine(T *delegate, const std::string &relativeToDirectory, std::set<std::string> ignoredFiles) {
	init(delegate, relativeToDirectory, ignoredFiles, true);
}

template <class T>
RecursionEngine<T>::RecursionEngine(T *delegate, const std::string &relativeToDirectory, std::set<std::string> ignoredFiles, bool recurse) {
	init(delegate, relativeToDirectory, ignoredFiles, recurse);
}

template <class T>
void RecursionEngine<T>::init(T *delegate, const std::string &relativeToDirectory, std::set<std::string> ignoredFiles, bool recurse) {
	mDelegate = delegate;
	mRelativeToDirectory = canonical_path(relativeToDirectory);
	mRecurse = recurse;
	mIgnoredFiles = ignoredFiles;
	
	// we definitely don't care about these "virtual" directories
	mIgnoredFiles.insert(".");
	mIgnoredFiles.insert("..");

	// default values for callback methods
	enteringDirectory = NULL;
	leavingDirectory = NULL;
	foundRegularFile = NULL;
	foundDirectory = NULL;
	foundDirectoryAfterRecursion = NULL;
	foundSymlink = NULL;
}

template <class T>
void RecursionEngine<T>::recurse(const std::string &dirPath) const {
	assert(enteringDirectory != NULL || leavingDirectory != NULL || foundRegularFile != NULL || foundDirectory != NULL || foundSymlink != NULL);
	assert(mRelativeToDirectory.length() == 0 || dirPath.substr(0, mRelativeToDirectory.length()) == mRelativeToDirectory);

	std::string relativePath = getRelativePath(dirPath);
	SyncItem *directory = new SyncItem(get_parent_path(relativePath), get_file_name(relativePath), DE_DIR);
	doRecursion(directory);
   directory->release();
}

template <class T>
void RecursionEngine<T>::doRecursion(SyncItem *entry) const {
	DIR *dip;
	PortableDirent *dit;
	std::string filename, absolutePath;

	// obtain the absolute path by adding the relative portion
	absolutePath = mRelativeToDirectory + "/" + entry->getRelativePath();
	// get into directory and notify the user
	if ((dip = opendir(absolutePath.c_str())) == NULL) {
		return;
	}
	if (enteringDirectory != NULL) (mDelegate->*enteringDirectory)(entry);

	while ((dit = portableReaddir(dip)) != NULL) {
		filename = dit->d_name;
		
		// check if filename is included in the ignored files list
		if (mIgnoredFiles.find(filename) != mIgnoredFiles.end()) {
			continue;
		}

		// notify user about found directory entry
		switch (dit->d_type) {
			case DT_DIR:
				// we are only creating a new directory entry if the user wants to see it, if has
				// to be used for recursion or both... otherwise we are skipping the whole stuff
				if (foundDirectory != NULL || mRecurse) {
					SyncItem *newEntry = new SyncItem(entry->getRelativePath(), filename, DE_DIR);
					if (notifyForDirectory(newEntry)) {
						doRecursion(newEntry); // user can decide to skip directories from recursion
					}
					notifyForDirectoryAfterRecursion(newEntry);
               newEntry->release();
				}
				break;
			case DT_REG:
				notifyForRegularFile(entry->getRelativePath(), filename);
				break;
			case DT_LNK:
				notifyForSymlink(entry->getRelativePath(), filename);
				break;
		}
	}

	// close directory and notify user
	if (closedir(dip) != 0) {
		return;
	}
	if (leavingDirectory != NULL) (mDelegate->*leavingDirectory)(entry);
}

template <class T>
bool RecursionEngine<T>::notifyForDirectory(SyncItem *entry) const {
	bool recurse = true;
	if (foundDirectory != NULL) {
		recurse = (mDelegate->*foundDirectory)(entry);
	}
	
	// we are only recursing, if it is generally enabeld (mRecurse) and if the user wants us to
	return ((recurse == RP_RECURSE) && mRecurse);
}

template <class T>
void RecursionEngine<T>::notifyForDirectoryAfterRecursion(SyncItem *entry) const {
	if (foundDirectoryAfterRecursion != NULL) {
		(mDelegate->*foundDirectoryAfterRecursion)(entry);
	}
}

template <class T>
void RecursionEngine<T>::notifyForRegularFile(const std::string &dirPath, const std::string &filename) const {
	if (foundRegularFile == NULL) {
		return;
	}

	SyncItem *entry = new SyncItem(dirPath, filename, DE_FILE);
	(mDelegate->*foundRegularFile)(entry);
   entry->release();
}

template <class T>
void RecursionEngine<T>::notifyForSymlink(const std::string &dirPath, const std::string &filename) const {
	if (foundSymlink == NULL) {
		return;
	}

	SyncItem *entry = new SyncItem(dirPath, filename, DE_SYMLINK);
	(mDelegate->*foundSymlink)(entry);
   entry->release();
}

template <class T>
std::string RecursionEngine<T>::getRelativePath(const std::string &absolutePath) const {
	// be careful of trailing '/' --> ( +1 if needed)
	return (absolutePath.length() == mRelativeToDirectory.length()) ? "" : absolutePath.substr(mRelativeToDirectory.length() + 1);
}

}

#endif
