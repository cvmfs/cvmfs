#ifndef CVMFS_SYNC_AUFS_H
#define CVMFS_SYNC_AUFS_H

#include <string>
#include <set>
#include <list>
#include <map>

#include "compat.h"

#include "cvmfs_sync_mediator.h"

namespace cvmfs {
	
	class DirEntry;
	class UnionFileSystemSync;
	
	enum FileType {FT_DIR, FT_REG, FT_SYM, FT_ERR};
	
	/**
	 *  abstract class for interface definition of repository sync based on
	 *  a union filesystem overlay over a mounted CVMFS volume.
	 */
	class UnionFilesystemSync {
	protected:
		std::string mRepositoryPath;
		std::string mOverlayPath;
		std::string mUnionPath;
		
		bool mCheckSymlinks;
		
		SyncMediator *mMediator;
		
		// Singleton
		static UnionFilesystemSync *mInstance;
		
	public:
		virtual ~UnionFilesystemSync();
		
		virtual bool goGetIt() = 0;
		Changeset getChangeset() const { return mMediator->getChangeset(); }
		
		static UnionFilesystemSync *sharedInstance();
		
		inline std::string getRepositoryPath() const { return mRepositoryPath; }
		inline std::string getUnionPath() const { return mUnionPath; }
		inline std::string getOverlayPath() const { return mOverlayPath; }
		
		virtual std::string getWhiteoutPrefix() const = 0;
		
		static void printWarning(const std::string &warningMessage);
		static void printError(const std::string &errorMessage);
		
	protected:
		UnionFilesystemSync(const std::string &repositoryPath, const std::string &unionPath, const std::string &overlayPath);
		
		/**
		 *  checks if the given filename (without path) is interesting for sync
		 *  @param filename the filename to check
		 *  @return true if interesting otherwise false
		 */
		inline bool isInterestingFilename(const std::string &filename) { return not isIgnoredFilename(filename); }
		
		/**
		 *  checks if a filename can be ignored while reading out the overlay directory
		 *  @param filename the filename to check
		 *  @return true if the file should be ignored otherwise false
		 */ 
		virtual bool isIgnoredFilename(const std::string &filename) const = 0;
		
		/**
		 *  checks if given filename (without path) is supposed to be whiteout
		 *  these files show the union file system, that a specific file should appear as deleted
		 *  @param filename the filename to check
		 *  @return true if filename seems to be whiteout otherwise false
		 */
		virtual bool isWhiteoutEntry(const DirEntry *entry) const = 0;

		/**
		 *  callback method for the main recursion when a regular file is found
		 *  @param dirPath the relative directory path
		 *  @param filename the filename
		 */
		virtual void processFoundRegularFile(DirEntry *entry);
		
		/**
		 *  callback method for the main recursion when a directory is found
		 *  @param dirPath the relative directory path
		 *  @param filename the filename
		 *  @return true if recursion should dig into the given directory, false otherwise
		 */
		virtual bool processFoundDirectory(DirEntry *entry);
		
		/**
		 *  callback method for the main recursion when a symlink is found
		 *  @param dirPath the relative directory path
		 *  @param filename the filename
		 */
		virtual void processFoundSymlink(DirEntry *entry);
		
		virtual void enteringDirectory(DirEntry *entry);
		virtual void leavingDirectory(DirEntry *entry);
	};
	
	/**
	 *  syncing a CVMFS repository by the help of an overlayed AUFS 1.x read-write volume
	 */
	class SyncAufs1 :
	 	public UnionFilesystemSync {
	private:
		std::set<std::string> mIgnoredFilenames;
		std::string mWhiteoutPrefix;
	
	public:
		virtual ~SyncAufs1();
		
		bool goGetIt();
		static void initialize(const std::string &repositoryPath, const std::string &unionPath, const std::string &aufsPath);
		
		inline std::string getWhiteoutPrefix() const { return mWhiteoutPrefix; }
		
	protected:
		SyncAufs1(const std::string &repositoryPath, const std::string &unionPath, const std::string &aufsPath);
		
		bool isWhiteoutEntry(const DirEntry *entry) const;
		int getHardlinkCount(const std::string &dirPath, const std::string filename) const;
		
		inline bool isIgnoredFilename(const std::string &filename) const { return (mIgnoredFilenames.find(filename) != mIgnoredFilenames.end()); }
		virtual bool isEditedItem(const std::string &dirPath, const std::string &filename) const;
		
		void copyUpHardlinks(const std::string &dirPath, const std::string &filename);
		
	private:
	};
}

#endif /* CVMFS_SYNC_AUFS_H */
