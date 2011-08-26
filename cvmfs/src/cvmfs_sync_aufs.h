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
	
	enum FileType {FT_DIR, FT_REG, FT_SYM, FT_ERR};
	
	/**
	 *  abstract class for interface definition of repository sync based on
	 *  a union filesystem overlay over a mounted CVMFS volume.
	 *
	 *  this class is implemented as singleton and defines more or less just the interface
	 *  to use it you have to initialize a concrete implementation of it. Afterwards
	 *  you can use UnionSync::sharedInstance()->...
	 */
	class UnionSync {
	protected:
		std::string mRepositoryPath;
		std::string mOverlayPath;
		std::string mUnionPath;
		
		SyncMediator *mMediator;
		
		// Singleton
		static UnionSync *mInstance;
		
	public:
		/**
		 *  after everything is set up properly, just call this and hold fast
		 */
		virtual bool doYourMagic() = 0;
		
		/** singleton... get shared instance (will die, if no concrete implentation was initialized)*/
		static UnionSync *sharedInstance();
		
		/** clean up */
		virtual void fini();
		
		inline std::string getRepositoryPath() const { return mRepositoryPath; }
		inline std::string getUnionPath() const { return mUnionPath; }
		inline std::string getOverlayPath() const { return mOverlayPath; }
		
		virtual std::string unwindWhiteoutFilename(const std::string &filename) const = 0;
		
		/**
		 *  union file systems use opaque directories to fully support rmdir
		 *  f.e:   $ rm -rf directory
		 *         $ mkdir directory
		 *  this would produce an opaque directory whose contents are NOT merged with the underlying
		 *  directory in the read-only branch
		 *  @param directory the directory to check for opacity
		 *  @return true if directory is opaque, otherwise false
		 */
		virtual bool isOpaqueDirectory(const DirEntry *directory) const = 0;
		
		/**
		 *  checks if given file is supposed to be whiteout
		 *  these files show the union file system, that a specific file in the read-only branch
		 *  should appear as deleted
		 *  @param filename the filename to check
		 *  @return true if filename seems to be whiteout otherwise false
		 */
		virtual bool isWhiteoutEntry(const DirEntry *entry) const = 0;
		
		/**
		 *  union file systems may use some special files for bookkeeping reasons
		 *  they must not show up in to repository and are ignored by the recursion
		 *  @return a set of filenames to be ignored
		 */
		virtual std::set<std::string> getIgnoredFilenames() const = 0;

		virtual ~UnionSync();
		
	protected:
		/**
		 *  the constructor of this class is protected because it is implementing the singleton pattern
		 *  @param repositoryPath the absolute path to the mounted cvmfs repository
		 *  @param unionPath the absolute path to the mounted union file system volume
		 *  @param overlayPath the absolute path to the read write branch attached to the union file system
		 *  @param mediator a reference to a SyncMediator object used as bridge to the actual sync process
		 */
		UnionSync(const std::string &repositoryPath, const std::string &unionPath, const std::string &overlayPath, SyncMediator *mediator);
		
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
		virtual RecursionPolicy processFoundDirectory(DirEntry *entry);
		
		/**
		 *  callback method for the main recursion when a symlink is found
		 *  @param dirPath the relative directory path
		 *  @param filename the filename
		 */
		virtual void processFoundSymlink(DirEntry *entry);
		
		/**
		 *  called if the main recursion enters a directory for further recursion
		 *  @param entry the directory which was stepped into
		 */
		virtual void enteringDirectory(DirEntry *entry);
		
		/**
		 *  called before the main recursion leaves the directory after recursing it
		 *  @param entry the directory which was stepped out
		 */
		virtual void leavingDirectory(DirEntry *entry);
	};
	
	/**
	 *  syncing a CVMFS repository by the help of an overlayed AUFS 1.x read-write volume
	 */
	class SyncAufs1 :
	 	public UnionSync {
	private:
		std::set<std::string> mIgnoredFilenames;
		std::string mWhiteoutPrefix;
	
	public:
		virtual ~SyncAufs1();
		
		bool doYourMagic();
		static void initialize(const std::string &repositoryPath, const std::string &unionPath, const std::string &aufsPath, SyncMediator *mediator);
		
		inline bool isWhiteoutEntry(const DirEntry *entry) const { return (entry->getFilename().substr(0,mWhiteoutPrefix.length()) == mWhiteoutPrefix); }
		inline bool isOpaqueDirectory(const DirEntry *directory) const { return file_exists(directory->getOverlayPath() + "/.wh..wh..opq"); }
		inline std::string unwindWhiteoutFilename(const std::string &filename) const { return filename.substr(mWhiteoutPrefix.length()); }
		
		inline std::set<std::string> getIgnoredFilenames() const { return mIgnoredFilenames; };
		
	protected:
		SyncAufs1(const std::string &repositoryPath, const std::string &unionPath, const std::string &aufsPath, SyncMediator *mediator);
	};
}

#endif /* CVMFS_SYNC_AUFS_H */
