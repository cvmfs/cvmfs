/**
 *  This file defines a class to abstract information retrieval from
 *  union file system components.
 *
 *  Here you have a class UnionSync which is the base class for all
 *  supported union file systems and provides some basic principals.
 *
 *  There are three main things, an union file system has to do:
 *    1. Copy on Write
 *       read-only files in CVMFS will be copied up to an overlay
 *       volume on write access.
 *    2. Whiteout
 *       the union file system has to mark files as deleted, if you
 *       remove them from a read only file system
 *    3. Opaque Directories
 *       if you delete an entire directory from the read only file
 *       system and recreate it empty afterwards the union file system
 *       has to mark this directory as opaque to hide all old contents
 *
 *  Furthermore the union file system could create special files
 *  which we don't care about.
 *
 *  All these intrinsics are encapsulated and handled be the classes in
 *  derived from UnionSync
 *
 *  Developed by René Meusel 2011 at CERN
 *  rene@renemeusel.de
 */

#ifndef SYNC_UNION_H
#define SYNC_UNION_H 1

#include "cvmfs_sync_recursion.h"

namespace cvmfs {

class SyncItem;
class SyncMediator;

enum FileType {
  FT_DIR,
  FT_REG,
  FT_SYM,
  FT_ERR
};

/**
 *  abstract class for interface definition of repository sync based on
 *  a union filesystem overlay on top of a mounted CVMFS volume.
 *
 *  this class defines more or less just the interface for union based sync
 *  to use it you have to initialize a concrete implementation of it.
 */
class SyncUnion {
 protected:
	std::string mRepositoryPath;
  std::string mOverlayPath;
	std::string mUnionPath;

	SyncMediator *mMediator;

 public:
 	/**
 	 *  create a new instance of SyncUnion
 	 *  @param repositoryPath the absolute path to the mounted cvmfs repository
 	 *  @param unionPath the absolute path to the mounted union file system volume
 	 *  @param overlayPath the absolute path to the read write branch attached to the union file system
 	 *  @param mediator a reference to a SyncMediator object used as bridge to the actual sync process
 	 */
 	SyncUnion(SyncMediator *mediator,
 	          const std::string &repository_path,
            const std::string &union_path,
            const std::string &overlay_path);
 	virtual ~SyncUnion() {}

	/**
	 *  after everything is set up properly, just call this and hold fast
	 */
	virtual bool DoYourMagic() = 0;

	/** @return the path to the CVMFS repository */
	inline std::string GetRepositoryPath() const { return mRepositoryPath; }

	/** @return the path to the union volume (mounted union file system) */
	inline std::string GetUnionPath() const { return mUnionPath; }

	/** @return the path to the overlay directory of the union file system (write overlay) */
	inline std::string GetOverlayPath() const { return mOverlayPath; }

	/**
	 *  whiteout files may have special naming conventions
	 *  this method scratches them and retrieves the original file name
	 *  @param filename the filename to be mangled
	 *  @return the original filename of the scratched out file in CVMFS repository
	 */
	virtual std::string UnwindWhiteoutFilename(const std::string &filename) const = 0;

	/**
	 *  union file systems use opaque directories to fully support rmdir
	 *  f.e:   $ rm -rf directory
	 *         $ mkdir directory
	 *  this would produce an opaque directory whose contents are NOT merged with the underlying
	 *  directory in the read-only branch
	 *  @param directory the directory to check for opacity
	 *  @return true if directory is opaque, otherwise false
	 */
	virtual bool IsOpaqueDirectory(const SyncItem *directory) const = 0;

	/**
	 *  checks if given file is supposed to be whiteout
	 *  these files show the union file system, that a specific file in the read-only branch
	 *  should appear as deleted
	 *  @param filename the filename to check
	 *  @return true if filename seems to be whiteout otherwise false
	 */
	virtual bool IsWhiteoutEntry(const SyncItem &entry) const = 0;

	/**
	 *  union file systems may use some special files for bookkeeping
	 *  they must not show up in to repository and are ignored by the recursion
	 *  @return a set of filenames to be ignored
	 */
	virtual std::set<std::string> GetIgnoredFilenames() const = 0;

 protected:

	/**
	 *  callback method for the main recursion when a regular file is found
	 *  @param dirPath the relative directory path
	 *  @param filename the filename
	 */
	virtual void ProcessFoundRegularFile(const std::string &parent_dir,
	                                     const std::string &file_name);

	/**SyncItem
	 *  callback method for the main recursion when a directory is found
	 *  @param dirPath the relative directory path
	 *  @param filename the filename
	 *  @return true if recursion should dig into the given directory, false otherwise
	 */
	virtual bool ProcessFoundDirectory(const std::string &parent_dir,
 	                                   const std::string &dir_name);

	/**
	 *  callback method for the main recursion when a symlink is found
	 *  @param dirPath the relative directory path
	 *  @param filename the filename
	 */
	virtual void ProcessFoundSymlink(const std::string &parent_dir,
 	                                 const std::string &link_name);

	/**
	 *  called if the main recursion enters a directory for further recursion
	 *  @param entry the directory which was stepped into
	 */
	virtual void EnteringDirectory(const std::string &parent_dir,
 	                               const std::string &dir_name);

	/**
	 *  called before the main recursion leaves the directory after recursing it
	 *  @param entry the directory which was stepped out
	 */
	virtual void LeavingDirectory(const std::string &parent_dir,
 	                              const std::string &dir_name);

 private:
   void ProcessFoundFile(SyncItem &entry);
};
}

#endif /* SYNC_UNION_H */
