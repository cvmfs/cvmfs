/**
 * This file is part of the CernVM File System
 *
 * This file defines a class to abstract information retrieval from
 * union file system components.
 *
 * There is a class UnionSync which is the base class for all
 * supported union file systems and provides support for the basic union fs
 * principals.
 *
 * There are three main principals, union file systems follow:
 *   1. Copy on Write
 *      read-only files in CVMFS will be copied up to an overlay
 *      volume on write access.
 *   2. Whiteout
 *      the union file system has to mark files as deleted, if you
 *      remove them from a read only file system
 *   3. Opaque Directories
 *      if you delete an entire directory from the read only file
 *      system and recreate it empty afterwards the union file system
 *      has to mark this directory as opaque to hide all old contents
 *
 * Furthermore, the union file system could create special files
 * for internal bookkeeping which should be ignored.
 *
 * Classes that derive from UnionSync implement the specifics of a concrete
 * union file system (e.g. AUFS, overlayfs).
 */

#ifndef CVMFS_SYNC_UNION_H_
#define CVMFS_SYNC_UNION_H_

#include <inttypes.h>

#include <set>
#include <string>

#include "path_filters/dirtab.h"
#include "sync_item.h"

namespace publish {

class SyncMediator;

/**
 * Interface definition of repository synchronization based on
 * a union filesystem overlay on top of a mounted CVMFS volume.
 */
class SyncUnion {
 public:
  /**
   * @param rdonly_path the absolute path to the mounted cvmfs repository
   * @param union_path the absolute path to the mounted union file system volume
   * @param scratch_path the absolute path to the read write branch attached to
   *        the union file system
   * @param mediator a reference to a SyncMediator object used as bridge to
   *        the actual sync process
   */
  SyncUnion(SyncMediator *mediator,
            const std::string &rdonly_path,
            const std::string &union_path,
            const std::string &scratch_path);
  virtual ~SyncUnion() {}

  /**
   * Initialize internal state of the synchronisation. This needs to be called
   * before running anything else.
   * Note: should be up-called!
   */
  virtual bool Initialize();

  /**
   * Main routine, process scratch space
   */
  virtual void Traverse() = 0;

  /**
   * This produces a SyncItem and initialises it accordingly. This is the only
   * way client code can generate SyncItems to make sure it is always set up
   * properly (see SyncItem::SyncItem() for further details).
   * @param relative_parent_path  the directory path the SyncItem resides in
   * @param filename              file/directory name of directory entry
   * @param entry_type            type of the item in the union directory
   * @return                      a SyncItem object wrapping the dirent
   */
  SyncItem CreateSyncItem(const std::string  &relative_parent_path,
                          const std::string  &filename,
                          const SyncItemType  entry_type) const;

  inline std::string rdonly_path() const { return rdonly_path_; }
  inline std::string union_path() const { return union_path_; }
  inline std::string scratch_path() const { return scratch_path_; }

  /**
   * Whiteout files may have special naming conventions.
   * This method "unmangles" them and retrieves the original file name
   * @param filename the filename as in the scratch directory
   * @return the original filename of the scratched out file in CVMFS repository
   */
  virtual std::string UnwindWhiteoutFilename(const SyncItem &entry) const = 0;

  /**
   * Union file systems use opaque directories to fully support rmdir
   * e.g:   $ rm -rf directory
   *        $ mkdir directory
   * This would produce an opaque directory whose contents are NOT merged with
   * the underlying directory in the read-only branch
   * @param directory the directory to check for opacity
   * @return true if directory is opaque, otherwise false
   */
  virtual bool IsOpaqueDirectory(const SyncItem &directory) const = 0;

  /**
   * Checks if given file is supposed to be whiteout.
   * These files indicate that a specific file has been deleted.
   * @param filename the filename to check
   * @return true if filename seems to be whiteout otherwise false
   */
  virtual bool IsWhiteoutEntry(const SyncItem &entry) const = 0;

  /**
   * Union file systems may use some special files for bookkeeping.
   * They must not show up in to repository and are ignored by the recursion.
   * @param parent directory in which file resides
   * @param filename to decide whether to ignore or not
   * @return true if file should be ignored, othewise false
   */
  virtual bool IgnoreFilePredicate(const std::string &parent_dir,
                                   const std::string &filename) = 0;

  bool IsInitialized() const { return initialized_; }
  virtual bool SupportsHardlinks() const { return false; }

 protected:
  std::string rdonly_path_;
  std::string scratch_path_;
  std::string union_path_;

  SyncMediator *mediator_;

  /**
   * Allow for preprocessing steps before emiting any SyncItems from SyncUnion.
   * This can be overridden by sub-classes but should always be up-called. Typi-
   * cally this sets whiteout and opaque-directory flags or handles hardlinks.
   * @param entry  the SyncItem to be pre-processed
   *               (pointer parameter for google style guide compliance [1])
   * [1] https://google-styleguide.googlecode.com/svn/trunk/
   *             cppguide.html#Function_Parameter_Ordering
   */
  virtual void PreprocessSyncItem(SyncItem *entry) const;

  /**
   * Callback when a regular file is found.
   * @param parent_dir the relative directory path
   * @param filename the filename
   */
  virtual void ProcessRegularFile(const std::string &parent_dir,
                                  const std::string &filename);

  /**
   * Callback when a directory is found.
   * @param parent_dir the relative directory path
   * @param dir_name the filename
   * @return true if file system traversal should branch into
   *         the given directory, false otherwise
   */
  virtual bool ProcessDirectory(const std::string &parent_dir,
                                const std::string &dir_name);

  /**
   * Callback when a symlink is found.
   * @param parent_dir the relative directory path
   * @param link_name the filename
   */
  virtual void ProcessSymlink(const std::string &parent_dir,
                              const std::string &link_name);

  /**
   * Called if the file system traversal enters a directory for processing.
   * @param parent_dir the relative directory path.
   */
  virtual void EnterDirectory(const std::string &parent_dir,
                              const std::string &dir_name);


  /**
   * Called before the file system traversal leaves a processed directory.
   * @param parent_dir the relative directory path.
   */
  virtual void LeaveDirectory(const std::string &parent_dir,
                              const std::string &dir_name);


  /**
   * Called to actually process the file entry.
   * @param entry the SyncItem corresponding to the union file to be processed
   */
  void ProcessFile(const SyncItem &entry);

 private:
  bool initialized_;
};  // class SyncUnion


/**
 * Syncing a cvmfs repository by the help of an overlayed AUFS
 * read-write volume.
 */
class SyncUnionAufs : public SyncUnion {
 public:
  SyncUnionAufs(SyncMediator *mediator,
                const std::string &rdonly_path,
                const std::string &union_path,
                const std::string &scratch_path);

  void Traverse();
  bool SupportsHardlinks() const { return true; }

 protected:
  bool IsWhiteoutEntry(const SyncItem &entry) const;
  bool IsOpaqueDirectory(const SyncItem &directory) const;
  bool IgnoreFilePredicate(const std::string &parent_dir,
                           const std::string &filename);
  std::string UnwindWhiteoutFilename(const SyncItem &entry) const;

 private:
  std::set<std::string> ignore_filenames_;
  std::string whiteout_prefix_;
};  // class SyncUnionAufs


/**
 * Syncing a cvmfs repository by the help of an overlayed overlayfs
 * read-write volume.
 */
class SyncUnionOverlayfs : public SyncUnion {
 public:
  SyncUnionOverlayfs(SyncMediator *mediator,
                     const std::string &rdonly_path,
                     const std::string &union_path,
                     const std::string &scratch_path);

  bool Initialize();

  void Traverse();
  static bool ReadlinkEquals(std::string const &path,
                             std::string const &compare_value);
  static bool HasXattr(std::string const &path, std::string const &attr_name);

 protected:
  void PreprocessSyncItem(SyncItem *entry) const;

  bool IsWhiteoutEntry(const SyncItem &entry) const;
  bool IsOpaqueDirectory(const SyncItem &directory) const;
  bool IsWhiteoutSymlinkPath(const std::string &path) const;

  bool IgnoreFilePredicate(const std::string &parent_dir,
                           const std::string &filename);
  void ProcessCharacterDevice(const std::string &parent_dir,
                              const std::string &filename);
  std::string UnwindWhiteoutFilename(const SyncItem &entry) const;
  std::set<std::string> GetIgnoreFilenames() const;

  void CheckForBrokenHardlink(const SyncItem &entry) const;
  void MaskFileHardlinks(SyncItem *entry) const;

  bool ObtainSysAdminCapability() const;

 private:
  bool IsOpaqueDirPath(const std::string &path) const;

  std::set<std::string> hardlink_lower_files_;
  uint64_t hardlink_lower_inode_;
};  // class SyncUnionOverlayfs

}  // namespace publish

#endif  // CVMFS_SYNC_UNION_H_
