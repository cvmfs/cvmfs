/**
 * This file is part of the CernVM File System.
 *
 * The SyncMediator is an intermediate layer between the UnionSync
 * implementation and the CatalogManager.
 * It's main responsibility is to unwind file system intrinsics as
 * deleting all files in a deleted directory. Furthermore newly
 * created directories are recursed and all included files are
 * added as a whole (performance improvement).
 *
 * Furthermore it keeps track of hard link relations.  As we cannot store the
 * transient inode of the union file system in cvmfs, we just keep track of
 * hard link relations itself.  Inodes will be assigned at run time of the CVMFS
 * client taking these relations into account.
 *
 * Another responsibility of this class is the creation and destruction
 * of nested catalogs.  If a .cvmfscatalog magic file is encountered,
 * either on delete or add, it will be treated as nested catalog change.
 *
 * New and modified files are piped to external processes for hashing and
 * compression.  Results come back in a pipe.
 */

#ifndef CVMFS_SYNC_MEDIATOR_H_
#define CVMFS_SYNC_MEDIATOR_H_

#include <pthread.h>

#include <list>
#include <string>
#include <map>
#include <stack>

#include "platform.h"
#include "catalog_mgr_rw.h"
#include "cvmfs_sync.h"
#include "sync_item.h"

class Manifest;

namespace publish {

/**
 * If we encounter a file with linkcount > 1 it will be added to a HardlinkGroup
 * After processing all files, the HardlinkGroups are populated with
 * related hardlinks
 * Assertion: linkcount == HardlinkGroup::hardlinks.size() at the end
 */
struct HardlinkGroup {
  HardlinkGroup(SyncItem &item) : master(item) {
    hardlinks[master.GetRelativePath()] = master;
  }

  void AddHardlink(SyncItem &entry) {
    hardlinks[entry.GetRelativePath()] = entry;
  }

  SyncItem master;
  SyncItemList hardlinks;
};

/**
 * Mapping of inode number to the related HardlinkGroup.
 */
typedef std::map<uint64_t, HardlinkGroup> HardlinkGroupMap;


/**
 * Callback object for newly added files.  The callback sets the hash.
 */
class SyncMediator;
class PublishFilesCallback : public upload::SpoolerCallback {
 public:
  PublishFilesCallback(SyncMediator *mediator);
  void Callback(const std::string &path, int retval,
                const std::string &digest);
 private:
  SyncMediator *mediator_;
};


/**
 * The SyncMediator refines the input received from a concrete UnionSync object.
 * For example, it resolves the insertion and deletion of complete directories
 * by recursing them.  It works as a mediator between the union file system and
 * forwards the correct database commands to the catalog handler to sync the
 * changes into the repository.
 * Furthermore it sends new and modified files to the spooler for compression
 * and hashing.
 */
class SyncMediator {
  friend class PublishFilesCallback;
  friend class SyncUnion;
 public:
  SyncMediator(catalog::WritableCatalogManager *catalogManager,
               const SyncParameters *params);

  void Add(SyncItem &entry);
  void Touch(SyncItem &entry);
  void Remove(SyncItem &entry);
  void Replace(SyncItem &entry);

	void EnterDirectory(SyncItem &entry);
  void LeaveDirectory(SyncItem &entry);

  Manifest *Commit();

 private:
  typedef std::stack<HardlinkGroupMap> HardlinkGroupMapStack;
  typedef std::list<HardlinkGroup> HardlinkGroupList;

  void RegisterUnionEngine(SyncUnion *engine) {
    union_engine_ = engine;
  }

  // Called after figuring out the type of a path (file, symlink, dir)
  void AddFile(SyncItem &entry);
  void RemoveFile(SyncItem &entry);
  void TouchFile(SyncItem &entry);

  void AddDirectory(SyncItem &entry);
  void RemoveDirectory(SyncItem &entry);
  void TouchDirectory(SyncItem &entry);

  void CreateNestedCatalog(SyncItem &requestFile);
  void RemoveNestedCatalog(SyncItem &requestFile);

	// Called by file system traversal
  void EnterAddedDirectoryCallback(const std::string &parent_dir,
                                   const std::string &dir_name);
  void LeaveAddedDirectoryCallback(const std::string &parent_dir,
                                   const std::string &dir_name);

  void AddDirectoryRecursively(SyncItem &entry);
  bool AddDirectoryCallback(const std::string &parent_dir,
                            const std::string &dir_name);
  void AddFileCallback(const std::string &parent_dir,
                       const std::string &file_name);
  void AddSymlinkCallback(const std::string &parent_dir,
                          const std::string &link_name);

  void RemoveDirectoryRecursively(SyncItem &entry);
  void RemoveFileCallback(const std::string &parent_dir,
                          const std::string &file_name);
  void RemoveSymlinkCallback(const std::string &parent_dir,
                             const std::string &link_name);
  void RemoveDirectoryCallback(const std::string &parent_dir,
                               const std::string &dir_name);

  // Hardlink handling
  void CompleteHardlinks(SyncItem &entry);
  HardlinkGroupMap& GetHardlinkMap() { return hardlink_stack_.top(); }
	void InsertExistingHardlinkFileCallback(const std::string &parent_dir,
                                          const std::string &file_name);
  void InsertExistingHardlinkSymlinkCallback(const std::string &parent_dir,
                                             const std::string &file_name);
  void InsertExistingHardlink(SyncItem &entry);
  uint64_t GetTemporaryHardlinkGroupNumber(SyncItem &entry) const;
  void InsertHardlink(SyncItem &entry);

  void AddHardlinkGroups(const HardlinkGroupMap &hardlinks);
  void AddHardlinkGroup(const HardlinkGroup &group);

  catalog::WritableCatalogManager *catalog_manager_;
  SyncUnion *union_engine_;

  /**
   * Hardlinks are supported as long as they all reside in the same directory.
   * If a recursion enters a directory, we push an empty HardlinkGroupMap to
   * keep track of the hardlinks of this directory.
   * When leaving a directory (i.e. it is completely processed) the stack is
   * popped and the HardlinkGroupMap is processed.
   */
  HardlinkGroupMapStack hardlink_stack_;

	/**
	 * New and modified files are sent to an external spooler for hashing and
   * compression.  A spooler callback adds them to the catalogs, once processed.
	 */
  pthread_mutex_t lock_file_queue_;
	SyncItemList file_queue_;
	HardlinkGroupList mHardlinkQueue;

	const SyncParameters *params_;
};  // class SyncMediator

}  // namespace publish

#endif  // CVMFS_SYNC_MEDIATOR_H_
