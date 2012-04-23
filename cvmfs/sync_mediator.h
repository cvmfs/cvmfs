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

namespace publish {

/**
 *  If we encounter a file with linkcount > 1 it will be added to a HardlinkGroup
 *  After processing all files, the HardlinkGroups are populated with related hardlinks
 *  Assertion: linkcount == HardlinkGroup::hardlinks.size() at the end!!
 */

struct HardlinkGroup {
  HardlinkGroup(SyncItem &master_entry) :
    masterFile(master_entry) {
    //hardlinks.push_back(master_entry);
      hardlinks[master_entry.GetRelativePath()] = master_entry;
  }

  void AddHardlink(SyncItem &entry) {
    //hardlinks.push_back(entry);
    hardlinks[entry.GetRelativePath()] = entry;
  }

	SyncItem masterFile;
	SyncItemList hardlinks;
};

/**
 *  a mapping of inode number to the related HardlinkGroup
 */
typedef std::map<uint64_t, HardlinkGroup> HardlinkGroupMap;

void *MainReceive(void *data);

/**
 *  The SyncMediator refines the input received from a concrete UnionSync object
 *  for example it resolves the insertion and deletion of complete directories by recursing them
 *  It works as a mediator between the union file system and forwards the correct database
 *  commands to the CatalogHandler to sync the changes into the repository
 *  Furthermore it handles the data compression and storage
 */
class SyncMediator {
  friend void *MainReceive(void *data);
private:
	typedef std::stack<HardlinkGroupMap> HardlinkGroupMapStack;
	typedef std::list<HardlinkGroup> HardlinkGroupList;

private:
  catalog::WritableCatalogManager *mCatalogManager;
  SyncUnion *mUnionEngine;

	/**
	 *  hardlinks are supported as long as they all reside in the SAME directory.
	 *  If a recursion enters a directory, we push an empty HardlinkGroupMap to accommodate the hardlinks of this directory.
	 *  When leaving a directory (i.e. it is completely processed) the stack is popped and the HardlinkGroupMap receives
	 *  further processing.
	 */
	HardlinkGroupMapStack mHardlinkStack;

	/**
	 *  Files and hardlinks are not simply added to the catalogs but kept in a queue
	 *  when committing the changes after recursing the read write branch of the union file system
	 *  the queues will processed en bloc (for parallelization purposes) and afterwards added
	 */
  pthread_mutex_t lock_file_queue_;
  uint64_t num_files_process;
	SyncItemList mFileQueue;
	HardlinkGroupList mHardlinkQueue;
  pthread_t thread_receive_;

	const SyncParameters *params_;
  int pipe_fanout_;
  int pipe_hashes_;

public:
	SyncMediator(catalog::WritableCatalogManager *catalogManager,
	             const SyncParameters *params);

	/**
	 *  adding an entry to the repository
	 *  this method does further processing: f.e. added directories will be recursed
	 *  to add all its contents
	 *  @param entry the entry to add
	 */
	void Add(SyncItem &entry);

	/**
	 *  touching an entry in the repository
	 *  @param the entry to touch
	 */
	void Touch(SyncItem &entry);

	/**
	 *  removing an entry from the repository
	 *  directories will be recursively removed to get rid of its contents
	 *  @param entry the entry to remove
	 */
	void Remove(SyncItem &entry);

	/**
	 *  meta operation
	 *  basically remove the old entry and add the new one
	 *  @param entry the entry to be replaced by a new one
	 */
	void Replace(SyncItem &entry);

	/**
	 *  notifier that a new directory was opened for recursion
	 *  @param entry the opened directory
	 */
	void EnterDirectory(SyncItem &entry);

	/**
	 *  notifier that a directory is fully processed by a recursion
	 *  @param entry the directory which will be left by the recursion
	 */
	void LeaveDirectory(SyncItem &entry, const bool complete_hardlinks = true);

	/**
	 *  do any pending processing and commit all changes to the catalogs
	 *  to be called AFTER all recursions are finished
	 */
	void Commit();

private:
  // -------------------------------------------------------------
	void AddDirectoryRecursively(SyncItem &entry);

	bool AddDirectoryCallback(const std::string &parent_dir,
                            const std::string &dir_name);
	void AddFileCallback(const std::string &parent_dir,
                       const std::string &file_name);
  void AddSymlinkCallback(const std::string &parent_dir,
                          const std::string &link_name);
	void EnterAddedDirectoryCallback(const std::string &parent_dir,
                                   const std::string &dir_name);
	void LeaveAddedDirectoryCallback(const std::string &parent_dir,
                                   const std::string &dir_name);
  // -------------------------------------------------------------
  void RemoveDirectoryRecursively(SyncItem &entry);

  void RemoveFileCallback(const std::string &parent_dir,
                          const std::string &file_name);
  void RemoveSymlinkCallback(const std::string &parent_dir,
                             const std::string &link_name);
  void RemoveDirectoryCallback(const std::string &parent_dir,
                               const std::string &dir_name);
	// -------------------------------------------------------------
	void CompleteHardlinks(SyncItem &entry);

	void InsertExistingHardlinkFileCallback(const std::string &parent_dir,
	                                        const std::string &file_name);
	void InsertExistingHardlinkSymlinkCallback(const std::string &parent_dir,
	                                           const std::string &file_name);
  void InsertExistingHardlink(SyncItem &entry);
  // -------------------------------------------------------------

	uint64_t GetTemporaryHardlinkGroupNumber(SyncItem &entry) const;
	void InsertHardlink(SyncItem &entry);

	void AddFile(SyncItem &entry);
	void RemoveFile(SyncItem &entry);
	void TouchFile(SyncItem &entry);

	void AddDirectory(SyncItem &entry);
	void RemoveDirectory(SyncItem &entry);
	void TouchDirectory(SyncItem &entry);

	void CreateNestedCatalog(SyncItem &requestFile);
	void RemoveNestedCatalog(SyncItem &requestFile);

	void CompressAndHashFileQueue();
	void AddFileQueueToCatalogs();

	inline bool AddFileToDatastore(SyncItem &entry, hash::Any &hash) { return AddFileToDatastore(entry, "", hash); }
	bool AddFileToDatastore(SyncItem &entry, const std::string &suffix, hash::Any &hash);

	inline HardlinkGroupMap& GetHardlinkMap() { return mHardlinkStack.top(); }

	void AddHardlinkGroups(const HardlinkGroupMap &hardlinks);
  void AddHardlinkGroup(const HardlinkGroup &group);

  friend class SyncUnion;
  inline void RegisterSyncUnionEngine(SyncUnion *syncUnionEngine) {
    mUnionEngine = syncUnionEngine;
  }
};  // class SyncMediator

}  // namespace sync

#endif  // CVMFS_SYNC_MEDIATOR_H_
