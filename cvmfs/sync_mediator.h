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

#include <map>
#include <set>
#include <stack>
#include <string>
#include <vector>

#include "catalog_mgr_rw.h"
#include "compression.h"
#include "platform.h"
#include "swissknife_sync.h"
#include "sync_item.h"
#include "xattr.h"

namespace manifest {
class Manifest;
}

namespace publish {

/**
 * If we encounter a file with linkcount > 1 it will be added to a HardlinkGroup
 * After processing all files, the HardlinkGroups are populated with
 * related hardlinks
 * Assertion: linkcount == HardlinkGroup::hardlinks.size() at the end
 */
struct HardlinkGroup {
  explicit HardlinkGroup(const SyncItem &item) : master(item) {
    hardlinks[master.GetRelativePath()] = master;
  }

  void AddHardlink(const SyncItem &entry) {
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
 * The SyncMediator refines the input received from a concrete UnionSync object.
 * For example, it resolves the insertion and deletion of complete directories
 * by recursing them.  It works as a mediator between the union file system and
 * forwards the correct database commands to the catalog handler to sync the
 * changes into the repository.
 * Furthermore it sends new and modified files to the spooler for compression
 * and hashing.
 */
class SyncMediator {
  friend class SyncUnion;
 private:
  enum ChangesetAction {
    kAdd,
    kAddCatalog,
    kAddHardlinks,
    kTouch,
    kRemove,
    kRemoveCatalog
  };

 public:
  static const unsigned int processing_dot_interval = 100;

  SyncMediator(catalog::WritableCatalogManager *catalog_manager,
               const SyncParameters *params);
  virtual ~SyncMediator();

  void Add(const SyncItem &entry);
  void Touch(const SyncItem &entry);
  void Remove(const SyncItem &entry);
  void Replace(const SyncItem &entry);

  void EnterDirectory(const SyncItem &entry);
  void LeaveDirectory(const SyncItem &entry);

  bool Commit(manifest::Manifest *manifest);

  // The sync union engine uses this information to create properly initialized
  // sync items
  bool IsExternalData() const { return params_->external_data; }
  zlib::Algorithms GetCompressionAlgorithm() const {
    return params_->compression_alg;
  }

 private:
  typedef std::stack<HardlinkGroupMap> HardlinkGroupMapStack;
  typedef std::vector<HardlinkGroup> HardlinkGroupList;

  void RegisterUnionEngine(SyncUnion *engine);

  void PrintChangesetNotice(const ChangesetAction action,
                            const std::string &extra_info) const;

  // Called after figuring out the type of a path (file, symlink, dir)
  void AddFile(const SyncItem &entry);
  void RemoveFile(const SyncItem &entry);

  void AddDirectory(const SyncItem &entry);
  void RemoveDirectory(const SyncItem &entry);
  void TouchDirectory(const SyncItem &entry);

  void CreateNestedCatalog(const SyncItem &requestFile);
  void RemoveNestedCatalog(const SyncItem &requestFile);

  // Called by file system traversal
  void EnterAddedDirectoryCallback(const std::string &parent_dir,
                                   const std::string &dir_name);
  void LeaveAddedDirectoryCallback(const std::string &parent_dir,
                                   const std::string &dir_name);

  void AddDirectoryRecursively(const SyncItem &entry);
  bool AddDirectoryCallback(const std::string &parent_dir,
                            const std::string &dir_name);
  void AddFileCallback(const std::string &parent_dir,
                       const std::string &file_name);
  void AddSymlinkCallback(const std::string &parent_dir,
                          const std::string &link_name);

  void RemoveDirectoryRecursively(const SyncItem &entry);
  void RemoveFileCallback(const std::string &parent_dir,
                          const std::string &file_name);
  void RemoveSymlinkCallback(const std::string &parent_dir,
                             const std::string &link_name);
  void RemoveDirectoryCallback(const std::string &parent_dir,
                               const std::string &dir_name);

  bool IgnoreFileCallback(const std::string &parent_dir,
                          const std::string &file_name);

  SyncItem CreateSyncItem(const std::string  &relative_parent_path,
                          const std::string  &filename,
                          const SyncItemType  entry_type) const;

  // Called by Upload Spooler
  void PublishFilesCallback(const upload::SpoolerResult &result);
  void PublishHardlinksCallback(const upload::SpoolerResult &result);

  // Hardlink handling
  void CompleteHardlinks(const SyncItem &entry);
  HardlinkGroupMap &GetHardlinkMap() { return hardlink_stack_.top(); }
  void LegacyRegularHardlinkCallback(const std::string &parent_dir,
                                     const std::string &file_name);
  void LegacySymlinkHardlinkCallback(const std::string &parent_dir,
                                      const std::string &file_name);
  void InsertLegacyHardlink(const SyncItem &entry);
  uint64_t GetTemporaryHardlinkGroupNumber(const SyncItem &entry) const;
  void InsertHardlink(const SyncItem &entry);

  void AddLocalHardlinkGroups(const HardlinkGroupMap &hardlinks);
  void AddHardlinkGroup(const HardlinkGroup &group);

  catalog::WritableCatalogManager *catalog_manager_;
  SyncUnion *union_engine_;

  bool handle_hardlinks_;

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

  HardlinkGroupList hardlink_queue_;

  const SyncParameters *params_;
  mutable unsigned int changed_items_;

  /**
   * By default, files have no extended attributes.
   */
  XattrList default_xattrs;
};  // class SyncMediator

}  // namespace publish

#endif  // CVMFS_SYNC_MEDIATOR_H_
