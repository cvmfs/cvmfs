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
#include "file_chunk.h"
#include "platform.h"
#include "publish/repository.h"
#include "statistics.h"
#include "swissknife_sync.h"
#include "sync_item.h"
#include "util/pointer.h"
#include "util/shared_ptr.h"
#include "xattr.h"

namespace manifest {
class Manifest;
}

struct Counters;

namespace publish {

class SyncDiffReporter : public DiffListener {
 public:
  enum PrintAction {
    kPrintDots,
    kPrintChanges
  };

  explicit SyncDiffReporter(PrintAction print_action = kPrintChanges,
                            unsigned int processing_dot_interval = 100)
      : print_action_(print_action),
        processing_dot_interval_(processing_dot_interval),
        changed_items_(0) {}

  virtual void OnInit(const history::History::Tag &from_tag,
                      const history::History::Tag &to_tag);
  virtual void OnStats(const catalog::DeltaCounters &delta);

  virtual void OnAdd(const std::string &path,
                     const catalog::DirectoryEntry &entry);
  virtual void OnRemove(const std::string &path,
                        const catalog::DirectoryEntry &entry);
  virtual void OnModify(const std::string &path,
                        const catalog::DirectoryEntry &entry_from,
                        const catalog::DirectoryEntry &entry_to);
  void CommitReport();

 private:
  void PrintDots();
  void AddImpl(const std::string &path);
  void RemoveImpl(const std::string &path);
  void ModifyImpl(const std::string &path);

  PrintAction print_action_;
  unsigned int processing_dot_interval_;
  unsigned int changed_items_;
};

/**
 * If we encounter a file with linkcount > 1 it will be added to a HardlinkGroup
 * After processing all files, the HardlinkGroups are populated with
 * related hardlinks
 * Assertion: linkcount == HardlinkGroup::hardlinks.size() at the end
 */
struct HardlinkGroup {
  explicit HardlinkGroup(SharedPtr<SyncItem> item) : master(item) {
    hardlinks[master->GetRelativePath()] = item;
  }

  void AddHardlink(SharedPtr<SyncItem> entry) {
    hardlinks[entry->GetRelativePath()] = entry;
  }

  SharedPtr<SyncItem> master;
  SyncItemList hardlinks;
  FileChunkList file_chunks;
};

class AbstractSyncMediator {
 public:
  virtual ~AbstractSyncMediator() = 0;

  virtual void RegisterUnionEngine(SyncUnion *engine) = 0;

  virtual void Add(SharedPtr<SyncItem> entry) = 0;
  virtual void Touch(SharedPtr<SyncItem> entry) = 0;
  virtual void Remove(SharedPtr<SyncItem> entry) = 0;
  virtual void Replace(SharedPtr<SyncItem> entry) = 0;
  virtual void Clone(const std::string from, const std::string to) = 0;

  virtual void AddUnmaterializedDirectory(SharedPtr<SyncItem> entry) = 0;

  virtual void EnterDirectory(SharedPtr<SyncItem> entry) = 0;
  virtual void LeaveDirectory(SharedPtr<SyncItem> entry) = 0;

  virtual bool Commit(manifest::Manifest *manifest) = 0;

  virtual bool IsExternalData() const = 0;
  virtual bool IsDirectIo() const = 0;
  virtual zlib::Algorithms GetCompressionAlgorithm() const = 0;
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
class SyncMediator : public virtual AbstractSyncMediator {
 public:
  static const unsigned int processing_dot_interval = 100;

  SyncMediator(catalog::WritableCatalogManager *catalog_manager,
               const SyncParameters *params,
               perf::StatisticsTemplate statistics);
  void RegisterUnionEngine(SyncUnion *engine);
  // Final class, it is not meant to be derived any further
  ~SyncMediator();

  void Add(SharedPtr<SyncItem> entry);
  void Touch(SharedPtr<SyncItem> entry);
  void Remove(SharedPtr<SyncItem> entry);
  void Replace(SharedPtr<SyncItem> entry);
  void Clone(const std::string from, const std::string to);

  void AddUnmaterializedDirectory(SharedPtr<SyncItem> entry);

  void EnterDirectory(SharedPtr<SyncItem> entry);
  void LeaveDirectory(SharedPtr<SyncItem> entry);

  bool Commit(manifest::Manifest *manifest);

  // The sync union engine uses this information to create properly initialized
  // sync items
  bool IsExternalData() const { return params_->external_data; }
  bool IsDirectIo() const { return params_->direct_io; }
  zlib::Algorithms GetCompressionAlgorithm() const {
    return params_->compression_alg;
  }

 private:
  typedef std::stack<HardlinkGroupMap> HardlinkGroupMapStack;
  typedef std::vector<HardlinkGroup> HardlinkGroupList;

  void EnsureAllowed(SharedPtr<SyncItem> entry);

  // Called after figuring out the type of a path (file, symlink, dir)
  void AddFile(SharedPtr<SyncItem> entry);
  void RemoveFile(SharedPtr<SyncItem> entry);

  void AddDirectory(SharedPtr<SyncItem> entry);
  void RemoveDirectory(SharedPtr<SyncItem> entry);
  void TouchDirectory(SharedPtr<SyncItem> entry);

  void CreateNestedCatalog(SharedPtr<SyncItem> directory);
  void RemoveNestedCatalog(SharedPtr<SyncItem> directory);

  void TouchDirectoryRecursively(SharedPtr<SyncItem> entry);
  void TouchingFileCallback(const std::string &parent_dir,
                          const std::string &file_name);
  void TouchingSymlinkCallback(const std::string &parent_dir,
                             const std::string &link_name);
  void TouchDirectoryCallback(const std::string &parent_dir,
                               const std::string &dir_name);
  void RemoveDirectoryRecursively(SharedPtr<SyncItem> entry);
  void RemoveFileCallback(const std::string &parent_dir,
                          const std::string &file_name);
  void RemoveSymlinkCallback(const std::string &parent_dir,
                             const std::string &link_name);
  void RemoveCharacterDeviceCallback(const std::string &parent_dir,
                                     const std::string &link_name);
  void RemoveBlockDeviceCallback(const std::string &parent_dir,
                                 const std::string &link_name);
  void RemoveFifoCallback(const std::string &parent_dir,
                          const std::string &link_name);
  void RemoveSocketCallback(const std::string &parent_dir,
                            const std::string &link_name);
  void RemoveDirectoryCallback(const std::string &parent_dir,
                               const std::string &dir_name);
  bool IgnoreFileCallback(const std::string &parent_dir,
                          const std::string &file_name);
  // Called by file system traversal
  void EnterAddedDirectoryCallback(const std::string &parent_dir,
                                   const std::string &dir_name);
  void LeaveAddedDirectoryCallback(const std::string &parent_dir,
                                   const std::string &dir_name);
  void AddDirectoryRecursively(SharedPtr<SyncItem> entry);
  bool AddDirectoryCallback(const std::string &parent_dir,
                            const std::string &dir_name);
  void AddFileCallback(const std::string &parent_dir,
                       const std::string &file_name);
  void AddCharacterDeviceCallback(const std::string &parent_dir,
                                  const std::string &file_name);
  void AddBlockDeviceCallback(const std::string &parent_dir,
                              const std::string &file_name);
  void AddFifoCallback(const std::string &parent_dir,
                       const std::string &file_name);
  void AddSocketCallback(const std::string &parent_dir,
                         const std::string &file_name);
  void AddSymlinkCallback(const std::string &parent_dir,
                          const std::string &link_name);
  SharedPtr<SyncItem> CreateSyncItem(const std::string &relative_parent_path,
                                     const std::string &filename,
                                     const SyncItemType entry_type) const;

  // Called by Upload Spooler
  void PublishFilesCallback(const upload::SpoolerResult &result);
  void PublishHardlinksCallback(const upload::SpoolerResult &result);

  // Hardlink handling
  void CompleteHardlinks(SharedPtr<SyncItem> entry);
  HardlinkGroupMap &GetHardlinkMap() { return hardlink_stack_.top(); }
  void LegacyRegularHardlinkCallback(const std::string &parent_dir,
                                     const std::string &file_name);
  void LegacySymlinkHardlinkCallback(const std::string &parent_dir,
                                     const std::string &file_name);
  void LegacyCharacterDeviceHardlinkCallback(const std::string &parent_dir,
                                             const std::string &file_name);
  void LegacyBlockDeviceHardlinkCallback(const std::string &parent_dir,
                                         const std::string &file_name);
  void LegacyFifoHardlinkCallback(const std::string &parent_dir,
                                  const std::string &file_name);
  void LegacySocketHardlinkCallback(const std::string &parent_dir,
                                    const std::string &file_name);
  void InsertLegacyHardlink(SharedPtr<SyncItem> entry);
  uint64_t GetTemporaryHardlinkGroupNumber(SharedPtr<SyncItem> entry) const;
  void InsertHardlink(SharedPtr<SyncItem> entry);

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
  XattrList default_xattrs_;
  UniquePtr<perf::FsCounters> counters_;

  UniquePtr<SyncDiffReporter> reporter_;
};  // class SyncMediator

}  // namespace publish

#endif  // CVMFS_SYNC_MEDIATOR_H_
