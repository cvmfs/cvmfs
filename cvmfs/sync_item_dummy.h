/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_DUMMY_H_
#define CVMFS_SYNC_ITEM_DUMMY_H_

#include "sync_item.h"

#include "sync_union_tarball.h"

namespace publish {

class SyncItemDummy : public SyncItem {
  friend SyncUnionTarball;

 protected:
  SyncItemDummy(const std::string &relative_parent_path,
                const std::string &filename, const SyncUnion *union_engine,
                const SyncItemType entry_type)
      : SyncItem(relative_parent_path, filename, union_engine, entry_type) {
    union_stat_.obtained = true;
    union_stat_.stat.st_mode = S_IFDIR;
    union_stat_.stat.st_nlink = 1;
    scratch_type_ = kItemDir;
  }
};

}  // namespace publish

#endif  // CVMFS_SYNC_ITEM_DUMMY_H_
