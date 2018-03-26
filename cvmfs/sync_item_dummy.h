/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_SYNC_ITEM_DUMMY_H_
#define CVMFS_SYNC_ITEM_DUMMY_H_

#include "sync_item.h"

#include <string>

#include "sync_union_tarball.h"

namespace publish {
/*
 * This class represents dummy directories that we know are going to be there
 * but we still haven't found yet. This is possible in the extraction of
 * tarball, where the files are not extracted in order (root to leaves) but in a
 * random fashion.
 */
class SyncItemDummyDir : public SyncItem {
  friend SyncUnionTarball;

 protected:
  SyncItemDummyDir(const std::string &relative_parent_path,
                   const std::string &filename, const SyncUnion *union_engine,
                   const SyncItemType entry_type)
      : SyncItem(relative_parent_path, filename, union_engine, entry_type) {
    assert(kItemDir == entry_type);

    union_stat_.obtained = true;
    union_stat_.stat.st_mode = S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP |
                               S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH;
    union_stat_.stat.st_nlink = 1;
    union_stat_.stat.st_uid = getuid();
    union_stat_.stat.st_gid = getgid();

    scratch_stat_.obtained = true;
    scratch_stat_.stat.st_mode = S_IFDIR | S_IRUSR | S_IWUSR | S_IXUSR |
                                 S_IRGRP | S_IWGRP | S_IXGRP | S_IROTH |
                                 S_IWOTH;
    scratch_stat_.stat.st_nlink = 1;
    scratch_stat_.stat.st_uid = getuid();
    scratch_stat_.stat.st_gid = getgid();
    scratch_type_ = kItemDir;
  }
};

}  // namespace publish

#endif  // CVMFS_SYNC_ITEM_DUMMY_H_
