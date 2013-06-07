/**
 * This file is part of the CernVM File System.
 */

#include "compat.h"

using namespace std;  // NOLINT

namespace compat {
namespace inode_tracker{

bool InodeContainer::ConstructPath(const uint64_t inode, PathString *path) {
  InodeMap::const_iterator needle = map_.find(inode);
  if (needle == map_.end())
    return false;

  if (needle->second.name.IsEmpty())
    return true;

  bool retval = ConstructPath(needle->second.parent_inode, path);
  path->Append("/", 1);
  path->Append(needle->second.name.GetChars(),
               needle->second.name.GetLength());
  assert(retval);
  return retval;
}


InodeTracker::~InodeTracker() {
  pthread_mutex_destroy(lock_);
  free(lock_);
}

void Migrate(InodeTracker *old_tracker, glue::InodeTracker *new_tracker) {
  InodeContainer::InodeMap::const_iterator i, iEnd;
  i = old_tracker->inode2path_.map_.begin();
  iEnd = old_tracker->inode2path_.map_.end();
  for (; i != iEnd; ++i) {
    uint64_t inode = i->first;
    uint32_t references = i->second.references;
    PathString path;
    old_tracker->inode2path_.ConstructPath(inode, &path);
    new_tracker->VfsGetBy(inode, references, path);
  }
}

}  // namespace inode_tracker

}  // namespace compat
