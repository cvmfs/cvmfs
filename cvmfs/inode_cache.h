#ifndef INODE_CACHE_H
#define INODE_CACHE_H 1

#include <string>

#include <fuse/fuse_lowlevel.h>
#include "lru.h"
#include "dirent.h"

namespace lru {

class InodeCache : public LruCache<fuse_ino_t, catalog::DirectoryEntry> {
 public:
  InodeCache(unsigned int cacheSize);

  bool insert(const fuse_ino_t inode, const catalog::DirectoryEntry &dirEntry);
  bool lookup(const fuse_ino_t inode, catalog::DirectoryEntry *dirEntry);
  void drop();
};

} // namespace cvmfs

#endif /* INODE_CACHE_H */
