#ifndef INODE_CACHE_H
#define INODE_CACHE_H 1

#include <string>

#include "lru_cache.h"
#include "catalog.h"
#include "fuse-duplex.h"

namespace cvmfs {

   class InodeCache :
      public LruCache<fuse_ino_t, struct catalog::t_dirent> {
      
      public:
         InodeCache(unsigned int cacheSize);
         
         bool insert(const fuse_ino_t inode, const struct catalog::t_dirent &dirEntry);
         bool lookup(const fuse_ino_t inode, struct  catalog::t_dirent &dirEntry);
   };

} // namespace cvmfs

#endif /* INODE_CACHE_H */
