#ifndef INODE_CACHE_H
#define INODE_CACHE_H 1

#include <string>

#include "lru_cache.h"
#include "catalog.h"
#include "fuse-duplex.h"
#include "directory_entry.h"

namespace cvmfs {

   class InodeCache :
      public LruCache<fuse_ino_t, DirectoryEntry>
   {
      
      public:
         InodeCache(unsigned int cacheSize);
         
         bool insert(const fuse_ino_t inode, const DirectoryEntry &dirEntry);
         bool lookup(const fuse_ino_t inode, DirectoryEntry *dirEntry);
         void drop();
   };

} // namespace cvmfs

#endif /* INODE_CACHE_H */
