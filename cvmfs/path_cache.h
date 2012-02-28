#ifndef PATH_CACHE_H
#define PATH_CACHE_H 1

#include "lru_cache.h"
#include "inode_cache.h"

#include "fuse-duplex.h"

namespace cvmfs {

   class PathCache :
      public LruCache<fuse_ino_t, std::string> {
      public:
         PathCache(unsigned int cacheSize);

         bool insert(const fuse_ino_t inode, const std::string &path);
         bool lookup(const fuse_ino_t inode, std::string *path);
         void drop();
   };

} // namespace cvmfs

#endif /* PATH_CACHE_H */
