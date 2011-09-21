#include "inode_cache.h"

#include "hash.h"

extern "C" {
   #include "debug.h"
}

using namespace std;

namespace cvmfs {

   InodeCache::InodeCache(unsigned int cacheSize) :
      LruCache<fuse_ino_t, struct catalog::t_dirent>(cacheSize) {}

   bool InodeCache::insert(const fuse_ino_t inode, const struct catalog::t_dirent &dirEntry) {
      pmesg(D_INO_CACHE, "insert inode: %d -> '%s'", inode, dirEntry.name.c_str());
      return LruCache<fuse_ino_t, struct catalog::t_dirent>::insert(inode, dirEntry);
   }

   bool InodeCache::lookup(const fuse_ino_t inode, struct catalog::t_dirent &dirEntry) {
      pmesg(D_INO_CACHE, "lookup inode: %d", inode);
      return LruCache<fuse_ino_t, struct catalog::t_dirent>::lookup(inode, dirEntry);
   }
   
   void InodeCache::drop() {
      pmesg(D_INO_CACHE, "dropping cache");
      LruCache<fuse_ino_t, struct catalog::t_dirent>::drop();
   }

} // namespace cvmfs
