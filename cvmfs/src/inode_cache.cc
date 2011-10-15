#include "inode_cache.h"

extern "C" {
   #include "debug.h"
}

using namespace std;

namespace cvmfs {

   InodeCache::InodeCache(unsigned int cacheSize) :
      LruCache<fuse_ino_t, DirectoryEntry>(cacheSize) {
      
      this->setSpecialHashTableKeys(1000000000, 1000000001);
   }

   bool InodeCache::insert(const fuse_ino_t inode, const DirectoryEntry &dirEntry) {
      pmesg(D_INO_CACHE, "insert inode: %d -> '%s'", inode, dirEntry.name().c_str());
      return LruCache<fuse_ino_t, DirectoryEntry>::insert(inode, dirEntry);
   }

   bool InodeCache::lookup(const fuse_ino_t inode, DirectoryEntry *dirEntry) {
      pmesg(D_INO_CACHE, "lookup inode: %d", inode);
      return LruCache<fuse_ino_t, DirectoryEntry>::lookup(inode, dirEntry);
   }
   
   void InodeCache::drop() {
      pmesg(D_INO_CACHE, "dropping cache");
      LruCache<fuse_ino_t, DirectoryEntry>::drop();
   }

} // namespace cvmfs
