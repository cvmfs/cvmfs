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
      pmesg(D_INO_CACHE, "insert inode: %d -> %s", inode, dirEntry.name.c_str());
      return LruCache<fuse_ino_t, struct catalog::t_dirent>::insert(inode, dirEntry);
   }

   bool InodeCache::lookup(const fuse_ino_t inode, struct catalog::t_dirent &dirEntry) {
      pmesg(D_INO_CACHE, "lookup inode: %d", inode);
      
      return LruCache<fuse_ino_t, struct catalog::t_dirent>::lookup(inode, dirEntry);
      // if (LruCache<fuse_ino_t, catalog::t_dirent>::lookup(inode, dirEntry)) {
      //          // cache hit
      //          pmesg(D_INO_CACHE, "cache HIT | %d -> %s", inode, dirEntry.name.c_str());
      //          return true;
      //       }
      //       
      //       if (catalog::lookup_inode(inode, dirEntry)) {
      //          // catalog hit
      //          pmesg(D_INO_CACHE, "catalog HIT | %d -> %s", inode, dirEntry.name.c_str());
      //          this->insert(inode, dirEntry);
      //          return true;
      //       }
      //       
      //       pmesg(D_INO_CACHE, "no such file or directory");
      //       
      //       // not found
      //       return false;
   }

} // namespace cvmfs
