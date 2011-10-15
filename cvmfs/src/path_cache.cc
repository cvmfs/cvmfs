#include "path_cache.h"

extern "C" {
   #include "debug.h"
}

using namespace std;

namespace cvmfs {

   PathCache::PathCache(unsigned int cacheSize) :
      LruCache<fuse_ino_t, string>(cacheSize) {
      
      this->setSpecialHashTableKeys(1000000000, 1000000001);
   }
   
   bool PathCache::insert(const fuse_ino_t inode, const string &path) {
      pmesg(D_PATH_CACHE, "insert into cache %d -> '%s'", inode, path.c_str());
      return LruCache<fuse_ino_t, string>::insert(inode, path);
   }

   bool PathCache::lookup(const fuse_ino_t inode, string *path) {
      pmesg(D_PATH_CACHE, "lookup inode: %d", inode);
      return LruCache<fuse_ino_t, string>::lookup(inode, path);
   }
   
   void PathCache::drop() {
      pmesg(D_PATH_CACHE, "dropping cache");
      LruCache<fuse_ino_t, string>::drop();
   }

} // namespace cvmfs
