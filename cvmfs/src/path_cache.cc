#include "path_cache.h"

#include "logging.h"

using namespace std;

namespace cvmfs {

   PathCache::PathCache(unsigned int cacheSize) :
      LruCache<fuse_ino_t, string>(cacheSize) {

      this->setSpecialHashTableKeys(1000000000, 1000000001);
   }

   bool PathCache::insert(const fuse_ino_t inode, const string &path) {
      LogCvmfs(kLogPathCache, kLogDebug, "insert into cache %d -> '%s'", inode, path.c_str());
      return LruCache<fuse_ino_t, string>::insert(inode, path);
   }

   bool PathCache::lookup(const fuse_ino_t inode, string *path) {
      LogCvmfs(kLogPathCache, kLogDebug, "lookup inode: %d", inode);
      return LruCache<fuse_ino_t, string>::lookup(inode, path);
   }

   void PathCache::drop() {
      LogCvmfs(kLogPathCache, kLogDebug, "dropping cache");
      LruCache<fuse_ino_t, string>::drop();
   }

} // namespace cvmfs
