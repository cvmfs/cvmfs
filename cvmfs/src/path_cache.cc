#include "path_cache.h"

extern "C" {
   #include "debug.h"
}

using namespace std;

namespace cvmfs {

   PathCache::PathCache(unsigned int cacheSize, InodeCache *inodeCache) :
                              LruCache<fuse_ino_t, string>(cacheSize)
   {
      mInodeCache = inodeCache;
   }
   
   bool PathCache::insert(const fuse_ino_t inode, const string &path) {
      pmesg(D_PATH_CACHE, "insert into cache %d -> %s", inode, path.c_str());
      return LruCache<fuse_ino_t, string>::insert(inode, path);
   }

   bool PathCache::lookup(const fuse_ino_t inode, string &path) {
      pmesg(D_PATH_CACHE, "lookup inode: %d", inode);
      
      // // check for root inode (is always known)
      //       if (inode == catalog::ROOT_INODE) {
      //             // found root
      //             pmesg(D_PATH_CACHE, "found root");
      //             path = "";
      //             return true;
      //       }
      //       
      //       // lookup in the cache
      //       if (LruCache<fuse_ino_t, string>::lookup(inode, path)) {
      //          // cache hit
      //          pmesg(D_PATH_CACHE, "cache HIT | %d -> %s", inode, path.c_str());
      //          return true;
      //       }
      //       
      //       pmesg(D_PATH_CACHE, "cache MISS -> digging deeper");
      //       
      //       // looking recursively for parent path
      //       catalog::t_dirent dirent;
      //       if(mInodeCache->lookup(inode, dirent)) {
      //          assert (dirent.parentInode != catalog::INVALID_INODE);
      //          
      //          string pathPart;
      //          if (this->lookup(dirent.parentInode, pathPart)) {
      //             path = pathPart + "/" + dirent.name;
      //             this->insert(inode, path);
      //             return true;
      //          }
      //       }
      //       
      //       pmesg(D_PATH_CACHE, "no such file or directory");
      
      // not found
      return LruCache<fuse_ino_t, string>::lookup(inode, path);
   }

} // namespace cvmfs
