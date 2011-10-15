#ifndef MD5PATH_CACHE_H
#define MD5PATH_CACHE_H 1

#include <string>

#include "lru_cache.h"
#include "catalog.h"
#include "hash.h"

#include "atomic.h"

extern "C" {
   #include "debug.h"
}

namespace cvmfs {

   struct hash_md5 {
      size_t operator() (const hash::t_md5 &md5) const {
         return (size_t)*((size_t*)md5.digest);
      }
   };
   
   struct hash_equal {
      bool operator() (const hash::t_md5 &a, const hash::t_md5 &b) const {
         return a == b;
      }
   };

   /**
    *  this is currently just a quick and dirty prototype!!
    */
   class Md5PathCache :
      public LruCache<hash::t_md5, struct catalog::t_dirent, hash_md5, hash_equal >
   {
   private:
      
      public:
         Md5PathCache(unsigned int cacheSize);
         
         bool insert(const hash::t_md5 &hash, const struct catalog::t_dirent &dirEntry);
         bool lookup(const hash::t_md5 &hash, struct catalog::t_dirent *dirEntry);
         bool forget(const hash::t_md5 &hash);
   };

} // namespace cvmfs

#endif /* MD5PATH_CACHE_H */
