#include "md5path_cache.h"

extern "C" {
   #include "debug.h"
}

using namespace std;

namespace cvmfs {

   Md5PathCache::Md5PathCache(unsigned int cacheSize) :
      LruCache<hash::t_md5, struct catalog::t_dirent, hash_md5, hash_equal >(cacheSize) {
      
      this->setSpecialHashTableKeys(hash::t_md5("!"), hash::t_md5("?"));
   }
   
   bool Md5PathCache::insert(const hash::t_md5 &hash, const struct catalog::t_dirent &dirEntry) {
      pmesg(D_MD5_CACHE, "insert md5: %s -> '%s'", hash.to_string().c_str(), dirEntry.name.c_str());
      return LruCache<hash::t_md5, struct catalog::t_dirent, hash_md5, hash_equal >::insert(hash, dirEntry);
   }
   
   bool Md5PathCache::lookup(const hash::t_md5 &hash, struct  catalog::t_dirent &dirEntry) {
      pmesg(D_MD5_CACHE, "lookup md5: %s", hash.to_string().c_str());
      return LruCache<hash::t_md5, struct catalog::t_dirent, hash_md5, hash_equal >::lookup(hash, dirEntry);
   }
   
   bool Md5PathCache::forget(const hash::t_md5 &hash) {
      pmesg(D_MD5_CACHE, "forget md5: %s", hash.to_string().c_str());
      return LruCache<hash::t_md5, struct catalog::t_dirent, hash_md5, hash_equal >::forget(hash);
   }

} // namespace cvmfs
