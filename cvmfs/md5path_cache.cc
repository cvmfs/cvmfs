#include "md5path_cache.h"

#include "logging.h"

using namespace std;

// TODO: enable this - slowed things down last time
#define DISALBE_MD5_PATH_CACHE

namespace lru {

   Md5PathCache::Md5PathCache(unsigned int cacheSize) :
      LruCache<hash::Md5, catalog::DirectoryEntry, hash_md5, hash_equal >(cacheSize) {

        this->setSpecialHashTableKeys(hash::Md5(hash::AsciiPtr("!")), hash::Md5(hash::AsciiPtr("?")));
   }

   bool Md5PathCache::insert(const hash::Md5 &hash, const catalog::DirectoryEntry &dirEntry) {
#ifdef DISALBE_MD5_PATH_CACHE
      return true;
#endif
      LogCvmfs(kLogMd5Cache, kLogDebug, "insert md5: %s -> '%s'", hash.ToString().c_str(), dirEntry.name().c_str());
      return LruCache<hash::Md5, catalog::DirectoryEntry, hash_md5, hash_equal >::insert(hash, dirEntry);
   }

   bool Md5PathCache::lookup(const hash::Md5 &hash, catalog::DirectoryEntry *dirEntry) {
#ifdef DISALBE_MD5_PATH_CACHE
      return false;
#endif
      LogCvmfs(kLogMd5Cache, kLogDebug, "lookup md5: %s", hash.ToString().c_str());
      return LruCache<hash::Md5, catalog::DirectoryEntry, hash_md5, hash_equal >::lookup(hash, dirEntry);
   }

   bool Md5PathCache::forget(const hash::Md5 &hash) {
#ifdef DISALBE_MD5_PATH_CACHE
      return true;
#endif
      LogCvmfs(kLogMd5Cache, kLogDebug, "forget md5: %s", hash.ToString().c_str());
      return LruCache<hash::Md5, catalog::DirectoryEntry, hash_md5, hash_equal >::forget(hash);
   }

} // namespace cvmfs
