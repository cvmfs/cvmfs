#ifndef MD5PATH_CACHE_H
#define MD5PATH_CACHE_H 1

#include <string>

#include "lru.h"
#include "hash.h"
#include "atomic.h"
#include "dirent.h"

namespace lru {

  struct hash_md5 {
    size_t operator() (const hash::Md5 &md5) const {
      return (size_t)*((size_t*)md5.digest);
    }
  };

  struct hash_equal {
    bool operator() (const hash::Md5 &a, const hash::Md5 &b) const {
      return a == b;
    }
  };

  /**
   *  this is currently just a quick and dirty prototype!!
   */
  class Md5PathCache :
  public LruCache<hash::Md5, catalog::DirectoryEntry, hash_md5, hash_equal >
  {
  private:

  public:
    Md5PathCache(unsigned int cacheSize);

    bool insert(const hash::Md5 &hash, const struct catalog::DirectoryEntry &dirEntry);
    bool lookup(const hash::Md5 &hash, struct catalog::DirectoryEntry *dirEntry);
    bool forget(const hash::Md5 &hash);
  };

} // namespace cvmfs

#endif /* MD5PATH_CACHE_H */
