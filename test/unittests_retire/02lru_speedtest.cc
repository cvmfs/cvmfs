#include <iostream>
#include <string>
#include <sstream>

#include "lru_cache.h"
#include "hash.h"
#include "catalog.h"
#include "md5path_cache.h"

#include "../test_functions.h"

/**
 *  highly directory specific build string... but better than nothing:
 *
 *  g++ -I ../cvmfs/src -I ../../build/3rdPartyBuild/sqlite3/src/ -I ../../build -o exec -lssl -lrt -DNDEBUGMSG unittests/02lru_speedtest.cc ../../build/cvmfs/src/CMakeFiles/cvmfs2.dir/hash.cc.o ../../build/cvmfs/src/CMakeFiles/cvmfs2.dir/sha1.c.o ../../build/cvmfs/src/CMakeFiles/cvmfs2.dir/md5path_cache.cc.o
 */

using namespace std;
using namespace hash;

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

inline t_md5 getMd5Path(const int i) {
   stringstream key;
   key << "/" << (i % 17) << "/" << (i + 17) << "/" << (i*3);
   return t_md5(key.str());
}

typedef cvmfs::Md5PathCache Cache;

int main(int argc, char **argv) {
   const unsigned int kCacheInserts = 300000;
   const unsigned int kCacheLookups = 300000;
   const unsigned int kCacheSize = 100000;
   
   cout << "--> creating stop watch" << endl;
   StopWatch stopper;
   
	cout << "--> create cache (size of " << kCacheSize << ")" << endl;
	Cache cache(kCacheSize);
	
   cout << "--> inserting " << kCacheInserts << " elements into cache" << endl;
   stopper.start();
   for (int i = 0; i < kCacheInserts; ++i) {
      struct catalog::t_dirent dirent;
      dirent.inode = i;
      cache.insert(getMd5Path(i), dirent);
   }
   stopper.stop();
   
   cout << "<-- took: " << stopper.getTime() << " seconds" << endl;
   
   cout << "--> random lookup of " << kCacheLookups << " entries" << endl;
   srand(time(NULL));
   stopper.reset();
   stopper.start();
   int keyVal;
   struct catalog::t_dirent result;
   int hits = 0;
   int misses = 0;
   for (int i = 0; i < kCacheLookups; ++i) {
      keyVal = getRandomValueBetween(0, kCacheInserts - 1);
      if (cache.lookup(getMd5Path(keyVal), result)) {
         ++hits;
         if (result.inode != keyVal) return 1;
      } else {
         ++misses;
      }
   }
   stopper.stop();
   
   cout << "<-- hits: " << hits << " misses: " << misses << endl;
   cout << "<-- took: " << stopper.getTime() << " seconds" << endl;
   
   cout << "--> clearing cache" << endl;
   stopper.start();
   cache.drop();
   stopper.stop();
   
   hits = 0;
   misses = 0;
   
   cout << "<-- took: " << stopper.getTime() << " seconds" << endl;
   
   cout << "--> inserting " << kCacheInserts << " and SIMULTANIOUSLY lookup " << (kCacheLookups * 3) << " entries" << endl;
   
   stopper.reset();
   stopper.start();
   for (int i = 0; i < kCacheInserts; ++i) {
      // insert
      struct catalog::t_dirent dirent;
      dirent.inode = i;
      cache.insert(getMd5Path(i), dirent);
      
      for (int j = 0; j < 3; ++j) {
         keyVal = getRandomValueBetween(0, kCacheInserts - 1);
         if (cache.lookup(getMd5Path(keyVal), result)) {
            ++hits;
            if (result.inode != keyVal) return 2;
         } else {
            ++misses;
         }
      }
   }
   stopper.stop();
   
   cout << "<-- hits: " << hits << " misses: " << misses << endl;
   cout << "<-- took: " << stopper.getTime() << " seconds" << endl;
   
   int randomCycles = kCacheInserts * 4;
   cout << "--> fully randomized insert and lookup with " << randomCycles << " inserts and SIMULTANIOUSLY lookup of " << randomCycles << " entries" << endl;
   
   hits = 0;
   misses = 0;
   
   stopper.reset();
   stopper.start();
   for (int i = 0; i < randomCycles; ++i) {
      keyVal = getRandomValueBetween(0, kCacheInserts - 1);
      struct catalog::t_dirent dirent;
      dirent.inode = keyVal;
      cache.insert(getMd5Path(keyVal), dirent);
   
      // lookup
      keyVal = getRandomValueBetween(0, kCacheInserts - 1);
      if (cache.lookup(getMd5Path(keyVal), result)) {
         ++hits;
         if (result.inode != keyVal) return 3;
      } else {
         ++misses;
      }
   }
   stopper.stop();
   
   cout << "<-- hits: " << hits << " misses: " << misses << endl;
   cout << "<-- took: " << stopper.getTime() << " seconds" << endl;
	
	return 0;
}
