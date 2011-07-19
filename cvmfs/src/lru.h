#ifndef _LRU_H
#define _LRU_H 1

#include <stdint.h>
#include <string>
#include <vector>
#include "hash.h"

namespace lru {
   const unsigned char FTYPE_REG = 0;
   const unsigned char FTYPE_CLG = 1;

   bool init(const std::string &cache_dir, const uint64_t limit, 
             const uint64_t limit_threshold, const bool dont_build);
   void spawn();
   void fini();
   
   bool build();
   bool cleanup(const uint64_t leave_size);
   bool cleanup_unprotected(const uint64_t leave_size);

   bool insert(const hash::t_sha1 &sha1, const uint64_t size, 
               const std::string &cmvfs_path);
   bool pin(const hash::t_sha1 &sha1, const uint64_t size, 
            const std::string &cmvfs_path);
   void touch(const hash::t_sha1 &file);
   void remove(const hash::t_sha1 &file);
   std::vector<std::string> list();
   std::vector<std::string> list_pinned();
   std::vector<std::string> list_catalogs();

   uint64_t max_file_size();
   uint64_t capacity();
   uint64_t size();
   uint64_t size_pinned();
   
   void lock();
   void unlock();
   
   std::string get_memory_usage(); /* Lock manually */
}

#endif
