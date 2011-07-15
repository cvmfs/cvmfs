#ifndef CACHE_H
#define CACHE_H 1

#include "catalog.h"
#include "hash.h"

#include <string>
#include <pthread.h>
#include <cstdlib>

extern "C" {
   #include "http_curl.h"
}

namespace cache {

   extern std::string cache_path;
   
   bool init(const std::string &c_path, const std::string &r_url, pthread_mutex_t * const m_download);
   void fini();
   
   int transaction(const hash::t_sha1 &id, std::string &lpath, std::string &txn);
   int abort(const std::string &txn);
   int commit(const std::string &lpath, const std::string &txn, const std::string &cvmfs_path,
              const hash::t_sha1 &sha1, const uint64_t size);
   bool contains(const hash::t_sha1 &id);
   int open(const hash::t_sha1 &id);
   int open_or_lock(const catalog::t_dirent &d);
   int fetch(const catalog::t_dirent &d, const std::string &path);
   
   bool mem_to_disk(const hash::t_sha1 &id, const char *buffer, const size_t size, const std::string &name);
   bool disk_to_mem(const hash::t_sha1 &id, char **buffer, size_t *size);

}


#endif
