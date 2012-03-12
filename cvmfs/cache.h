/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_H_
#define CVMFS_CACHE_H_

#include <stdint.h>
#include <string>

namespace catalog {
class DirectoryEntry;
}

namespace hash {
struct Any;
}

namespace cache {

extern std::string cache_path;

bool Init(const std::string &cache_path, const std::string &root_url);
void Fini();

int Open(const hash::Any &id);
bool Open2Mem(const hash::Any &id, char **buffer, uint64_t *size);
int StartTransaction(const hash::Any &id,
                     std::string *final_path, std::string *temp_path);
int AbortTransaction(const std::string &temp_path);
int CommitTransaction(const std::string &final_path,
                      const std::string &temp_path,
                      const std::string &cvmfs_path,
                      const hash::Any &hash,
                      const uint64_t size);
bool CommitFromMem(const hash::Any &id, const char *buffer,
                   const uint64_t size, const std::string &cvmfs_path);
bool Contains(const hash::Any &id);
int Fetch(const catalog::DirectoryEntry &d, const std::string &cvmfs_path);

}  // namespace cache

#endif  // CVMFS_CACHE_H_
