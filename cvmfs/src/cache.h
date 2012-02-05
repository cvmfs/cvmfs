/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_H_
#define CVMFS_CACHE_H_

#include <stdint.h>
#include <string>

namespace cvmfs {
class DirectoryEntry;
}

namespace hash {
struct t_sha1;
}

namespace cache {

extern std::string cache_path;

bool Init(const std::string &cache_path, const std::string &root_url);
void Fini();

int Open(const hash::t_sha1 &id);
bool Open2Mem(const hash::t_sha1 &id, char **buffer, uint64_t *size);
int StartTransaction(const hash::t_sha1 &id,
                     std::string *final_path, std::string *temp_path);
int AbortTransaction(const std::string &temp_path);
int CommitTransaction(const std::string &final_path,
                      const std::string &temp_path,
                      const std::string &cvmfs_path,
                      const hash::t_sha1 &hash,
                      const uint64_t size);
bool CommitFromMem(const hash::t_sha1 &id, const char *buffer,
                   const uint64_t size, const std::string &cvmfs_path);
bool Contains(const hash::t_sha1 &id);
int Fetch(const cvmfs::DirectoryEntry &d, const std::string &cvmfs_path);

}  // namespace cache

#endif  // CVMFS_CACHE_H_
