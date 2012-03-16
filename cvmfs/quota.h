/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_QUOTA_H_
#define CVMFS_QUOTA_H_

#include <stdint.h>

#include <string>
#include <vector>

namespace hash {
  struct Any;
}

namespace quota {

bool Init(const std::string &cache_dir, const uint64_t limit,
          const uint64_t limit_threshold, const bool rebuild_database);
void Spawn();
void Fini();

  bool RebuildDatabase();
bool Cleanup(const uint64_t leave_size);

void Insert(const hash::Any &hash, const uint64_t size,
            const std::string &cmvfs_path);
bool Pin(const hash::Any &hash, const uint64_t size,
         const std::string &path_on_cvmfs);
void Unpin(const hash::Any &hash);
void Touch(const hash::Any &hash);
void Remove(const hash::Any &file);
std::vector<std::string> List();
std::vector<std::string> ListPinned();
std::vector<std::string> ListCatalogs();

uint64_t GetMaxFileSize();
uint64_t GetCapacity();
uint64_t GetSize();
uint64_t GetSizePinned();
std::string GetMemoryUsage();

}  // namespace quota

#endif  // CVMFS_QUOTA_H_
