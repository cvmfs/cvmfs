/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_QUOTA_H_
#define CVMFS_QUOTA_H_

#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>
#include <vector>

namespace shash {
struct Any;
}

namespace quota {

static const std::string checksum_file_prefix = "cvmfschecksum";

bool Init(const std::string &cache_dir, const uint64_t limit,
          const uint64_t cleanup_threshold, const bool rebuild_database);
bool InitShared(const std::string &exe_path, const std::string &cache_dir,
                const uint64_t limit, const uint64_t cleanup_threshold);
void Spawn();
void Fini();
int MainCacheManager(int argc, char **argv);


bool RebuildDatabase();
bool Cleanup(const uint64_t leave_size);

void Insert(const shash::Any &hash, const uint64_t size,
            const std::string &cmvfs_path);
void InsertVolatile(const shash::Any &hash, const uint64_t size,
                    const std::string &cmvfs_path);
bool Pin(const shash::Any &hash, const uint64_t size,
         const std::string &path_on_cvmfs, const bool is_catalog);
void Unpin(const shash::Any &hash);
void Touch(const shash::Any &hash);
void Remove(const shash::Any &file);
std::vector<std::string> List();
std::vector<std::string> ListPinned();
std::vector<std::string> ListCatalogs();

void RegisterBackChannel(int back_channel[2], const std::string &channel_id);
void UnregisterBackChannel(int back_channel[2], const std::string &channel_id);

uint64_t GetMaxFileSize();
uint64_t GetCapacity();
uint64_t GetSize();
uint64_t GetSizePinned();
pid_t GetPid();
std::string GetMemoryUsage();

}  // namespace quota

#endif  // CVMFS_QUOTA_H_
