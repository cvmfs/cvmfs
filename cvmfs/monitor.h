/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_MONITOR_H_
#define CVMFS_MONITOR_H_

#include <string>

namespace monitor {

bool Init(const std::string &cache_dir, const std::string &process_name,
          const bool check_max_open_files);
void Fini();
void Spawn();

pid_t GetPid();

unsigned GetMaxOpenFiles();

}  // namespace monitor

#endif  // CVMFS_MONITOR_H_
