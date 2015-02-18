/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_SQLITEVFS_H_
#define CVMFS_SQLITEVFS_H_

#include <string>

namespace perf {
class Statistics;
}

namespace sqlite {

enum VfsOptions {
  kVfsOptNone = 0,
  kVfsOptDefault,
};

bool RegisterVfsRdOnly(const std::string &name,
                       perf::Statistics *statistics,
                       const VfsOptions options);
bool UnregisterVfsRdOnly(const std::string &vfs_name);

}  // namespace sqlite

#endif  // CVMFS_SQLITEVFS_H_
