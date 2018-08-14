/**
 * This file is part of the CernVM File System
 */

#ifndef CVMFS_CVMFS_SUID_UTIL_H_
#define CVMFS_CVMFS_SUID_UTIL_H_

#include <string>

namespace cvmfs_suid {

std::string EscapeSystemdUnit(const std::string &path);
bool PathExists(const std::string &path);
std::string ResolvePath(const std::string &path);

}  // namespace cvmfs_suid

#endif  // CVMFS_CVMFS_SUID_UTIL_H_
