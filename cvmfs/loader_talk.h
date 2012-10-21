/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_LOADER_TALK_H_
#define CVMFS_LOADER_TALK_H_

#include <string>

namespace loader {

bool Init(const std::string &socket_path);
void Spawn();
void Fini();

}  // namespace loader

#endif  // CVMFS_LOADER_TALK_H_
