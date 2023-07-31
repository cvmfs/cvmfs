/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_LOADER_TALK_H_
#define CVMFS_LOADER_TALK_H_

#include <string>

namespace loader {
namespace loader_talk {

bool Init(const std::string &socket_path);
void Spawn();
void Fini();

int MainReload(const std::string &socket_path, const bool stop_and_go,
               const bool debug);

}  // namespace loader_talk
}  // namespace loader

#endif  // CVMFS_LOADER_TALK_H_
