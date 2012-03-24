/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PEERS_H_
#define CVMFS_PEERS_H_

#include <string>

namespace peers {

bool Init(const std::string &cachedir, const std::string &exe_path);
void Fini();
int MainPeerServer(int argc, char **argv);

}  // namespace peers

#endif  // CVMFS_PEERS_H_
