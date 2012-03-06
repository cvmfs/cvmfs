/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_TALK_H_
#define CVMFS_TALK_H_

#include <string>

namespace talk {

bool Init(const std::string &cachedir);
void Spawn();
void Fini();

}  // namespace talk

#endif  // CVMFS_TALK_H_
