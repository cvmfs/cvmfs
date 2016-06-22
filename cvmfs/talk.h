/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_TALK_H_
#define CVMFS_TALK_H_

#include <string>

#include "util/pointer.h"

class MountPoint;
class OptionsManager;

/**
 * Provides a command & control interface to the MountPoint class.  Data is
 * exchanged through a UNIX domain socket.  Used by the cvmfs_talk utility.
 */
class TalkManager : SingleCopy {
 public:
  static TalkManager *Create(const std::string &socket_path,
                             MountPoint *mount_point);
  ~TalkManager();

  void Spawn();

 private:
  TalkManager();

  std::string socket_path_;
  MountPoint *mount_point_;
};

namespace talk {

bool Init(const std::string &cachedir, OptionsManager *options_manager);
void Spawn();
void Fini();

}  // namespace talk

#endif  // CVMFS_TALK_H_
