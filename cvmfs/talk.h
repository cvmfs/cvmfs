/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_TALK_H_
#define CVMFS_TALK_H_

#include <pthread.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "util/single_copy.h"

namespace download {
class DownloadManager;
}
class FileSystem;
class FuseRemounter;
class MountPoint;
class OptionsManager;


/**
 * Provides a command & control interface to the MountPoint class.  Data is
 * exchanged through a UNIX domain socket.  Used by the cvmfs_talk utility.
 */
class TalkManager : SingleCopy {
 public:
  static TalkManager *Create(const std::string &socket_path,
                             MountPoint *mount_point,
                             FuseRemounter *remounter);
  ~TalkManager();

  void Spawn();

 private:
  /**
   * Maximum number of characters that can be read as a command from the socket.
   */
  static const unsigned kMaxCommandSize = 512;

  TalkManager(const std::string &socket_path,
              MountPoint *mount_point,
              FuseRemounter *remounter);
  static void *MainResponder(void *data);
  void Answer(int con_fd, const std::string &msg);
  void AnswerStringList(int con_fd, const std::vector<std::string> &list);
  std::string FormatMetalinkInfo(download::DownloadManager *download_mgr);
  std::string FormatHostInfo(download::DownloadManager *download_mgr);
  std::string FormatProxyInfo(download::DownloadManager *download_mgr);
  std::string FormatLatencies(const MountPoint &mount_point,
                              FileSystem *file_system);

  std::string socket_path_;
  int socket_fd_;
  MountPoint *mount_point_;
  FuseRemounter *remounter_;
  pthread_t thread_talk_;
  bool spawned_;
};

#endif  // CVMFS_TALK_H_
