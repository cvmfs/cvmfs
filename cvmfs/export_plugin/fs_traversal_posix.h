/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_POSIX_H_
#define CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_POSIX_H_

#include <sys/stat.h>
#include <string>

#include "fs_traversal_interface.h"
#include "hash.h"
#include "shortstring.h"
#include "string.h"
#include "util/posix.h"


class PosixFsObjectImplementor : public FsObjectImplementor {
 public:
  explicit PosixFsObjectImplementor(PathString path_param);

  NameString DoGetIdentifier();

  PathString DoGetPath();

  void DoGetIterator(FsIterator *iterator);

  shash::Any DoGetHash();

  PathString DoGetDestination();

 private:
  PathString path_;
};

class PosixFsIterator : public FsIterator {
 public:
  FsObject *GetCurrent();

  bool HasCurrent();

  void Step();
};

class PosixDestinationFsInterface : public DestinationFsInterface {
 public:
  explicit PosixDestinationFsInterface(PathString root_path_param);

  void GetRootIterator(FsIterator* iter);

  bool HasFile(const shash::Any &hash);

  int Link(const PathString &path, const shash::Any &hash);

  int Unlink(const PathString &path);

  int CreateDirectory(const PathString &path);

  int RemoveDirectory(const PathString &path);

  int CopyFile(const FILE *fd, const shash::Any &hash);

  int CreateSymlink(const PathString &src, const PathString &dest);

  int GarbageCollection();

  static const std::string HiddenFileDir;

 private:
  PathString root_path_;

  std::string get_hashed_path(const shash::Any &hash);

  std::string get_visible_path(const PathString &path);
};

#endif  // CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_POSIX_H_
