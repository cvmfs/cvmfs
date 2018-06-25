/**
 * This file is part of the CernVM File System.
 */
#include "fs_traversal_posix.h"

#include <dirent.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>


#include "hash.h"
#include "shortstring.h"
#include "string.h"
#include "util/posix.h"

PosixFsObjectImplementor::PosixFsObjectImplementor(PathString path_param) {
  path_ = path_param;
}

NameString PosixFsObjectImplementor::DoGetIdentifier() {
  return GetFileName(path_);
}

PathString PosixFsObjectImplementor::DoGetPath() {
  return path_;
}

void PosixFsObjectImplementor::DoGetIterator(FsIterator *iterator) {
  assert(true);  // TODO(steuber): Assert folder
}

shash::Any PosixFsObjectImplementor::DoGetHash() {
  assert(true);  // TODO(steuber): Assert file
}

PathString PosixFsObjectImplementor::DoGetDestination() {
  assert(true);  // TODO(steuber): Assert symlink
}


FsObject *PosixFsIterator::GetCurrent() {
  // TODO(steuber)
}

bool PosixFsIterator::HasCurrent() {
  // TODO(steuber)
}

void PosixFsIterator::Step() {
  // TODO(steuber)
}


PosixDestinationFsInterface::PosixDestinationFsInterface(
  PathString root_path_param) {
  root_path_ = root_path_param;
}

void PosixDestinationFsInterface::GetRootIterator(FsIterator *iterator) {
  // TODO(steuber)
}

bool PosixDestinationFsInterface::HasFile(const shash::Any &hash) {
  return FileExists(get_hashed_path(hash));
}

int PosixDestinationFsInterface::Link(
  const PathString &path,
  const shash::Any &hash) {
  if (!HasFile(hash)) {
    return -4;
  }
  std::string complete_path = get_visible_path(path);
  std::string dirname;
  std::string filename;
  SplitPath(complete_path, &dirname, &filename);
  if (FileExists(complete_path)) {
    return -3;
  }
  if (!MkdirDeep(dirname, 0770)) {  // TODO(steuber): Mode?
    return -2;
  }
  int res = link(get_hashed_path(hash).c_str(), complete_path.c_str());
  if (res == -1) return -1;
  return 0;
}

int PosixDestinationFsInterface::Unlink(const PathString &path) {
  std::string complete_path = get_visible_path(path);
  int res = unlink(complete_path.c_str());
  if (res == -1) return -1;
  return 0;
}

int PosixDestinationFsInterface::CreateDirectory(const PathString &path) {
  std::string complete_path = get_visible_path(path);
  if (!MkdirDeep(complete_path.c_str(), 0770)) {  // TODO(steuber): Mode?
    return -1;
  }
  return 0;
}

int PosixDestinationFsInterface::RemoveDirectory(const PathString &path) {
  std::string complete_path = get_visible_path(path);
  if (!RemoveTree(complete_path.c_str())) {
    return -1;
  }
  return 0;
}

int PosixDestinationFsInterface::CopyFile(
  const FILE *fd,
  const shash::Any &hash) {
  // TODO(steuber)
}

int PosixDestinationFsInterface::CreateSymlink(
  const PathString &src,
  const PathString &dest) {
  if (!FileExists(get_visible_path(dest))) {
    return -4;
  }
  std::string dirname;
  std::string filename;
  SplitPath(get_visible_path(src), &dirname, &filename);
  if (!MkdirDeep(dirname, 0770)) {  // TODO(steuber): Mode?
    return -3;
  }
  int res = symlink(dest.c_str(), src.c_str());
  if (res == -1) return -1;
  return 0;
}

int PosixDestinationFsInterface::GarbageCollection() {
  DIR *dp;
  struct dirent *ep;  
  struct stat st; 
  std::string hidden_datapath = (root_path_.ToString() + "/" + HiddenFileDir);  
  dp = opendir(hidden_datapath.c_str());

  if (dp != NULL)
  {
    while (ep = readdir(dp)) {
      std::string curPath = hidden_datapath+std::string (ep->d_name);
      if (stat(curPath.c_str(), &st) == -1) return -2;
      if (st.st_nlink==1) {
        unlink(curPath.c_str());
      }
    }
    closedir(dp);
  } else {
    return -2;
  }
}
std::string PosixDestinationFsInterface::get_hashed_path(
    const shash::Any &hash) {
  return root_path_.ToString() + "/" + HiddenFileDir + "/" + hash.ToString();
}

std::string PosixDestinationFsInterface::get_visible_path(
  const PathString &path) {
  return root_path_.ToString() + "/" + path.ToString();
}

const std::string PosixDestinationFsInterface::HiddenFileDir = ".data";

std::string hidden_datapath = ".data";

struct DestinationFsContext {
  std::string 
}