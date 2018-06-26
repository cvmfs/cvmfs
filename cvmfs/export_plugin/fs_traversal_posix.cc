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


void AppendStringToList(char const   *str,
                        char       ***buf,
                        size_t       *listlen,
                        size_t       *buflen)
{
  if (*listlen + 1 >= *buflen) {
       size_t newbuflen = (*listlen)*2 + 5;
       *buf = reinterpret_cast<char **>(
         realloc(*buf, sizeof(char *) * newbuflen));
       assert(*buf);
       *buflen = newbuflen;
       assert(*listlen < *buflen);
  }
  if (str) {
    (*buf)[(*listlen)] = strdup(str);
    // null-terminate the list
    (*buf)[++(*listlen)] = NULL;
  } else {
    (*buf)[(*listlen)] = NULL;
  }
}



void listdir(const char *dir, char ***buf, size_t len){
  struct dirent *de;

  DIR *dr = opendir(dir);

  if(dr == NULL) {
    return;
  }

  size_t listlen = 0;
  AppendStringToList(NULL, buf, &listlen, buflen);

  while((de == readdir(dr)) != NULL){
    AppendStringToList(de->d_name, buf, &listlen, buflen);
  }

  closedir(dr);
  return 0;

}

bool has_hash(void *hash) {
  return FileExists(get_hashed_path((shash::Any *)hash));
}

int link(
  const PathString &path,
  void *hash) {
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
