/**
 * This file is part of the CernVM File System.
 */
#include "helpers.h"

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <utime.h>

#include <attr/xattr.h>  // NOLINT
// Necessary because xattr.h does not import sys/types.h

#include <string>
#include <vector>

#include "data_dir_mgmt.h"
#include "export_plugin/fs_traversal_interface.h"
#include "garbage_collector.h"
#include "util/posix.h"
#include "xattr.h"

void InitialFsOperations(struct fs_traversal_context *ctx) {
  InitializeDataDirectory(ctx);
  InitializeWarningFile(ctx);
  InitializeGarbageCollection(ctx);
}

void FinalizeFsOperations(struct fs_traversal_context *ctx) {
  FinalizeGarbageCollection(ctx);
}

void InitializeWarningFile(struct fs_traversal_context *ctx) {
  const char *warning = WARNING_FILE_NAME;
  FILE *f = fopen(BuildPath(ctx, "/" WARNING_FILE_NAME).c_str(), "w");
  if (f != NULL) {
    fwrite(warning, sizeof(char), strlen(warning), f);
    fclose(f);
  }
}

std::string BuildPath(struct fs_traversal_context *ctx,
  const char *dir) {
  std::string result = ctx->base;
  result += ctx->repo;
  if (dir[0] != '/') {
    result += "/";
  }
  result += dir;
  return result;
}

std::string BuildHiddenPath(struct fs_traversal_context *ctx,
  const char *ident) {
  std::string cur_path = ctx->data;
  cur_path += ident;
  return cur_path;
}

int PosixSetMeta(const char *path,
  const struct cvmfs_attr *stat_info, bool set_permissions/* = true*/) {
  int res = 0;
  if (set_permissions) {
    res = chmod(path, stat_info->st_mode);
    if (res != 0) return -1;
    res = chown(path, stat_info->st_uid, stat_info->st_gid);
    if (res != 0) return -1;
  }
  // TODO(steuber): Set xattrs with setxattr (if symlink no user. xattrs!)
  XattrList *xlist = reinterpret_cast<XattrList *>(stat_info->cvm_xattrs);
  if (xlist) {
    std::vector<std::string> v = xlist->ListKeys();
    std::string val;
    if (set_permissions) {
      for (std::vector<std::string>::iterator it = v.begin();
            it != v.end();
            ++it) {
        xlist->Get(*it, &val);
        int res = lsetxattr(path, it->c_str(), val.c_str(), val.length(), 0);
        if (res != 0) return -1;
      }
    }
  }
  if (res != 0) return -1;
  const struct timeval times[2] = {
    {stat_info->mtime, 0},
    {stat_info->mtime, 0}
  };
  res = lutimes(path, times);
  if (res != 0) return -1;
  return 0;
}
