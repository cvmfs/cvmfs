/**
 * This file is part of the CernVM File System.
 */
#include "helpers.h"

#ifdef __GLIBC__
#include <gnu/libc-version.h>
#endif
#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <utime.h>

// #include <attr/xattr.h>  // NOLINT
// Necessary because xattr.h does not import sys/types.h

#include <string>
#include <vector>

#include "data_dir_mgmt.h"
#include "garbage_collector.h"
#include "shrinkwrap/fs_traversal_interface.h"
#include "util/logging.h"
#include "util/platform.h"
#include "util/posix.h"
#include "xattr.h"

#ifdef __GLIBC__
#if __GLIBC_MINOR__ < 6
#warning No lutimes support, glibc >= 2.6 required
#else
#define CVMFS_HAS_LUTIMES 1
#endif
#else
#define CVMFS_HAS_LUTIMES 1
#endif


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
  } else {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Could not write warning file for posix file system!");
  }
}

std::string BuildPath(struct fs_traversal_context *ctx, const char *dir) {
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

int PosixSetMeta(const char *path, const struct cvmfs_attr *stat_info,
                 bool set_permissions /* = true*/) {
  int res = 0;
  if (set_permissions) {
    res = chmod(path, stat_info->st_mode);
    if (res != 0)
      return -1;
    res = chown(path, stat_info->st_uid, stat_info->st_gid);
    if (res != 0)
      return -1;
  }
  std::string path_str = std::string(path);
  if (stat_info->cvm_xattrs != NULL) {
    XattrList *xlist = reinterpret_cast<XattrList *>(stat_info->cvm_xattrs);
    if (xlist) {
      std::vector<std::string> v = xlist->ListKeys();
      std::string val;
      if (set_permissions) {
        for (std::vector<std::string>::iterator it = v.begin(); it != v.end();
             ++it) {
          xlist->Get(*it, &val);
          bool res = platform_lsetxattr(path_str, *it, val);
          if (!res)
            return -1;
        }
      }
    }
  }
#ifdef CVMFS_HAS_LUTIMES
  const struct timeval times[2] = {{stat_info->mtime, 0},
                                   {stat_info->mtime, 0}};
  res = lutimes(path, times);
  if (res != 0)
    return -1;
#endif
  return 0;
}

bool BackupMtimes(std::string path, struct utimbuf *mtimes) {
  // According to valgrind this is necessary due to struct paddings here...
  struct stat stat_buf;
  int res = stat(path.c_str(), &stat_buf);
  if (res != 0)
    return false;
  mtimes->actime = stat_buf.st_mtime;
  mtimes->modtime = stat_buf.st_mtime;
  return true;
}
