/**
 * This file is part of the CernVM File System.
 */
#include "fs_traversal_posix.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <string>

#include "fs_traversal_interface.h"
#include "hash.h"
#include "shortstring.h"
#include "string.h"
#include "util/posix.h"

// Maximum number of files open in parallel during rmdir
const int kPosixTraversalMaxDeletePar = 64;

const unsigned kDirLevels = 2;
const unsigned kDigitsPerDirLevel = 2;

void AppendStringToList(char const   *str,
                        char       ***buf,
                        size_t       *listlen,
                        size_t       *buflen) {
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

bool HasDir(std::string dirname) {
  struct stat sb;
  return (stat(dirname.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode));
}

std::string BuildPath(struct fs_traversal_context *ctx,
  const char *dir) {
  std::string result = ctx->repo;
  result += dir;
  return result;
}

std::string BuildHiddenPath(struct fs_traversal_context *ctx,
  const void *content,
  const void *meta) {
  std::string cur_path = ctx->repo;
  cur_path += "/";
  cur_path += ctx->data;
  shash::Any *content_hash = (shash::Any *)content;
  shash::Any *meta_hash = (shash::Any *)meta;
  return cur_path+"/"+content_hash->MakePathExplicit(
    kDirLevels, kDigitsPerDirLevel, '.')
    + meta_hash->ToString();
}

int PosixSetMeta(const char *path, const struct cvmfs_stat *stat_info) {
  int res = chmod(path, stat_info->st_mode);
  if (res != 0) return -1;
  // TODO(steuber): Set xattrs with setxattr
  if (res != 0) return -1;
  res = chown(path, stat_info->st_uid, stat_info->st_gid);
  if (res != 0) return -1;
  return 0;
}

void PosixCheckDirStructure(std::string cur_path, mode_t mode,
  unsigned int depth = 1) {
  // Maximum directory to search for:
  std::string max_dir_name = std::string(kDigitsPerDirLevel, 'f');
  // Build current base path

  int res = mkdir(cur_path.c_str(), mode);
  assert(res == 0 || errno == EEXIST);
  // Build template for directory names:
  assert(kDigitsPerDirLevel <= 99);
  char hex[kDigitsPerDirLevel+1];
  char dir_name_template[5];
  snprintf(dir_name_template,
    sizeof(dir_name_template),
    "%%%02ux",
    kDigitsPerDirLevel);
  // Go through all levels:
  for (; depth <= kDirLevels; depth++) {
    if (!HasDir(cur_path+"/"+max_dir_name)) {
      // Directories in this level not yet fully created...
      for (unsigned int i = 0;
      i < (((unsigned int) 1) << 4*kDigitsPerDirLevel);
      i++) {
        // Go through directories 0^kDigitsPerDirLevel to f^kDigitsPerDirLevel
        snprintf(hex, sizeof(hex), dir_name_template, i);
        std::string this_path = cur_path + "/" + std::string(hex);
        int res = mkdir(this_path.c_str(), mode);
        assert(res == 0 || errno == EEXIST);
        // Once directory created: Prepare substructures
        PosixCheckDirStructure(this_path, mode, depth+1);
      }
    } else {
      // Directories on this level fully created; check ./
      PosixCheckDirStructure(cur_path+"/"+max_dir_name, mode, depth+1);
    }
  }
}

void posix_list_dir(struct fs_traversal_context *ctx,
  const char *dir,
  char ***buf,
  size_t *len) {
  struct dirent *de;

  DIR *dr = opendir(BuildPath(ctx, dir).c_str());

  if (dr == NULL) {
    return;
  }

  *len = 0;
  AppendStringToList(NULL, buf, len, len);

  while ((de = readdir(dr)) != NULL) {
    AppendStringToList(de->d_name, buf, len, len);
  }

  closedir(dr);
  return;
}

struct cvmfs_stat *posix_get_stat(struct fs_traversal_context *ctx,
  const char *path) {
  // TODO(steuber): Where is the stat?
  // TODO(steuber): Save hash + last modified(!=changed) as xattr
  // -> Could also be done for directories! (chmod changes on file change)
  // Probably more realistic without last modified though
  // (means we need to know that fs wasn`t changed)
  // -> only hash if necessary?
  struct cvmfs_stat *result = new struct cvmfs_stat;
  return result;
}

bool posix_has_hash(struct fs_traversal_context *ctx,
  const void *content,
  const void *meta) {
  return FileExists(BuildHiddenPath(ctx, content, meta));
}

int posix_do_unlink(struct fs_traversal_context *ctx,
  const char *path) {
  std::string complete_path = BuildPath(ctx, path);
  int res = unlink(complete_path.c_str());
  if (res == -1) return -1;
  return 0;
}

int posix_do_link(struct fs_traversal_context *ctx,
  const char *path,
  void *content,
  void *meta) {
  std::string complete_path = BuildPath(ctx, path);
  std::string hidden_datapath = BuildHiddenPath(ctx, content, meta);
  std::string dirname = GetParentPath(complete_path);
  if (!FileExists(hidden_datapath)) {
    // Hash file doesn't exist
    return -2;
  }
  if (!HasDir(dirname)) {
    // Directory doesn't exist
    return -3;
  }
  if (FileExists(complete_path)) {
    // Unlink file if existing
    posix_do_unlink(ctx, path);
  }
  int res = link(hidden_datapath.c_str(), complete_path.c_str());
  if (res == -1) return -1;
  return 0;
}

int posix_do_mkdir(struct fs_traversal_context *ctx,
              const char *path,
              const struct cvmfs_stat *stat_info) {
  std::string complete_path = BuildPath(ctx, path);
  std::string dirname = GetParentPath(complete_path);
  if (!HasDir(dirname)) {
    // Parent directory doesn't exist
    return -3;
  }
  int res = mkdir(complete_path.c_str(), stat_info->st_mode);
  if (res != 0) return -1;
  return PosixSetMeta(complete_path.c_str(), stat_info);
}

int posix_do_unlink_cb(const char *fpath,
  const struct stat *sb,
  int typeflag,
  struct FTW *ftwbuf) {
  int rv = remove(fpath);
  return rv;
}

int posix_do_rmdir(struct fs_traversal_context *ctx,
              const char *path) {
  std::string complete_path = BuildPath(ctx, path);
  return nftw(complete_path.c_str(),
    posix_do_unlink_cb,
    kPosixTraversalMaxDeletePar,
    FTW_DEPTH | FTW_PHYS);
}

int posix_touch(struct fs_traversal_context *ctx,
              void *content,
              void *meta,
              const struct cvmfs_stat *stat_info) {
  if (posix_has_hash(ctx, content, meta)) {
    errno = EEXIST;
    return -1;
  }
  std::string hidden_datapath = BuildHiddenPath(ctx, content, meta);
  int res1 = creat(hidden_datapath.c_str(), stat_info->st_mode);
  if (res1 < 0) return -1;
  int res2 = close(res1);
  if (res2 < 0) return -1;
  return PosixSetMeta(hidden_datapath.c_str(), stat_info);
}


struct posix_file_handle {
  std::string path;
  int fd;
};

int posix_do_open(void *ctx, fs_open_type op_mode) {
  // TODO(steuber): ...
  return -1;
}

int posix_do_close(void *ctx) {
  // TODO(steuber): ...
  return -1;
}

int posix_do_read(void *ctx, char *buff, size_t len) {
  // TODO(steuber): ...
  return -1;
}

int posix_do_write(void *ctx, const char *buff) {
  // TODO(steuber): ...
  return -1;
}

/**
 * Retrieves a method struct which allows the manipulation of the file
 * defined by the given content and meta data hash
 * 
 * @param[in] content The content hash of the file
 * @param[in] meta The meta hash of the file
 */
struct fs_file *posix_get_handle(struct fs_traversal_context *ctx,
              void *content,
              void *meta) {
  struct fs_file *result = new struct fs_file;
  struct posix_file_handle *file_ctx = new struct posix_file_handle;
  file_ctx->path = BuildHiddenPath(ctx, content, meta);

  result->ctx = file_ctx;
  result->stat_info = NULL;  // TODO(steuber): Get stat
  return result;
}


/**
 * Method which creates a symlink at src which points to dest
 * 
 * @param[in] The position at which the symlink should be saved
 * (parent directory must exist)
 * @param[in] The position the symlink should point to
 */
int posix_do_symlink(struct fs_traversal_context *ctx,
              const char *src,
              const char *dest,
              const struct cvmfs_stat *stat_info) {
  std::string complete_src_path = BuildPath(ctx, src);
  std::string complete_dest_path = BuildPath(ctx, dest);
  std::string dirname = GetParentPath(complete_src_path);
  if (!HasDir(dirname)) {
    // Parent directory doesn't exist
    return -3;
  }
  int res = symlink(complete_dest_path.c_str(), complete_src_path.c_str());
  if (res != 0) return -1;
  return PosixSetMeta(complete_src_path.c_str(), stat_info);
}

/**
 * Method which executes a garbage collection on the destination file system.
 * This will remove all no longer linked content adressed files
 */
// NOTE(steuber): Shouldn't this maybe just be part of the finalize step?
int posix_garbage_collection(struct fs_traversal_context *ctx) {
  // TODO(steuber): ...
  return -1;
}





// NOTE(steuber): How does this work?
struct fs_traversal_context *posix_initialize(
  const char *repo,
  const char *data) {
  fs_traversal_context *result = new struct fs_traversal_context;
  result->version = 1;
  result->repo =  repo;
  result->data = data;
  std::string cur_path = repo;
  cur_path += "/";
  cur_path += data;
  PosixCheckDirStructure(cur_path, 0770);  // TODO(steuber): mode?
  return result;
}

void posix_finalize(struct fs_traversal_context *ctx) {
  // TODO(steuber): ...
}



struct fs_traversal *posix_get_interface() {
  struct fs_traversal *result = new struct fs_traversal;
  result->initialize = posix_initialize;
  result->finalize = posix_finalize;
  result->list_dir = posix_list_dir;
  result->get_stat = posix_get_stat;
  result->has_hash = posix_has_hash;
  result->do_link = posix_do_link;
  result->do_unlink = posix_do_unlink;
  result->do_mkdir = posix_do_mkdir;
  result->do_rmdir = posix_do_rmdir;
  result->touch = posix_touch;
  result->get_handle = posix_get_handle;
  result->do_symlink = posix_do_symlink;
  result->garbage_collection = posix_garbage_collection;

  result->do_open = posix_do_open;
  result->do_close = posix_do_close;
  result->do_read = posix_do_read;
  result->do_write = posix_do_write;

  return result;
}
