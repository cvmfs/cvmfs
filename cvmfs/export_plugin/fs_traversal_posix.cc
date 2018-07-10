/**
 * This file is part of the CernVM File System.
 */
#include "fs_traversal_posix.h"

// #include <attr/xattr.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <attr/xattr.h>  // NOLINT
// Necessary because xattr.h does not import sys/types.h

#include <iostream>
#include <string>
#include <vector>

#include "fs_traversal_interface.h"
#include "hash.h"
#include "libcvmfs.h"
#include "shortstring.h"
#include "string.h"
#include "util.h"
#include "util/posix.h"
#include "xattr.h"

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

bool IsPrefixOf(std::string str, std::string prefix)
{
    return std::equal(
      str.begin(),
      str.begin() + prefix.size(),
      prefix.begin() );
}

std::string BuildPath(struct fs_traversal_context *ctx,
  const char *dir) {
  std::string result = ctx->repo;
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
  const struct cvmfs_stat *stat_info, bool set_permissions = true)
{
  int res = 0;
  if (set_permissions) {
    res = chmod(path, stat_info->st_mode);
    if (res != 0) return -1;
    res = chown(path, stat_info->st_uid, stat_info->st_gid);
    if (res != 0) return -1;
  }
  // TODO(steuber): Set xattrs with setxattr (if symlink no user. xattrs!)
  XattrList *xlist = reinterpret_cast<XattrList *>(stat_info->cvm_xattrs);
  std::vector<std::string> v = xlist->ListKeys();
  std::string val;
  for (std::vector<std::string>::iterator it = v.begin(); it != v.end(); ++it) {
    if (!set_permissions) {
      continue;
    }
    xlist->Get(*it, &val);
    int res = lsetxattr(path, it->c_str(), val.c_str(), val.length(), 0);
    if (res != 0) return -1;
  }
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
    if (!DirectoryExists(cur_path+"/"+max_dir_name)) {
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
  *len = 0;
  size_t buflen = 5;
  *buf = reinterpret_cast<char **>(malloc(sizeof(char *) * buflen));

  DIR *dr = opendir(BuildPath(ctx, dir).c_str());

  if (dr == NULL) {
    return;
  }

  while ((de = readdir(dr)) != NULL) {
    if (strcmp(de->d_name, ".") != 0
      && strcmp(de->d_name, "..") != 0) {
      AppendStringToList(de->d_name, buf, len, &buflen);
    }
  }

  closedir(dr);
  return;
}

int posix_get_stat(struct fs_traversal_context *ctx,
  const char *path, struct cvmfs_stat *stat_result) {
  // NOTE(steuber): Save hash + last modified(!=changed) as xattr
  // -> Could also be done for directories! (chmod changes on file change)
  // Probably more realistic without last modified though
  // (means we need to know that fs wasn`t changed)
  // -> only hash if necessary?
  std::string complete_path = BuildPath(ctx, path);
  struct stat buf;
  int res = lstat(complete_path.c_str(), &buf);
  if (res == -1) {
    return -1;
  }
  stat_result->st_ino = buf.st_ino;
  stat_result->st_mode = buf.st_mode;
  stat_result->st_nlink = buf.st_nlink;
  stat_result->st_uid = buf.st_gid;
  stat_result->st_gid = buf.st_gid;
  stat_result->st_rdev = buf.st_rdev;
  stat_result->st_size = buf.st_size;
  stat_result->st_blksize = buf.st_blksize;
  stat_result->st_blocks = buf.st_blocks;
  stat_result->mtime = buf.st_mtime;

  // Calculate hash
  // NOTE(steuber): Which hashing algorithm? (Not Sha1?)
  shash::Any cvm_checksum = shash::Any(shash::kSha1);
  shash::HashFile(complete_path, &cvm_checksum);
  std::string checksum_string = cvm_checksum.ToString();
  stat_result->cvm_checksum = strdup(checksum_string.c_str());
  if (S_ISLNK(buf.st_mode)) {
    char slnk[PATH_MAX+1];
    const ssize_t length =
      readlink(complete_path.c_str(), slnk, PATH_MAX);
    if (length < 0) {
      return -1;
    }
    slnk[length] = '\0';
    stat_result->cvm_symlink = strdup(slnk);
  } else {
    stat_result->cvm_symlink = NULL;
  }
  stat_result->cvm_name = strdup(path);

  stat_result->cvm_xattrs = XattrList::CreateFromFile(complete_path);
  if (stat_result->cvm_xattrs == NULL) return -1;

  return 0;
}

const char *posix_get_identifier(struct fs_traversal_context *ctx,
  const struct cvmfs_stat *stat) {
  shash::Any content_hash =
    shash::MkFromHexPtr(shash::HexPtr(stat->cvm_checksum));
  shash::Any meta_hash = HashMeta(stat);
  std::string path = ("/"
    + content_hash.MakePathExplicit(kDirLevels, kDigitsPerDirLevel, '.')
    + meta_hash.ToString());
  const char *res = strdup(path.c_str());
  return res;
}

bool posix_has_file(struct fs_traversal_context *ctx,
  const char *ident) {
  return FileExists(BuildHiddenPath(ctx, ident));
}

int posix_do_unlink(struct fs_traversal_context *ctx,
  const char *path) {
  std::string complete_path = BuildPath(ctx, path);
  int res = unlink(complete_path.c_str());
  if (res == -1) return -1;
  return 0;
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

int posix_cleanup_path(struct fs_traversal_context *ctx,
  const char *path) {
  std::string complete_path = BuildPath(ctx, path);
  std::string dirname = GetParentPath(complete_path);
  if (!DirectoryExists(dirname)) {
    // Directory doesn't exist
    errno = ENOENT;
    return -1;
  }
  if (FileExists(complete_path) || SymlinkExists(complete_path)) {
    // Unlink file if existing
    int res = posix_do_unlink(ctx, path);
    if (res != 0) return -1;
  }
  if (DirectoryExists(complete_path)) {
    int res = posix_do_rmdir(ctx, path);
    if (res != 0) return -1;
  }
  return 0;
}

int posix_do_link(struct fs_traversal_context *ctx,
  const char *path,
  const char *identifier) {
  std::string complete_path = BuildPath(ctx, path);
  std::string hidden_datapath = BuildHiddenPath(ctx, identifier);
  if (!FileExists(hidden_datapath)) {
    // Hash file doesn't exist
    errno = ENOENT;
    return -1;
  }
  if (posix_cleanup_path(ctx, path) != 0) {
    return -1;
  }
  int res = link(hidden_datapath.c_str(), complete_path.c_str());
  if (res != 0) return -1;
  return 0;
}

int posix_do_mkdir(struct fs_traversal_context *ctx,
              const char *path,
              const struct cvmfs_stat *stat_info) {
  std::string complete_path = BuildPath(ctx, path);
  std::string dirname = GetParentPath(complete_path);
  if (posix_cleanup_path(ctx, path) != 0) {
    return -1;
  }
  int res = mkdir(complete_path.c_str(), stat_info->st_mode);
  if (res != 0) return -1;
  return PosixSetMeta(complete_path.c_str(), stat_info);
}

int posix_do_symlink(struct fs_traversal_context *ctx,
              const char *src,
              const char *dest,
              const struct cvmfs_stat *stat_info) {
  std::string complete_src_path = BuildPath(ctx, src);
  std::string complete_dest_path = dest;
  if (posix_cleanup_path(ctx, src) != 0) {
    return -1;
  }
  int res = symlink(complete_dest_path.c_str(), complete_src_path.c_str());
  if (res != 0) return -1;
  return PosixSetMeta(complete_src_path.c_str(), stat_info, false);
}

int posix_touch(struct fs_traversal_context *ctx,
              const struct cvmfs_stat *stat_info) {
  // NOTE(steuber): creat is only atomic on non-NFS paths!
  const char *identifier = posix_get_identifier(ctx, stat_info);
  if (posix_has_file(ctx, identifier)) {
    errno = EEXIST;
    return -1;
  }
  std::string hidden_datapath = BuildHiddenPath(ctx, identifier);
  int res1 = creat(hidden_datapath.c_str(), stat_info->st_mode);
  if (res1 < 0) return -1;
  int res2 = close(res1);
  if (res2 < 0) return -1;
  return PosixSetMeta(hidden_datapath.c_str(), stat_info);
}


struct posix_file_handle {
  std::string path;
  FILE *fd;
};

void *posix_get_handle(struct fs_traversal_context *ctx,
              const char *identifier) {
  struct posix_file_handle *file_ctx = new struct posix_file_handle;
  file_ctx->path = BuildHiddenPath(ctx, identifier);

  return file_ctx;
}

int posix_do_fopen(void *file_ctx, fs_open_type op_mode) {
  struct posix_file_handle *handle =
    reinterpret_cast<posix_file_handle *>(file_ctx);
  const char *mode = "r";
  if (op_mode == fs_open_write) {
    mode = "w";
  } else if (op_mode == fs_open_append) {
    mode = "a";
  }
  FILE *fd = fopen(handle->path.c_str(), mode);
  if (fd == NULL) {
    return -1;
  }
  handle->fd = fd;
  return 0;
}

int posix_do_fclose(void *file_ctx) {
  struct posix_file_handle *handle =
    reinterpret_cast<posix_file_handle *>(file_ctx);
  int res = fclose(handle->fd);
  if (res != 0) return -1;
  handle->fd = NULL;
  return 0;
}

int posix_do_fread(void *file_ctx, char *buff, size_t len, size_t *read_len) {
  struct posix_file_handle *handle =
    reinterpret_cast<posix_file_handle *>(file_ctx);
  *read_len = fread(buff, sizeof(char), len, handle->fd);
  if (*read_len < len && ferror(handle->fd) != 0) {
      clearerr(handle->fd);
      return -1;
  }
  return 0;
}

int posix_do_fwrite(void *file_ctx, const char *buff, size_t len) {
  struct posix_file_handle *handle =
    reinterpret_cast<posix_file_handle *>(file_ctx);
  size_t written_len = fwrite(buff, sizeof(char), len, handle->fd);
  if (written_len != len) {
    return -1;
  }
  return 0;
}

void posix_do_ffree(void *file_ctx) {
  struct posix_file_handle *handle =
    reinterpret_cast<posix_file_handle *>(file_ctx);
  if (handle->fd != NULL) {
    posix_do_fclose(file_ctx);
  }
  delete handle;
}


struct fs_traversal_context *posix_initialize(
  const char *repo,
  const char *data) {
  fs_traversal_context *result = new struct fs_traversal_context;
  result->version = 1;
  result->repo =  strdup(repo);
  result->data = strdup(data);
  PosixCheckDirStructure(data, 0770);  // NOTE(steuber): mode?
  return result;
}

void posix_finalize(struct fs_traversal_context *ctx) {
  // TODO(steuber): Garbage collection
  delete ctx->repo;
  delete ctx->data;
  delete ctx;
}



struct fs_traversal *posix_get_interface() {
  struct fs_traversal *result = new struct fs_traversal;
  result->initialize = posix_initialize;
  result->finalize = posix_finalize;
  result->list_dir = posix_list_dir;
  result->get_stat = posix_get_stat;
  result->has_file = posix_has_file;
  result->get_identifier = posix_get_identifier;
  result->do_link = posix_do_link;
  result->do_unlink = posix_do_unlink;
  result->do_mkdir = posix_do_mkdir;
  result->do_rmdir = posix_do_rmdir;
  result->touch = posix_touch;
  result->get_handle = posix_get_handle;
  result->do_symlink = posix_do_symlink;

  result->do_fopen = posix_do_fopen;
  result->do_fclose = posix_do_fclose;
  result->do_fread = posix_do_fread;
  result->do_fwrite = posix_do_fwrite;
  result->do_ffree = posix_do_ffree;

  return result;
}
