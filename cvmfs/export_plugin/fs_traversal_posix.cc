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
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <unistd.h>
#include <utime.h>

#include <attr/xattr.h>  // NOLINT
// Necessary because xattr.h does not import sys/types.h

#include <map>
#include <string>
#include <vector>

#include "fs_traversal_interface.h"
#include "hash.h"
#include "libcvmfs.h"
#include "logging.h"
#include "shortstring.h"
#include "string.h"
#include "util.h"
#include "util/posix.h"
#include "xattr.h"

const char kDirLevels = 2;
const char kDigitsPerDirLevel = 2;

struct fs_traversal_posix_context {
  std::map<ino_t, bool> gc_flagged;
};

struct posix_gc_thread{
  struct fs_traversal_context *ctx;
  unsigned thread_total;
  unsigned thread_num;
};

std::string BuildPath(struct fs_traversal_context *ctx,
  const char *dir) {
  std::string result = ctx->repo;
  result += "/";
  result += dir;
  return result;
}

std::string BuildHiddenPath(struct fs_traversal_context *ctx,
  const char *ident) {
  std::string cur_path = ctx->data;
  cur_path += ident;
  return cur_path;
}

void *PosixGcMainWorker(void *data) {
  struct posix_gc_thread *thread_context
    = reinterpret_cast<struct posix_gc_thread *>(data);
  struct fs_traversal_posix_context *posix_ctx
    = reinterpret_cast<struct fs_traversal_posix_context *>(
      thread_context->ctx->ctx);
  // Build path array
  int offset = strlen(thread_context->ctx->data)+1;
  // used for both path building and stat calls (therefore +257)
  char dir_path[offset+kDigitsPerDirLevel*kDirLevels+kDirLevels+257];
  snprintf(dir_path, offset, thread_context->ctx->data);
  dir_path[offset-1]='/';
  dir_path[offset+kDigitsPerDirLevel*kDirLevels+kDirLevels+257] = '\0';
  char dir_name_template[6];
  snprintf(dir_name_template,
    sizeof(dir_name_template),
    "%%%02ux/",
    kDigitsPerDirLevel);
  const unsigned directory_mask = (1 << (kDigitsPerDirLevel*4)) -1;
  const unsigned max_dir_name = (1 << kDigitsPerDirLevel*4);
  const unsigned max_val = (1 << (kDirLevels*kDigitsPerDirLevel*4));
  for (unsigned i = thread_context->thread_num*max_dir_name*(kDirLevels-1);
    i < max_val;
    i+=thread_context->thread_total*+(max_val/max_dir_name)) {
    // Iterate over paths of current subdirectory...
    for (unsigned j = i; j < i+(max_val/max_dir_name); j++) {
      // For every subdirectory chain (described by j)
      unsigned path_pos = offset;
      for (int level = kDirLevels-1;
        level >= 0;
        level--) {
        const unsigned cur_dir
          = (j >> (level*kDigitsPerDirLevel*4)) & directory_mask;
        snprintf(dir_path+path_pos,
          kDigitsPerDirLevel+2, dir_name_template, cur_dir);
        path_pos+=kDigitsPerDirLevel+1;
      }
      dir_path[path_pos]='\0';
      // Calculated path - now garbage collection...
      DIR *cur_dir_ent = opendir(dir_path);
      assert(cur_dir_ent != NULL);
      struct stat stat_buf;
      struct dirent *de;
      while ((de = readdir(cur_dir_ent)) != NULL) {
        if (posix_ctx->gc_flagged.count(de->d_ino) > 0
          && posix_ctx->gc_flagged[de->d_ino]) {
          snprintf(dir_path+path_pos, sizeof(dir_path)-path_pos, de->d_name);
          stat(dir_path, &stat_buf);
          if (stat_buf.st_nlink == 1) {
            int res = unlink(dir_path);
            assert(res == 0);
            posix_ctx->gc_flagged.erase(de->d_ino);
          }
        }
      }
      closedir(cur_dir_ent);
    }
  }
  return NULL;
}

int PosixSetMeta(const char *path,
  const struct cvmfs_attr *stat_info, bool set_permissions = true)
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

void PosixCheckDirStructure(std::string cur_path, mode_t mode,
  unsigned int depth = 1) {
  std::string max_dir_name = std::string(kDigitsPerDirLevel, 'f');
  // Build current base path
  if (depth == 1) {
    bool res1 = MkdirDeep(cur_path.c_str(), mode);
    assert(res1);
    int res2 = mkdir((cur_path+POSIX_GARBAGE_DIR).c_str(), mode);
    assert(res2 == 0 || errno == EEXIST);
  } else {
    int res = mkdir(cur_path.c_str(), mode);
    assert(res == 0 || errno == EEXIST);
  }
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

  // NULL terminate the list;
  AppendStringToList(NULL, buf, len, &buflen);

  if (dr == NULL) {
    return;
  }

  while ((de = readdir(dr)) != NULL) {
    if (strcmp(de->d_name, ".") != 0
      && strcmp(de->d_name, "..") != 0
      && strcmp(de->d_name, WARNING_FILE_NAME) != 0) {
      AppendStringToList(de->d_name, buf, len, &buflen);
    }
  }

  closedir(dr);
  return;
}

int posix_get_stat(struct fs_traversal_context *ctx,
  const char *path, struct cvmfs_attr *stat_result, bool get_hash) {
  std::string complete_path = BuildPath(ctx, path);
  struct stat buf;
  int res = lstat(complete_path.c_str(), &buf);
  if (res == -1) {
    return -1;
  }
  stat_result->st_dev = buf.st_dev;
  stat_result->st_ino = buf.st_ino;
  stat_result->st_mode = buf.st_mode;
  stat_result->st_nlink = buf.st_nlink;
  stat_result->st_uid = buf.st_gid;
  stat_result->st_gid = buf.st_gid;
  stat_result->st_rdev = buf.st_rdev;
  stat_result->st_size = buf.st_size;
  stat_result->mtime = buf.st_mtime;

  // Calculate hash
  if (get_hash) {
    shash::Any cvm_checksum = shash::Any(shash::kSha1);
    shash::HashFile(complete_path, &cvm_checksum);
    std::string checksum_string = cvm_checksum.ToString();
    stat_result->cvm_checksum = strdup(checksum_string.c_str());
  } else {
    // We usually do not calculate the checksum for posix files since it's a
    // destination file system.
    stat_result->cvm_checksum = NULL;
  }

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
  stat_result->cvm_parent = strdup(GetParentPath(path).c_str());
  stat_result->cvm_name = strdup(GetFileName(path).c_str());

  stat_result->cvm_xattrs = XattrList::CreateFromFile(complete_path);
  if (stat_result->cvm_xattrs == NULL) return -1;

  return 0;
}

int posix_set_meta(struct fs_traversal_context *ctx,
  const char *path, const struct cvmfs_attr *stat_info) {
  std::string complete_path = BuildPath(ctx, path);
  return PosixSetMeta(complete_path.c_str(), stat_info,
    !S_ISLNK(stat_info->st_mode));
}
const char *posix_get_identifier(struct fs_traversal_context *ctx,
  const struct cvmfs_attr *stat) {
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
  std::string parent_path = GetParentPath(complete_path);
  const char *complete_path_char = complete_path.c_str();
  struct stat buf;
  int res2 = lstat(complete_path_char, &buf);
  if (res2 == -1 && errno == ENOENT) return -1;
  assert(res2 == 0);
  struct stat parbuf;
  struct utimbuf mtimes;
  stat(parent_path.c_str(), &parbuf);
  mtimes.actime = parbuf.st_mtime;
  mtimes.modtime = parbuf.st_mtime;
  // Unlinking
  int res1 = unlink(complete_path_char);
  res1 |= utime(parent_path.c_str(), &mtimes);
  if (res1 == -1) return -1;
  // GC Flagging
  if (S_ISREG(buf.st_mode) && buf.st_nlink == 2) {
    struct fs_traversal_posix_context *pos_ctx
      =  reinterpret_cast<struct fs_traversal_posix_context*>(ctx->ctx);
    pos_ctx->gc_flagged[buf.st_ino] = true;
  }
  return 0;
}

int posix_do_rmdir(struct fs_traversal_context *ctx,
              const char *path) {
  std::string complete_path = BuildPath(ctx, path);
  std::string parent_path = GetParentPath(complete_path);
  struct stat parbuf;
  struct utimbuf mtimes;
  stat(parent_path.c_str(), &parbuf);
  mtimes.actime = parbuf.st_mtime;
  mtimes.modtime = parbuf.st_mtime;
  int res = rmdir(complete_path.c_str());
  res |= utime(parent_path.c_str(), &mtimes);
  if (res != 0) return -1;
  return 0;
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
  std::string parent_path = GetParentPath(complete_path);
  std::string hidden_datapath = BuildHiddenPath(ctx, identifier);
  const char *hidden_datapath_char = hidden_datapath.c_str();
  if (!FileExists(hidden_datapath)) {
    // Hash file doesn't exist
    errno = ENOENT;
    return -1;
  }
  struct stat parbuf;
  struct utimbuf mtimesParent;
  stat(parent_path.c_str(), &parbuf);
  mtimesParent.actime = parbuf.st_mtime;
  mtimesParent.modtime = parbuf.st_mtime;
  struct stat linkbuf;
  struct utimbuf mtimesLink;
  stat(hidden_datapath.c_str(), &linkbuf);
  mtimesLink.actime = linkbuf.st_mtime;
  mtimesLink.modtime = linkbuf.st_mtime;
  if (posix_cleanup_path(ctx, path) != 0) {
    return -1;
  }
    // GC Unflagging
  struct stat buf;
  int res2 = lstat(hidden_datapath_char, &buf);
  assert(res2 == 0);
  int res1 = link(hidden_datapath_char, complete_path.c_str());
  res1 |= utime(parent_path.c_str(), &mtimesParent);
  res1 |= utime(hidden_datapath.c_str(), &mtimesLink);
  if (res1 != 0) return -1;
  if (S_ISREG(buf.st_mode) && buf.st_nlink == 2) {
    struct fs_traversal_posix_context *pos_ctx
      =  reinterpret_cast<struct fs_traversal_posix_context*>(ctx->ctx);
    if (pos_ctx->gc_flagged.count(buf.st_ino) > 0) {
      pos_ctx->gc_flagged[buf.st_ino] = false;
    }
  }
  return 0;
}

int posix_do_mkdir(struct fs_traversal_context *ctx,
              const char *path,
              const struct cvmfs_attr *stat_info) {
  std::string complete_path = BuildPath(ctx, path);
  std::string parent_path = GetParentPath(complete_path);
  std::string dirname = GetParentPath(complete_path);
  struct stat parbuf;
  struct utimbuf mtimes;
  stat(parent_path.c_str(), &parbuf);
  mtimes.actime = parbuf.st_mtime;
  mtimes.modtime = parbuf.st_mtime;
  if (posix_cleanup_path(ctx, path) != 0) {
    return -1;
  }
  int res = mkdir(complete_path.c_str(), stat_info->st_mode);
  res |= utime(parent_path.c_str(), &mtimes);
  if (res != 0) return -1;
  return PosixSetMeta(complete_path.c_str(), stat_info);
}

int posix_do_symlink(struct fs_traversal_context *ctx,
              const char *src,
              const char *dest,
              const struct cvmfs_attr *stat_info) {
  std::string complete_src_path = BuildPath(ctx, src);
  std::string parent_path = GetParentPath(complete_src_path);
  std::string complete_dest_path = dest;
  struct stat parbuf;
  struct utimbuf mtimes;
  stat(parent_path.c_str(), &parbuf);
  mtimes.actime = parbuf.st_mtime;
  mtimes.modtime = parbuf.st_mtime;
  if (posix_cleanup_path(ctx, src) != 0) {
    return -1;
  }
  int res = symlink(complete_dest_path.c_str(), complete_src_path.c_str());
  res |= utime(parent_path.c_str(), &mtimes);
  if (res != 0) return -1;
  return PosixSetMeta(complete_src_path.c_str(), stat_info, false);
}

int posix_touch(struct fs_traversal_context *ctx,
              const struct cvmfs_attr *stat_info) {
  // NOTE(steuber): creat is only atomic on non-NFS paths!
  const char *identifier = posix_get_identifier(ctx, stat_info);
  if (posix_has_file(ctx, identifier)) {
    errno = EEXIST;
    return -1;
  }
  std::string hidden_datapath = BuildHiddenPath(ctx, identifier);
  delete identifier;
  int res1 = creat(hidden_datapath.c_str(), stat_info->st_mode);
  if (res1 < 0) return -1;
  int res2 = close(res1);
  if (res2 < 0) return -1;
  return PosixSetMeta(hidden_datapath.c_str(), stat_info);
}


struct posix_file_handle {
  std::string path;
  FILE *fd;
  struct utimbuf mtimes;
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
  struct stat statbuf;
  stat(handle->path.c_str(), &statbuf);
  handle->mtimes.actime = statbuf.st_mtime;
  handle->mtimes.modtime = statbuf.st_mtime;

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
  res |= utime(handle->path.c_str(), &(handle->mtimes));
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
    clearerr(handle->fd);
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

bool posix_is_hash_consistent(struct fs_traversal_context *ctx,
                const struct cvmfs_attr *stat_info) {
  errno = 0;
  std::string complete_path = BuildPath(ctx,
    (std::string(stat_info->cvm_parent)+"/"+stat_info->cvm_name).c_str());
  struct stat display_path_stat;
  int res1 = lstat(complete_path.c_str(), &display_path_stat);
  if (res1 == -1) {
    // If visible path doesn't exist => error
    return false;
  }
  const char *identifier = posix_get_identifier(ctx, stat_info);
  if (!posix_has_file(ctx, identifier)) {
    return false;
  }
  std::string hidden_datapath = BuildHiddenPath(ctx, identifier);
  struct stat hidden_path_stat;
  int res2 = stat(hidden_datapath.c_str(), &hidden_path_stat);
  if (res2 == -1) {
    // If hidden path doesn't exist although apprently existing => error
    return false;
  }
  delete identifier;
  return display_path_stat.st_ino == hidden_path_stat.st_ino;
}

int posix_garbage_collector(struct fs_traversal_context *ctx) {
  unsigned thread_total = get_nprocs()+1;
  pthread_t *workers
    = reinterpret_cast<pthread_t *>(smalloc(sizeof(pthread_t) * thread_total));
  struct posix_gc_thread *thread_contexts
    = reinterpret_cast<struct posix_gc_thread *>(
      smalloc(sizeof(struct posix_gc_thread) * thread_total));
  for (unsigned i = 0; i < thread_total; i++) {
    thread_contexts[i].thread_total = thread_total;
    thread_contexts[i].thread_num = i;
    thread_contexts[i].ctx = ctx;
    int retval = pthread_create(&workers[i], NULL,
      PosixGcMainWorker, &thread_contexts[i]);
    assert(retval == 0);
  }

  for (unsigned i = 0; i < thread_total; i++) {
    pthread_join(workers[i], NULL);
  }
  return -1;
}

struct fs_traversal_context *posix_initialize(
  const char *repo,
  const char *base,
  const char *data,
  const char *config) {
  fs_traversal_context *result = new struct fs_traversal_context;
  result->version = 1;

  char *def_base = NULL;
  if (!base) {
    def_base = strdup("/tmp/cvmfs/");
  } else {
    def_base = strdup(base);
  }

  size_t len = 2 + strlen(repo) + strlen(def_base);
  char *fqrn = reinterpret_cast<char *>(malloc(len*sizeof(char)));
  snprintf(fqrn, len, "%s/%s",  def_base, repo);
  result->repo = strdup(fqrn);
  free(fqrn);

  if (!data) {
    size_t len = 7 + strlen(def_base);
    char *def_data = reinterpret_cast<char *>(malloc(len*sizeof(char)));
    snprintf(def_data, len, "%s/.data",  def_base);
    result->data = strdup(def_data);
    free(def_data);
  } else {
    result->data = strdup(data);
  }
  free(def_base);

  if (!DirectoryExists(result->repo)) {
    if (!MkdirDeep(result->repo, 0744, true)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
        "Failed to create repository directory '%s'", result->repo);
      return NULL;
    }
  }
  PosixCheckDirStructure(result->data, 0744);  // NOTE(steuber): mode?
  const char *warning = WARNING_FILE_NAME;
  FILE *f = fopen(BuildPath(result, "/" WARNING_FILE_NAME).c_str(), "w");
  if (f != NULL) {
    fwrite(warning, sizeof(char), strlen(warning), f);
    fclose(f);
  }

  struct fs_traversal_posix_context *posix_ctx
    = new struct fs_traversal_posix_context;
  if (FileExists(std::string(result->data)
    + POSIX_GARBAGE_DIR + POSIX_GARBAGE_FLAGGED_FILE)) {
    FILE *gc_flagged_file = fopen((std::string(result->data)
      + POSIX_GARBAGE_DIR + POSIX_GARBAGE_FLAGGED_FILE).c_str(), "r");
    assert(gc_flagged_file != NULL);
    while (true) {
      ino_t cur_ino;
      size_t read = fread(&cur_ino, sizeof(ino_t), 1, gc_flagged_file);
      if (read == 1) {
        posix_ctx->gc_flagged[cur_ino] = true;
      } else {
        assert(feof(gc_flagged_file) != 0);
        break;
      }
    }
    int res = fclose(gc_flagged_file);
    assert(res == 0);
  }
  result->ctx = posix_ctx;
  return result;
}

void posix_finalize(struct fs_traversal_context *ctx) {
  struct fs_traversal_posix_context *posix_ctx
    =  reinterpret_cast<struct fs_traversal_posix_context*>(ctx->ctx);
  FILE *gc_flagged_file = fopen((std::string(ctx->data)
      + POSIX_GARBAGE_DIR + POSIX_GARBAGE_FLAGGED_FILE).c_str(), "w");
  for (
    std::map<ino_t, bool>::const_iterator it = posix_ctx->gc_flagged.begin();
    it != posix_ctx->gc_flagged.end();
    it++) {
    if (it->second) {
      fwrite(&(it->first), sizeof(ino_t), 1, gc_flagged_file);
    }
  }
  fclose(gc_flagged_file);
  delete ctx->repo;
  delete ctx->data;
  delete posix_ctx;
  delete ctx;
}



struct fs_traversal *posix_get_interface() {
  struct fs_traversal *result = new struct fs_traversal;
  result->initialize = posix_initialize;
  result->finalize = posix_finalize;
  result->list_dir = posix_list_dir;
  result->get_stat = posix_get_stat;
  result->is_hash_consistent = posix_is_hash_consistent;
  result->set_meta = posix_set_meta;
  result->has_file = posix_has_file;
  result->get_identifier = posix_get_identifier;
  result->do_link = posix_do_link;
  result->do_unlink = posix_do_unlink;
  result->do_mkdir = posix_do_mkdir;
  result->do_rmdir = posix_do_rmdir;
  result->touch = posix_touch;
  result->get_handle = posix_get_handle;
  result->do_symlink = posix_do_symlink;
  result->garbage_collector = posix_garbage_collector;

  result->do_fopen = posix_do_fopen;
  result->do_fclose = posix_do_fclose;
  result->do_fread = posix_do_fread;
  result->do_fwrite = posix_do_fwrite;
  result->do_ffree = posix_do_ffree;

  return result;
}
