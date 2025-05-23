/**
 * This file is part of the CernVM File System.
 */
#include "interface.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <utime.h>

#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "crypto/hash.h"
#include "garbage_collector.h"
#include "helpers.h"
#include "libcvmfs.h"
#include "shrinkwrap/fs_traversal_interface.h"
#include "shrinkwrap/util.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "util/string.h"
#include "xattr.h"

struct posix_file_handle {
  std::string path;
  FILE *fd;
  struct utimbuf mtimes;
  int original_mode;
};

/*
 * BASIC FS OPERATIONS
 */
void posix_list_dir(struct fs_traversal_context *ctx, const char *dir,
                    char ***buf, size_t *len);

int posix_get_stat(struct fs_traversal_context *ctx, const char *path,
                   struct cvmfs_attr *stat_result, bool get_hash);

int posix_set_meta(struct fs_traversal_context *ctx, const char *path,
                   const struct cvmfs_attr *stat_info);

char *posix_get_identifier(struct fs_traversal_context *ctx,
                           const struct cvmfs_attr *stat);

bool posix_has_file(struct fs_traversal_context *ctx, const char *ident);

int posix_do_unlink(struct fs_traversal_context *ctx, const char *path);

int posix_do_rmdir(struct fs_traversal_context *ctx, const char *path);

int posix_do_link(struct fs_traversal_context *ctx, const char *path,
                  const char *identifier);

int posix_do_mkdir(struct fs_traversal_context *ctx, const char *path,
                   const struct cvmfs_attr *stat_info);

int posix_do_symlink(struct fs_traversal_context *ctx, const char *src,
                     const char *dest, const struct cvmfs_attr *stat_info);

int posix_touch(struct fs_traversal_context *ctx,
                const struct cvmfs_attr *stat_info);

bool posix_is_hash_consistent(struct fs_traversal_context *ctx,
                              const struct cvmfs_attr *stat_info);

/*
 * FILE OPERATIONS
 */

void *posix_get_handle(struct fs_traversal_context *ctx,
                       const char *identifier);

int posix_do_fopen(void *file_ctx, fs_open_type op_mode);

int posix_do_fclose(void *file_ctx);

int posix_do_fread(void *file_ctx, char *buff, size_t len, size_t *read_len);

int posix_do_fwrite(void *file_ctx, const char *buff, size_t len);

void posix_do_ffree(void *file_ctx);

/*
 * GARBAGE COLLECTION
 */

int posix_garbage_collector(struct fs_traversal_context *ctx);


/*
 * ARCHIVE PROVENANCE INFORMATION
 */
bool posix_archive_config(std::string config_name, std::string prov_name);

void posix_write_provenance_info(struct fs_traversal_context *ctx,
                                 const char *info_file);

void posix_archive_provenance(struct fs_traversal_context *src,
                              struct fs_traversal_context *dest);

/*
 * INITIALIZATION
 */

struct fs_traversal_context *posix_initialize(const char *repo,
                                              const char *base,
                                              const char *data,
                                              const char *config,
                                              int num_threads);

void posix_finalize(struct fs_traversal_context *ctx);

struct fs_traversal *posix_get_interface() {
  struct fs_traversal *result = new struct fs_traversal;
  result->initialize = posix_initialize;
  result->finalize = posix_finalize;
  result->archive_provenance = posix_archive_provenance;
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

// Utility function
int posix_cleanup_path(struct fs_traversal_context *ctx, const char *path) {
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
    if (res != 0)
      return -1;
  }
  if (DirectoryExists(complete_path)) {
    struct dirent *de;
    DIR *dr = opendir(complete_path.c_str());
    while ((de = readdir(dr)) != NULL) {
      if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0) {
        std::string cur_path = std::string(path) + "/" + de->d_name;
        if (posix_cleanup_path(ctx, cur_path.c_str()) != 0) {
          return -1;
        }
      }
    }
    int res = closedir(dr);
    res |= posix_do_rmdir(ctx, path);
    if (res != 0)
      return -1;
  }
  return 0;
}

/**
 * INTERFACE FUNCTION IMPLEMENTATIONS
 */

void posix_list_dir(struct fs_traversal_context *ctx,
                    const char *dir,
                    char ***buf,
                    size_t *len) {
  struct dirent *de;
  *len = 0;
  size_t buflen = 5;
  *buf = reinterpret_cast<char **>(smalloc(sizeof(char *) * buflen));

  DIR *dr = opendir(BuildPath(ctx, dir).c_str());

  // NULL terminate the list;
  AppendStringToList(NULL, buf, len, &buflen);

  if (dr == NULL) {
    return;
  }

  while ((de = readdir(dr)) != NULL) {
    if (strcmp(de->d_name, ".") != 0 && strcmp(de->d_name, "..") != 0
        && strcmp(de->d_name, WARNING_FILE_NAME) != 0) {
      AppendStringToList(de->d_name, buf, len, &buflen);
    }
  }

  closedir(dr);
  return;
}

int posix_get_stat(struct fs_traversal_context *ctx,
                   const char *path,
                   struct cvmfs_attr *stat_result,
                   bool get_hash) {
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
  stat_result->st_uid = buf.st_uid;
  stat_result->st_gid = buf.st_gid;
  stat_result->st_rdev = buf.st_rdev;
  stat_result->st_size = buf.st_size;
  stat_result->mtime = buf.st_mtime;

  if (get_hash && S_ISREG(buf.st_mode)) {
    // We cannot reliably figure out the hash
    // because we don't know the hash algorithm
    return -1;
  } else {
    // We usually do not calculate the checksum for posix files since it's a
    // destination file system.
    stat_result->cvm_checksum = NULL;
  }

  if (S_ISLNK(buf.st_mode)) {
    char slnk[PATH_MAX + 1];
    const ssize_t length = readlink(complete_path.c_str(), slnk, PATH_MAX);
    if (length < 0) {
      return -1;
    }
    slnk[length] = '\0';
    stat_result->cvm_symlink = strdup(slnk);
  } else {
    stat_result->cvm_symlink = NULL;
  }
  std::string parent_path = GetParentPath(path);
  std::string file_name = GetFileName(path);
  stat_result->cvm_parent = strdup(parent_path.c_str());
  stat_result->cvm_name = strdup(file_name.c_str());

  stat_result->cvm_xattrs = XattrList::CreateFromFile(complete_path);
  if (stat_result->cvm_xattrs == NULL)
    return -1;

  return 0;
}

int posix_set_meta(struct fs_traversal_context *ctx, const char *path,
                   const struct cvmfs_attr *stat_info) {
  std::string complete_path = BuildPath(ctx, path);
  return PosixSetMeta(complete_path.c_str(), stat_info,
                      !S_ISLNK(stat_info->st_mode));
}

char *posix_get_identifier(struct fs_traversal_context *ctx,
                           const struct cvmfs_attr *stat) {
  shash::Any content_hash = shash::MkFromHexPtr(
      shash::HexPtr(stat->cvm_checksum));
  shash::Any meta_hash = HashMeta(stat);
  std::string path = ("/"
                      + content_hash.MakePathExplicit(kDirLevels,
                                                      kDigitsPerDirLevel, '.')
                      + meta_hash.ToString());
  char *res = strdup(path.c_str());
  return res;
}

bool posix_has_file(struct fs_traversal_context *ctx, const char *ident) {
  return FileExists(BuildHiddenPath(ctx, ident));
}

int posix_do_unlink(struct fs_traversal_context *ctx, const char *path) {
  std::string complete_path = BuildPath(ctx, path);
  std::string parent_path = GetParentPath(complete_path);
  const char *complete_path_char = complete_path.c_str();
  struct stat buf;
  int res2 = lstat(complete_path_char, &buf);
  if (res2 == -1 && errno == ENOENT)
    return -1;
  assert(res2 == 0);
  struct utimbuf mtimes;
  if (!BackupMtimes(parent_path, &mtimes))
    return -1;
  // Unlinking
  int res1 = unlink(complete_path_char);
  res1 |= utime(parent_path.c_str(), &mtimes);
  if (res1 == -1)
    return -1;
  // GC Flagging
  if (S_ISREG(buf.st_mode) && buf.st_nlink == 2) {
    struct fs_traversal_posix_context
        *pos_ctx = reinterpret_cast<struct fs_traversal_posix_context *>(
            ctx->ctx);
    pos_ctx->gc_flagged[buf.st_ino] = true;
  }
  return 0;
}

int posix_do_rmdir(struct fs_traversal_context *ctx, const char *path) {
  std::string complete_path = BuildPath(ctx, path);
  std::string parent_path = GetParentPath(complete_path);
  struct utimbuf mtimes;
  if (!BackupMtimes(parent_path, &mtimes))
    return -1;
  int res = rmdir(complete_path.c_str());
  res |= utime(parent_path.c_str(), &mtimes);
  if (res != 0)
    return -1;
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
  struct utimbuf mtimes_parent;
  if (!BackupMtimes(parent_path, &mtimes_parent))
    return -1;

  struct utimbuf mtimes_link;
  if (!BackupMtimes(hidden_datapath, &mtimes_link))
    return -1;

  if (posix_cleanup_path(ctx, path) != 0) {
    return -1;
  }
  // GC Unflagging
  struct stat buf;
  int res2 = lstat(hidden_datapath_char, &buf);
  assert(res2 == 0);
  int res1 = link(hidden_datapath_char, complete_path.c_str());
  res1 |= utime(parent_path.c_str(), &mtimes_parent);
  res1 |= utime(hidden_datapath.c_str(), &mtimes_link);
  if (res1 != 0) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Failed to create link : %s->%s : %d : %s\n", hidden_datapath_char,
             complete_path.c_str(), errno, strerror(errno));
    return -1;
  }
  if (S_ISREG(buf.st_mode) && buf.st_nlink == 2) {
    struct fs_traversal_posix_context
        *pos_ctx = reinterpret_cast<struct fs_traversal_posix_context *>(
            ctx->ctx);
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
  struct utimbuf mtimes;
  if (!BackupMtimes(parent_path, &mtimes))
    return -1;
  if (posix_cleanup_path(ctx, path) != 0) {
    return -1;
  }
  int res = mkdir(complete_path.c_str(), stat_info->st_mode);
  res |= utime(parent_path.c_str(), &mtimes);
  if (res != 0)
    return -1;
  return PosixSetMeta(complete_path.c_str(), stat_info);
}

int posix_do_symlink(struct fs_traversal_context *ctx,
                     const char *src,
                     const char *dest,
                     const struct cvmfs_attr *stat_info) {
  std::string complete_src_path = BuildPath(ctx, src);
  std::string parent_path = GetParentPath(complete_src_path);
  std::string complete_dest_path = dest;
  struct utimbuf mtimes;
  if (!BackupMtimes(parent_path, &mtimes))
    return -1;
  if (posix_cleanup_path(ctx, src) != 0) {
    return -1;
  }
  int res = symlink(complete_dest_path.c_str(), complete_src_path.c_str());
  res |= utime(parent_path.c_str(), &mtimes);
  if (res != 0)
    return -1;
  return PosixSetMeta(complete_src_path.c_str(), stat_info, false);
}

int posix_touch(struct fs_traversal_context *ctx,
                const struct cvmfs_attr *stat_info) {
  // NOTE(steuber): creat is only atomic on non-NFS paths!
  char *identifier = posix_get_identifier(ctx, stat_info);
  if (posix_has_file(ctx, identifier)) {
    free(identifier);
    errno = EEXIST;
    return -1;
  }
  std::string hidden_datapath = BuildHiddenPath(ctx, identifier);
  free(identifier);
  int res1 = creat(hidden_datapath.c_str(), stat_info->st_mode);
  if (res1 < 0)
    return -1;
  int res2 = close(res1);
  if (res2 < 0)
    return -1;
  int res3 = PosixSetMeta(hidden_datapath.c_str(), stat_info);
  if (res3 < 0)
    return -1;
  return 0;
}

bool posix_is_hash_consistent(struct fs_traversal_context *ctx,
                              const struct cvmfs_attr *stat_info) {
  errno = 0;
  std::string complete_path = BuildPath(
      ctx,
      (std::string(stat_info->cvm_parent) + "/" + stat_info->cvm_name).c_str());
  struct stat display_path_stat;
  int res1 = lstat(complete_path.c_str(), &display_path_stat);
  if (res1 == -1) {
    // If visible path doesn't exist => error
    return false;
  }
  char *identifier = posix_get_identifier(ctx, stat_info);
  if (!posix_has_file(ctx, identifier)) {
    free(identifier);
    return false;
  }
  std::string hidden_datapath = BuildHiddenPath(ctx, identifier);
  free(identifier);
  struct stat hidden_path_stat;
  int res2 = stat(hidden_datapath.c_str(), &hidden_path_stat);
  if (res2 == -1) {
    // If hidden path doesn't exist although apparently existing => error
    return false;
  }
  return display_path_stat.st_ino == hidden_path_stat.st_ino;
}

void *posix_get_handle(struct fs_traversal_context *ctx,
                       const char *identifier) {
  struct posix_file_handle *file_ctx = new struct posix_file_handle;
  file_ctx->path = BuildHiddenPath(ctx, identifier);

  return file_ctx;
}

bool EnableWriteAccess(posix_file_handle *handle) {
  struct stat info;
  int retval = stat(handle->path.c_str(), &info);
  if (retval != 0)
    return false;
  handle->original_mode = info.st_mode;
  retval = chmod(handle->path.c_str(), info.st_mode | S_IWUSR | S_IWUSR);
  return retval == 0;
}

int posix_do_fopen(void *file_ctx, fs_open_type op_mode) {
  struct posix_file_handle *handle = reinterpret_cast<posix_file_handle *>(
      file_ctx);
  const char *mode = "r";
  if (op_mode == fs_open_write) {
    mode = "w";
  } else if (op_mode == fs_open_append) {
    mode = "a";
  }
  if (!BackupMtimes(handle->path, &(handle->mtimes)))
    return -1;
  handle->original_mode = 0;

  FILE *fd = fopen(handle->path.c_str(), mode);
  if (fd == NULL) {
    if (errno == EACCES) {
      EnableWriteAccess(handle);
      fd = fopen(handle->path.c_str(), mode);
      if (fd == NULL)
        return -1;
    } else {
      return -1;
    }
  }
  handle->fd = fd;
  return 0;
}

int posix_do_fclose(void *file_ctx) {
  struct posix_file_handle *handle = reinterpret_cast<posix_file_handle *>(
      file_ctx);
  int res = 0;
  if (handle->original_mode != 0) {
    res = fchmod(fileno(handle->fd), handle->original_mode);
  }
  res |= fclose(handle->fd);
  handle->fd = NULL;
  if (res != 0) {
    // Opportunistic approach to reset time stamp
    utime(handle->path.c_str(), &(handle->mtimes));
    return -1;
  }
  res = utime(handle->path.c_str(), &(handle->mtimes));
  if (res != 0)
    return -1;
  return 0;
}

int posix_do_fread(void *file_ctx, char *buff, size_t len, size_t *read_len) {
  struct posix_file_handle *handle = reinterpret_cast<posix_file_handle *>(
      file_ctx);
  *read_len = fread(buff, sizeof(char), len, handle->fd);
  if (*read_len < len && ferror(handle->fd) != 0) {
    clearerr(handle->fd);
    return -1;
  }
  return 0;
}

int posix_do_fwrite(void *file_ctx, const char *buff, size_t len) {
  struct posix_file_handle *handle = reinterpret_cast<posix_file_handle *>(
      file_ctx);
  size_t written_len = fwrite(buff, sizeof(char), len, handle->fd);
  if (written_len != len) {
    clearerr(handle->fd);
    return -1;
  }
  return 0;
}

void posix_do_ffree(void *file_ctx) {
  struct posix_file_handle *handle = reinterpret_cast<posix_file_handle *>(
      file_ctx);
  if (handle->fd != NULL) {
    posix_do_fclose(file_ctx);
  }
  delete handle;
}

int posix_garbage_collector(struct fs_traversal_context *ctx) {
  return RunGarbageCollection(ctx);
}

bool posix_archive_config(std::string config_name, std::string prov_name) {
  FILE *prov = fopen(prov_name.c_str(), "w");
  if (prov == NULL) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Archive config: failed to open : %s : %d : %s\n",
             prov_name.c_str(), errno, strerror(errno));
    return false;
  }

  std::vector<std::string> config_files = SplitString(config_name, ':');
  for (unsigned i = 0; i < config_files.size(); ++i) {
    FILE *config = fopen(config_files[i].c_str(), "r");
    if (config == NULL) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Archive config: failed to open : %s : %d : %s\n",
               config_name.c_str(), errno, strerror(errno));
      fclose(prov);
      return false;
    }

    while (1) {
      char buffer[COPY_BUFFER_SIZE];

      size_t nbytes = 0;
      nbytes = fread(&buffer, sizeof(char), sizeof(buffer), config);
      if (nbytes < sizeof(buffer) && ferror(config) != 0) {
        LogCvmfs(kLogCvmfs, kLogStderr, "Archive config: read failed : %d %s\n",
                 errno, strerror(errno));
        fclose(config);
        fclose(prov);
        return false;
      }

      size_t written_len = fwrite(buffer, sizeof(char), nbytes, prov);
      if (written_len != nbytes) {
        LogCvmfs(kLogCvmfs, kLogStderr,
                 "Archive config: write failed : %d %s\n", errno,
                 strerror(errno));
        fclose(config);
        fclose(prov);
        return false;
      }

      if (nbytes < COPY_BUFFER_SIZE)
        break;
    }
  }

  return true;
}

void posix_write_provenance_info(struct fs_traversal_context *ctx,
                                 const char *info_file) {
  FILE *f = fopen(info_file, "w");
  if (f != NULL) {
    fwrite("repo : ", sizeof(char), 7, f);
    fwrite(ctx->repo, sizeof(char), strlen(ctx->repo), f);
    fwrite("\nversion : ", sizeof(char), 11, f);
    fwrite(ctx->lib_version, sizeof(char), strlen(ctx->lib_version), f);
    fwrite("\nbase : ", sizeof(char), 8, f);
    if (ctx->base)
      fwrite(ctx->base, sizeof(char), strlen(ctx->base), f);
    fwrite("\ncache : ", sizeof(char), 9, f);
    if (ctx->data)
      fwrite(ctx->data, sizeof(char), strlen(ctx->data), f);
    fwrite("\nconfig : ", sizeof(char), 10, f);
    if (ctx->config)
      fwrite(ctx->config, sizeof(char), strlen(ctx->config), f);
    fclose(f);
  }
}

void posix_archive_provenance(struct fs_traversal_context *src,
                              struct fs_traversal_context *dest) {
  std::string prov_dir;
  prov_dir.append(dest->base);
  prov_dir.append(".provenance/");
  prov_dir.append(dest->repo);
  prov_dir.append("/");

  if (!DirectoryExists(prov_dir.c_str())) {
    if (!MkdirDeep(prov_dir.c_str(), 0755, true)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Failed to create repository directory '%s'", prov_dir.c_str());
    }
  }

  std::string src_info = prov_dir;
  src_info.append("src.info");
  posix_write_provenance_info(src, src_info.c_str());

  if (src->config) {
    std::string src_config = prov_dir + "src.config";
    if (!posix_archive_config(src->config, src_config)) {
      LogCvmfs(kLogCvmfs, kLogStderr, "Failed to archive source config '%s'",
               src->config);
    }
  }

  std::string dest_info = prov_dir;
  dest_info.append("dest.info");
  posix_write_provenance_info(dest, dest_info.c_str());

  if (dest->config) {
    std::string dest_config = prov_dir + "dest.config";
    if (!posix_archive_config(dest->config, dest_config)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Failed to archive destination config '%s'", dest->config);
    }
  }
}

struct fs_traversal_context *posix_initialize(const char *repo,
                                              const char *base,
                                              const char *data,
                                              const char *config,
                                              int num_threads) {
  fs_traversal_context *result = new struct fs_traversal_context;
  result->version = 1;
  result->lib_version = strdup("1.0");
  struct fs_traversal_posix_context
      *posix_ctx = new struct fs_traversal_posix_context;
  posix_ctx->num_threads = num_threads;
  result->ctx = posix_ctx;

  // Retrieve base directory for traversal
  if (!base) {
    result->base = strdup("/tmp/cvmfs/");
  } else {
    if (base[strlen(base) - 1] != '/') {
      size_t len = 2 + strlen(base);
      char *base_dir = reinterpret_cast<char *>(smalloc(len * sizeof(char)));
      snprintf(base_dir, len, "%s/", base);
      result->base = strdup(base_dir);
      free(base_dir);
    } else {
      result->base = strdup(base);
    }
  }

  // Retrieve repository (inside base directory) for traversal
  if (!repo) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Repository name must be specified");
    return NULL;
  }
  result->repo = strdup(repo);

  // Retrieve data directory (for hidden dedup directory)
  if (!data) {
    size_t len = 6 + strlen(result->base);
    char *def_data = reinterpret_cast<char *>(smalloc(len * sizeof(char)));
    snprintf(def_data, len, "%s.data", result->base);
    result->data = strdup(def_data);
    free(def_data);
  } else {
    result->data = strdup(data);
  }

  if (config && strlen(config) > 0) {
    LogCvmfs(kLogCvmfs, kLogStderr,
             "Configuration file is not supported in POSIX interface '%s'",
             config);
    return NULL;
  }
  result->config = NULL;

  // Build directory if not there yet
  std::string req_dirs = BuildPath(result, "");
  if (!DirectoryExists(req_dirs.c_str())) {
    if (!MkdirDeep(req_dirs.c_str(), 0755, true)) {
      LogCvmfs(kLogCvmfs, kLogStderr,
               "Failed to create repository directory '%s'", req_dirs.c_str());
      return NULL;
    }
  }

  // Initializes Data Directory, Garbage Collection and Warning file
  InitialFsOperations(result);
  return result;
}

void posix_finalize(struct fs_traversal_context *ctx) {
  FinalizeFsOperations(ctx);
  free(ctx->repo);
  free(ctx->base);
  free(ctx->data);
  free(ctx->config);
  free(ctx->lib_version);
  struct fs_traversal_posix_context
      *posix_ctx = reinterpret_cast<struct fs_traversal_posix_context *>(
          ctx->ctx);
  delete posix_ctx;
  delete ctx;
}
