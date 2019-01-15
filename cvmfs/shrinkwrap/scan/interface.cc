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

#include <map>
#include <set>
#include <string>

#include "hash.h"
#include "libcvmfs.h"
#include "logging.h"
#include "shrinkwrap/fs_traversal_interface.h"
#include "shrinkwrap/util.h"
#include "smalloc.h"
#include "string.h"
#include "xattr.h"

const char kDirLevels = 2;
const char kDigitsPerDirLevel = 2;

struct scan_file_handle {
  std::string path;
};


/*
 * BASIC FS OPERATIONS
 */
void scan_list_dir(struct fs_traversal_context *ctx, const char *dir,
  char ***buf, size_t *len);

int scan_get_stat(struct fs_traversal_context *ctx,
  const char *path, struct cvmfs_attr *stat_result, bool get_hash);

int scan_set_meta(struct fs_traversal_context *ctx,
  const char *path, const struct cvmfs_attr *stat_info);

char *scan_get_identifier(struct fs_traversal_context *ctx,
  const struct cvmfs_attr *stat);

bool scan_has_file(struct fs_traversal_context *ctx,
  const char *ident);

int scan_do_unlink(struct fs_traversal_context *ctx,
  const char *path);

int scan_do_rmdir(struct fs_traversal_context *ctx,
  const char *path);

int scan_do_link(struct fs_traversal_context *ctx,
  const char *path, const char *identifier);

int scan_do_mkdir(struct fs_traversal_context *ctx, const char *path,
  const struct cvmfs_attr *stat_info);

int scan_do_symlink(struct fs_traversal_context *ctx, const char *src,
  const char *dest, const struct cvmfs_attr *stat_info);

int scan_touch(struct fs_traversal_context *ctx,
  const struct cvmfs_attr *stat_info);

bool scan_is_hash_consistent(struct fs_traversal_context *ctx,
  const struct cvmfs_attr *stat_info);

/*
 * FILE OPERATIONS
 */

void *scan_get_handle(struct fs_traversal_context *ctx,
  const char *identifier);

int scan_do_fopen(void *file_ctx, fs_open_type op_mode);

int scan_do_fclose(void *file_ctx);

int scan_do_fread(void *file_ctx, char *buff, size_t len, size_t *read_len);

int scan_do_fwrite(void *file_ctx, const char *buff, size_t len);

void scan_do_ffree(void *file_ctx);


/*
 * ARCHIVE PROVENANCE INFORMATION
 */
bool scan_archive_config(
  std::string config_name,
  std::string prov_name
);

void scan_write_provenance_info(
  struct fs_traversal_context *ctx,
  const char *info_file
);

void scan_archive_provenance(
  struct fs_traversal_context *src,
  struct fs_traversal_context *dest);

/*
 * INITIALIZATION
 */

struct fs_traversal_context *scan_initialize(
  const char *repo,
  const char *base,
  const char *data,
  const char *config,
  int num_threads);

void scan_finalize(struct fs_traversal_context *ctx);

struct fs_traversal *scan_get_interface() {
  struct fs_traversal *result = new struct fs_traversal;
  result->initialize = scan_initialize;
  result->finalize = scan_finalize;
  result->archive_provenance = scan_archive_provenance;
  result->list_dir = scan_list_dir;
  result->get_stat = scan_get_stat;
  result->is_hash_consistent = scan_is_hash_consistent;
  result->set_meta = scan_set_meta;
  result->has_file = scan_has_file;
  result->get_identifier = scan_get_identifier;
  result->do_link = scan_do_link;
  result->do_unlink = scan_do_unlink;
  result->do_mkdir = scan_do_mkdir;
  result->do_rmdir = scan_do_rmdir;
  result->touch = scan_touch;
  result->get_handle = scan_get_handle;
  result->do_symlink = scan_do_symlink;
  result->do_fopen = scan_do_fopen;
  result->do_fclose = scan_do_fclose;
  result->do_fread = scan_do_fread;
  result->do_fwrite = scan_do_fwrite;
  result->do_ffree = scan_do_ffree;

  return result;
}

/**
 * INTERFACE FUNCTION IMPLEMENTATIONS
 */

void scan_list_dir(struct fs_traversal_context *ctx,
  const char *dir,
  char ***buf,
  size_t *len) {
  return;
}

int scan_get_stat(struct fs_traversal_context *ctx,
  const char *path, struct cvmfs_attr *stat_result, bool get_hash) {
  return 0;
}

int scan_set_meta(struct fs_traversal_context *ctx,
  const char *path, const struct cvmfs_attr *stat_info) {
  return 0;
}

char *scan_get_identifier(struct fs_traversal_context *ctx,
  const struct cvmfs_attr *stat) {
  shash::Any content_hash =
    shash::MkFromHexPtr(shash::HexPtr(stat->cvm_checksum));
  shash::Any meta_hash = HashMeta(stat);
  std::string path = (
    content_hash.MakePathExplicit(kDirLevels, kDigitsPerDirLevel, '.')
    + meta_hash.ToString());
  char *res = strdup(path.c_str());
  return res;
}

bool scan_has_file(struct fs_traversal_context *ctx,
  const char *ident) {
  struct fs_traversal_scan_context *scan_ctx
    =  reinterpret_cast<struct fs_traversal_scan_context*>(ctx->ctx);
  std::string identifier = ident;
  return (scan_ctx->data_dir.find(identifier) != scan_ctx->data_dir.end());
}

int scan_do_unlink(struct fs_traversal_context *ctx,
  const char *path) {
  return 0;
}

int scan_do_rmdir(struct fs_traversal_context *ctx,
  const char *path) {
  return 0;
}

int scan_do_link(struct fs_traversal_context *ctx,
  const char *path,
  const char *identifier) {
  return 0;
}

int scan_do_mkdir(struct fs_traversal_context *ctx,
  const char *path,
  const struct cvmfs_attr *stat_info) {
  return 0;
}

int scan_do_symlink(struct fs_traversal_context *ctx,
  const char *src,
  const char *dest,
  const struct cvmfs_attr *stat_info) {
  return 0;
}

int scan_touch(struct fs_traversal_context *ctx,
  const struct cvmfs_attr *stat_info) {
  // NOTE(steuber): creat is only atomic on non-NFS paths!
  char *identifier = scan_get_identifier(ctx, stat_info);

  std::string ident = identifier;
  if (scan_has_file(ctx, identifier)) {
    free(identifier);
    errno = EEXIST;
    return -1;
  }
  struct fs_traversal_scan_context *scan_ctx
    =  reinterpret_cast<struct fs_traversal_scan_context*>(ctx->ctx);
  scan_ctx->data_dir.insert(scan_ctx->data_dir.end(), ident);
  free(identifier);
  return 0;
}

bool scan_is_hash_consistent(struct fs_traversal_context *ctx,
  const struct cvmfs_attr *stat_info) {
  return false;
}

void *scan_get_handle(struct fs_traversal_context *ctx,
  const char *identifier) {
  struct scan_file_handle *file_ctx = new struct scan_file_handle;
  file_ctx->path = identifier;

  return file_ctx;
}

int scan_do_fopen(void *file_ctx, fs_open_type op_mode) {
  return 0;
}

int scan_do_fclose(void *file_ctx) {
  return 0;
}

int scan_do_fread(void *file_ctx, char *buff, size_t len, size_t *read_len) {
  return 0;
}

int scan_do_fwrite(void *file_ctx, const char *buff, size_t len) {
  return 0;
}

void scan_do_ffree(void *file_ctx) {
  struct scan_file_handle *handle =
    reinterpret_cast<scan_file_handle *>(file_ctx);
  delete handle;
}

bool scan_archive_config(
  std::string config_name,
  std::string prov_name
) {
  return true;
}

void scan_write_provenance_info(
  struct fs_traversal_context *ctx,
  const char *info_file
) {
  return;
}

void scan_archive_provenance(
  struct fs_traversal_context *src,
  struct fs_traversal_context *dest)
{
  return;
}

struct fs_traversal_context *scan_initialize(
  const char *repo,
  const char *base,
  const char *data,
  const char *config,
  int num_threads) {
  fs_traversal_context *result = new struct fs_traversal_context;
  result->version = 1;
  result->lib_version = strdup("1.0");
  result->type = strdup("scan");
  struct fs_traversal_scan_context *scan_ctx
    = new struct fs_traversal_scan_context;
  result->ctx = scan_ctx;

  LogCvmfs(kLogCvmfs, kLogStdout,
      "Initializing scan interface with its directory structure");

  // Retrieve base directory for traversal
  if (!base) {
    result->base = strdup("/tmp/cvmfs/");
  } else {
    if  (base[strlen(base)-1] != '/') {
      size_t len = 2 + strlen(base);
      char *base_dir = reinterpret_cast<char *>(smalloc(len*sizeof(char)));
      snprintf(base_dir, len, "%s/",  base);
      result->base = strdup(base_dir);
      free(base_dir);
    } else {
      result->base = strdup(base);
    }
  }

  // Retrieve repository (inside base directory) for traversal
  if (!repo) {
    LogCvmfs(kLogCvmfs, kLogStderr,
      "Repository name must be specified");
    return NULL;
  }
  result->repo = strdup(repo);

  // Retrieve data directory (for hidden dedup directory)
  if (!data) {
    size_t len = 6 + strlen(result->base);
    char *def_data = reinterpret_cast<char *>(smalloc(len*sizeof(char)));
    snprintf(def_data, len, "%s.data",  result->base);
    result->data = strdup(def_data);
    free(def_data);
  } else {
    result->data = strdup(data);
  }

  result->config = NULL;

  return result;
}

void scan_finalize(struct fs_traversal_context *ctx) {
  free(ctx->repo);
  free(ctx->base);
  free(ctx->data);
  free(ctx->config);
  free(ctx->lib_version);
  struct fs_traversal_scan_context *scan_ctx
    =  reinterpret_cast<struct fs_traversal_scan_context*>(ctx->ctx);
  delete scan_ctx;
  delete ctx;
}
