/**
 * This file is part of the CernVM File System.
 */
#include "fs_traversal_libcvmfs.h"

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

void libcvmfs_list_dir(struct fs_traversal_context *ctx,
  const char *dir,
  char ***buf,
  size_t *len) {
  cvmfs_context *context = reinterpret_cast<cvmfs_context *>(ctx->ctx);
  size_t buf_len = 0;
  cvmfs_listdir_contents(context, dir, buf, len, &buf_len);
  return;
}

int libcvmfs_get_stat(struct fs_traversal_context *ctx,
  const char *path, struct cvmfs_attr *stat_result) {
  cvmfs_context *context = reinterpret_cast<cvmfs_context *>(ctx->ctx);
  return cvmfs_stat_attr(context, path, stat_result);
}

const char *libcvmfs_get_identifier(struct fs_traversal_context *ctx,
  const struct cvmfs_attr *stat) {
  const char *res = strdup(stat->cvm_name);
  return res;
}

bool libcvmfs_has_file(struct fs_traversal_context *ctx,
  const char *ident) {
  cvmfs_context *context = reinterpret_cast<cvmfs_context *>(ctx->ctx);
  struct cvmfs_attr *attr = cvmfs_attr_init();
  int retval = cvmfs_stat_attr(context, ident, attr);
  cvmfs_attr_free(attr);
  return retval;
}

struct libcvmfs_file_handle {
  char *path;
  int fd;
  cvmfs_context *ctx;
};

void *libcvmfs_get_handle(struct fs_traversal_context *ctx,
              const char *identifier) {
  struct libcvmfs_file_handle *file_ctx = new struct libcvmfs_file_handle;
  cvmfs_context *context = reinterpret_cast<cvmfs_context *>(ctx->ctx);
  file_ctx->ctx = context;
  file_ctx->path = strdup(identifier);

  return file_ctx;
}

int libcvmfs_do_fopen(void *file_ctx, fs_open_type op_mode) {
  struct libcvmfs_file_handle *handle =
    reinterpret_cast<libcvmfs_file_handle *>(file_ctx);
  handle->fd = cvmfs_open(handle->ctx, handle->path);
  if (handle->fd == -1) {
    return -1;
  }
  return 0;
}

int libcvmfs_do_fclose(void *file_ctx) {
  struct libcvmfs_file_handle *handle =
    reinterpret_cast<libcvmfs_file_handle *>(file_ctx);
  int res = cvmfs_close(handle->ctx, handle->fd);
  if (res != 0) return -1;
  handle->fd = -1;
  return 0;
}

int libcvmfs_do_fread(
  void *file_ctx,
  char *buff,
  size_t len,
  size_t *read_len
) {
  struct libcvmfs_file_handle *handle =
    reinterpret_cast<libcvmfs_file_handle *>(file_ctx);
  *read_len = cvmfs_pread(handle->ctx, handle->fd, buff, len, handle->fd);
  return 0;
}

int libcvmfs_do_fwrite(void *file_ctx, const char *buff, size_t len) {
  return -1;
}

void libcvmfs_do_ffree(void *file_ctx) {
  struct libcvmfs_file_handle *handle =
    reinterpret_cast<libcvmfs_file_handle *>(file_ctx);
  if (handle->fd > 0) {
    libcvmfs_do_fclose(file_ctx);
  }
  delete handle;
}


struct fs_traversal_context *libcvmfs_initialize(
  const char *repo,
  const char *data) {
  struct fs_traversal_context *result = new struct fs_traversal_context;
  result->version = 1;
  result->size = sizeof(*result);
  result->repo = strdup(repo);
  result->data = NULL;
  if (data) {
    result->data = strdup(data);
  }
  /*
  cvmfs_option_map *opts = cvmfs_options_init();
  cvmfs_init_v2(opts);
  cvmfs_context *ctx;
  cvmfs_attach_repo_v2(repo, opts, &ctx);
  result->ctx = ctx;
  */
  return result;
}

void libcvmfs_finalize(struct fs_traversal_context *ctx) {
  cvmfs_context *context = reinterpret_cast<cvmfs_context *>(ctx->ctx);
  cvmfs_detach_repo(context);
  cvmfs_fini();
  delete ctx;
}

struct fs_traversal *libcvmfs_get_interface() {
  struct fs_traversal *result = new struct fs_traversal;
  result->initialize = libcvmfs_initialize;
  result->finalize = libcvmfs_finalize;
  result->list_dir = libcvmfs_list_dir;
  result->get_stat = libcvmfs_get_stat;
  result->has_file = libcvmfs_has_file;
  result->get_identifier = libcvmfs_get_identifier;
  result->get_handle = libcvmfs_get_handle;

  result->do_fopen = libcvmfs_do_fopen;
  result->do_fclose = libcvmfs_do_fclose;
  result->do_fread = libcvmfs_do_fread;
  result->do_fwrite = libcvmfs_do_fwrite;
  result->do_ffree = libcvmfs_do_ffree;

  return result;
}
