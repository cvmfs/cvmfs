/**
 * This file is part of the CernVM File System.
 */

#include <stdlib.h>

#include "fs_traversal_interface.h"
#include "fs_traversal_libcvmfs.h"
#include "libcvmfs.h"
#include "logging.h"
#include "string.h"

void libcvmfs_list_dir(struct fs_traversal_context *ctx,
  const char *dir,
  char ***buf,
  size_t *len) {
  cvmfs_context *context = reinterpret_cast<cvmfs_context *>(ctx->ctx);
  size_t buf_len = 0;
  struct cvmfs_nc_attr *nc_attr = cvmfs_nc_attr_init();
  cvmfs_stat_nc(context, dir, nc_attr);
  cvmfs_nc_attr_free(nc_attr);
  cvmfs_listdir_contents(context, dir, buf, len, &buf_len);
  return;
}

int libcvmfs_get_stat(struct fs_traversal_context *ctx,
  const char *path, struct cvmfs_attr *stat_result, bool get_hash) {
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
  off_t off;
  cvmfs_context *ctx;
};

void *libcvmfs_get_handle(struct fs_traversal_context *ctx,
              const char *identifier) {
  struct libcvmfs_file_handle *file_ctx =
    reinterpret_cast<libcvmfs_file_handle *>(calloc(1, sizeof(*file_ctx)));
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
  ssize_t read = cvmfs_pread(handle->ctx, handle->fd, buff, len, handle->off);
  if (read == -1)  {
    return -1;
  }
  *read_len = read;
  handle->off += *read_len;
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
  const char *base,
  const char *data,
  const char *config) {
  struct fs_traversal_context *result = new struct fs_traversal_context;
  result->version = 1;
  result->size = sizeof(*result);
  result->repo = strdup(repo);
  result->data = NULL;
  if (data) {
    result->data = strdup(data);
  }

  cvmfs_option_map *options_mgr = cvmfs_options_init();
  int retval = cvmfs_options_parse(options_mgr, config);
  if (retval) {
    LogCvmfs(kLogCvmfs, kLogStderr,
    "CVMFS Options failed to parse from : %s", config);
  }
  retval = cvmfs_init_v2(options_mgr);
  if (retval) {
    LogCvmfs(kLogCvmfs, kLogStderr,
    "CVMFS Initilization failed : %s", repo);
  }
  cvmfs_context *ctx;
  retval = cvmfs_attach_repo_v2(repo, options_mgr, &ctx);
  if (retval) {
    LogCvmfs(kLogCvmfs, kLogStderr,
    "CVMFS Attach to %s failed", repo);
  }
  cvmfs_enable_threaded(ctx);
  result->ctx = ctx;
  cvmfs_adopt_options(ctx, options_mgr);
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
