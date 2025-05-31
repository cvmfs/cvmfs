/**
 * This file is part of the CernVM File System.
 */

#include "fs_traversal_libcvmfs.h"

#include <stdio.h>
#include <stdlib.h>

#include <cstring>
#include <string>
#include <vector>

#include "fs_traversal_interface.h"
#include "libcvmfs.h"
#include "util/logging.h"
#include "util/smalloc.h"
#include "util/string.h"

#define MAX_INTEGER_DIGITS 20


void libcvmfs_sw_log(const char *msg) { printf("(libcvmfs) %s\n", msg); }


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
                      const char *path,
                      struct cvmfs_attr *stat_result,
                      bool get_hash) {
  cvmfs_context *context = reinterpret_cast<cvmfs_context *>(ctx->ctx);
  return cvmfs_stat_attr(context, path, stat_result);
}


char *libcvmfs_get_identifier(struct fs_traversal_context *ctx,
                              const struct cvmfs_attr *stat) {
  const int length = 2 + strlen(stat->cvm_parent) + strlen(stat->cvm_name);
  char *res = reinterpret_cast<char *>(smalloc(length));
  snprintf(res, length, "%s/%s", stat->cvm_parent, stat->cvm_name);
  return res;
}


bool libcvmfs_has_file(struct fs_traversal_context *ctx, const char *ident) {
  cvmfs_context *context = reinterpret_cast<cvmfs_context *>(ctx->ctx);
  struct cvmfs_attr *attr = cvmfs_attr_init();
  const int retval = cvmfs_stat_attr(context, ident, attr);
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
  struct libcvmfs_file_handle
      *file_ctx = reinterpret_cast<libcvmfs_file_handle *>(
          calloc(1, sizeof(*file_ctx)));
  cvmfs_context *context = reinterpret_cast<cvmfs_context *>(ctx->ctx);
  file_ctx->ctx = context;
  file_ctx->path = strdup(identifier);

  return file_ctx;
}


int libcvmfs_do_fopen(void *file_ctx, fs_open_type op_mode) {
  struct libcvmfs_file_handle
      *handle = reinterpret_cast<libcvmfs_file_handle *>(file_ctx);
  handle->fd = cvmfs_open(handle->ctx, handle->path);
  if (handle->fd == -1) {
    return -1;
  }
  return 0;
}


int libcvmfs_do_fclose(void *file_ctx) {
  struct libcvmfs_file_handle
      *handle = reinterpret_cast<libcvmfs_file_handle *>(file_ctx);
  const int res = cvmfs_close(handle->ctx, handle->fd);
  if (res != 0)
    return -1;
  handle->fd = -1;
  return 0;
}


int libcvmfs_do_fread(void *file_ctx,
                      char *buff,
                      size_t len,
                      size_t *read_len) {
  struct libcvmfs_file_handle
      *handle = reinterpret_cast<libcvmfs_file_handle *>(file_ctx);
  const ssize_t read =
      cvmfs_pread(handle->ctx, handle->fd, buff, len, handle->off);
  if (read == -1) {
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
  struct libcvmfs_file_handle
      *handle = reinterpret_cast<libcvmfs_file_handle *>(file_ctx);
  if (handle->fd > 0) {
    libcvmfs_do_fclose(file_ctx);
  }
  free(handle->path);
  free(handle);
}


struct fs_traversal_context *libcvmfs_initialize(const char *repo,
                                                 const char *base,
                                                 const char *data,
                                                 const char *config,
                                                 int num_threads) {
  if (!repo) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Repository name must be specified");
    return NULL;
  }
  int retval = 0;

  struct fs_traversal_context *result = new struct fs_traversal_context;
  result->version = 1;
  char *major = reinterpret_cast<char *>(smalloc(MAX_INTEGER_DIGITS));
  snprintf(major, MAX_INTEGER_DIGITS, "%d", LIBCVMFS_VERSION_MAJOR);
  char *minor = reinterpret_cast<char *>(smalloc(MAX_INTEGER_DIGITS));
  snprintf(minor, MAX_INTEGER_DIGITS, "%d", LIBCVMFS_VERSION_MINOR);
  char *rev = reinterpret_cast<char *>(smalloc(MAX_INTEGER_DIGITS));
  snprintf(rev, MAX_INTEGER_DIGITS, "%d", LIBCVMFS_REVISION);
  const size_t len = 3 + strlen(major) + strlen(minor) + strlen(rev);
  char *lib_version = reinterpret_cast<char *>(smalloc(len));
  snprintf(lib_version, len, "%s.%s:%s", major, minor, rev);

  result->lib_version = strdup(lib_version);

  free(major);
  free(minor);
  free(rev);
  free(lib_version);

  result->size = sizeof(*result);
  result->repo = strdup(repo);
  result->base = NULL;
  result->data = NULL;
  result->config = NULL;

  // Make cvmfs options part of the environment
  cvmfs_option_map *options_mgr = cvmfs_options_init_v2(1);
  if (config) {
    result->config = strdup(config);
  } else {
    const size_t len = 8 + strlen(result->repo);
    char *def_config = reinterpret_cast<char *>(smalloc(len * sizeof(char)));
    snprintf(def_config, len, "%s.config", result->repo);
    result->config = strdup(def_config);
    free(def_config);
  }
  std::vector<std::string> config_files = SplitString(result->config, ':');
  for (unsigned i = 0; i < config_files.size(); ++i) {
    retval = cvmfs_options_parse(options_mgr, config_files[i].c_str());
    if (retval) {
      LogCvmfs(kLogCvmfs, kLogStderr, "CVMFS Options failed to parse from : %s",
               config_files[i].c_str());
      return NULL;
    }
  }

  // Override repository name even if specified
  cvmfs_options_set(options_mgr, "CVMFS_REPOSITORY_NAME", repo);

  // Override Mount dir if not specified in configuration
  if (base && !cvmfs_options_get(options_mgr, "CVMFS_MOUNT_DIR")) {
    cvmfs_options_set(options_mgr, "CVMFS_MOUNT_DIR", base);
  }

  // Override Cache base if not specified in configuration
  if (data
      && (!cvmfs_options_get(options_mgr, "CVMFS_CACHE_BASE")
          && !cvmfs_options_get(options_mgr, "CVMFS_CACHE_DIR"))) {
    cvmfs_options_set(options_mgr, "CVMFS_CACHE_BASE", data);
    result->data = strdup(data);
  }

  retval = cvmfs_init_v2(options_mgr);
  if (retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "CVMFS Initialization failed : %s", repo);
    return NULL;
  }

  cvmfs_set_log_fn(libcvmfs_sw_log);

  cvmfs_context *ctx;
  retval = cvmfs_attach_repo_v2(repo, options_mgr, &ctx);
  if (retval) {
    LogCvmfs(kLogCvmfs, kLogStderr, "CVMFS Attach to %s failed", repo);
    return NULL;
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
  free(ctx->repo);
  free(ctx->base);
  free(ctx->data);
  free(ctx->config);
  free(ctx->lib_version);
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
