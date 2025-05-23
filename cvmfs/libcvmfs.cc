/**
 * This file is part of the CernVM File System.
 *
 * libcvmfs provides an API for the CernVM-FS client.  This is an
 * alternative to FUSE for reading a remote CernVM-FS repository.
 */

#define _FILE_OFFSET_BITS 64

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif


#include "libcvmfs.h"

#include <errno.h>
#include <inttypes.h>
#include <stddef.h>
#include <sys/stat.h>

#include <cassert>
#include <cstdlib>
#include <string>

#include "libcvmfs_int.h"
#include "statistics.h"
#include "util/logging.h"
#include "util/posix.h"
#include "util/smalloc.h"
#include "xattr.h"

using namespace std;  // NOLINT

/**
 * Create the cvmfs_attr struct which contains the same information
 * as a stat, but also has pointers to the hash, symlink, and name.
 */
struct cvmfs_attr *cvmfs_attr_init() {
  struct cvmfs_attr *attr;
  attr = reinterpret_cast<cvmfs_attr *>(calloc(1, sizeof(*attr)));
  attr->version = 1;
  attr->size = sizeof(*attr);
  return attr;
}


/**
 * Destroy the cvmfs_attr struct and frees the checksum, symlink,
 * name, and xattrs.
 */
void cvmfs_attr_free(struct cvmfs_attr *attr) {
  if (attr) {
    free(attr->cvm_checksum);
    free(attr->cvm_symlink);
    free(attr->cvm_name);
    free(attr->cvm_parent);
    delete reinterpret_cast<XattrList *>(attr->cvm_xattrs);
  }
  free(attr);
}


struct cvmfs_nc_attr *cvmfs_nc_attr_init() {
  struct cvmfs_nc_attr *attr;
  attr = reinterpret_cast<cvmfs_nc_attr *>(calloc(1, sizeof(*attr)));
  return attr;
}

void cvmfs_nc_attr_free(struct cvmfs_nc_attr *nc_attr) {
  if (nc_attr) {
    free(nc_attr->mountpoint);
    free(nc_attr->hash);
  }
  free(nc_attr);
}


/**
 * Expand symlinks in all levels of a path.  Also, expand ".." and ".".  This
 * also has the side-effect of ensuring that cvmfs_getattr() is called on all
 * parent paths, which is needed to ensure proper loading of nested catalogs
 * before the child is accessed.
 */
static int expand_path(const int depth,
                       LibContext *ctx,
                       char const *path,
                       string *expanded_path) {
  string p_path = GetParentPath(path);
  string fname = GetFileName(path);
  int rc;

  if (fname == "..") {
    rc = expand_path(depth, ctx, p_path.c_str(), expanded_path);
    if (rc != 0) {
      return -1;
    }
    if (*expanded_path == "/") {
      // attempt to access parent path of the root of the repository
      LogCvmfs(kLogCvmfs, kLogDebug,
               "libcvmfs cannot resolve symlinks to paths outside of the repo: "
               "%s",
               path);
      errno = ENOENT;
      return -1;
    }
    *expanded_path = GetParentPath(*expanded_path);
    if (*expanded_path == "") {
      *expanded_path = "/";
    }
    return 0;
  }

  string buf;
  if (p_path != "") {
    rc = expand_path(depth, ctx, p_path.c_str(), &buf);
    if (rc != 0) {
      return -1;
    }

    if (fname == ".") {
      *expanded_path = buf;
      return 0;
    }
  }

  if (buf.length() == 0 || buf[buf.length() - 1] != '/') {
    buf += "/";
  }
  buf += fname;

  struct stat st;
  rc = ctx->GetAttr(buf.c_str(), &st);
  if (rc != 0) {
    errno = -rc;
    return -1;
  }

  if (!S_ISLNK(st.st_mode)) {
    *expanded_path = buf;
    return 0;
  }

  if (depth > 1000) {
    // avoid unbounded recursion due to symlinks
    LogCvmfs(kLogCvmfs, kLogDebug,
             "libcvmfs hit its symlink recursion limit: %s", path);
    errno = ELOOP;
    return -1;
  }

  // expand symbolic link

  char *ln_buf = reinterpret_cast<char *>(alloca(st.st_size + 2));
  if (!ln_buf) {
    errno = ENOMEM;
    return -1;
  }
  rc = ctx->Readlink(buf.c_str(), ln_buf, st.st_size + 2);
  if (rc != 0) {
    errno = -rc;
    return -1;
  }
  if (ln_buf[0] == '/') {
    // symlink is absolute path, strip /cvmfs/$repo
    unsigned len = ctx->mount_point()->fqrn().length();
    if (strncmp(ln_buf, ctx->mount_point()->fqrn().c_str(), len) == 0
        && (ln_buf[len] == '/' || ln_buf[len] == '\0')) {
      buf = ln_buf + len;
      if (ln_buf[len] == '\0') {
        buf += "/";
      }
    } else {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "libcvmfs can't resolve symlinks to paths outside of the repo: "
               "%s --> %s (mountpoint=%s)",
               path, ln_buf, ctx->mount_point()->fqrn().c_str());
      errno = ENOENT;
      return -1;
    }
  } else {
    // symlink is relative path
    buf = GetParentPath(buf);
    buf += "/";
    buf += ln_buf;
  }

  // In case the symlink references other symlinks or contains ".."
  // or "."  we must now call expand_path on the result.

  return expand_path(depth + 1, ctx, buf.c_str(), expanded_path);
}

/**
 * Like expand_path(), but do not expand the final element of the path.
 */
static int expand_ppath(LibContext *ctx,
                        const char *path,
                        string *expanded_path) {
  string p_path = GetParentPath(path);
  string fname = GetFileName(path);

  if (p_path == "") {
    *expanded_path = path;
    return 0;
  }

  int rc = expand_path(0, ctx, p_path.c_str(), expanded_path);
  if (rc != 0) {
    return rc;
  }

  (*expanded_path) += "/";
  (*expanded_path) += fname;

  return 0;
}


int cvmfs_open(LibContext *ctx, const char *path) {
  string lpath;
  int rc;
  rc = expand_path(0, ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  rc = ctx->Open(path);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return rc;
}


ssize_t cvmfs_pread(
    LibContext *ctx, int fd, void *buf, size_t size, off_t off) {
  ssize_t nbytes = ctx->Pread(fd, buf, size, off);
  if (nbytes < 0) {
    errno = -nbytes;
    return -1;
  }
  return nbytes;
}


int cvmfs_close(LibContext *ctx, int fd) {
  int rc = ctx->Close(fd);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}


int cvmfs_readlink(LibContext *ctx, const char *path, char *buf, size_t size) {
  string lpath;
  int rc;
  rc = expand_ppath(ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  rc = ctx->Readlink(path, buf, size);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}


int cvmfs_stat(LibContext *ctx, const char *path, struct stat *st) {
  string lpath;
  int rc;
  rc = expand_path(0, ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  rc = ctx->GetAttr(path, st);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}


int cvmfs_lstat(LibContext *ctx, const char *path, struct stat *st) {
  string lpath;
  int rc;
  rc = expand_ppath(ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  rc = ctx->GetAttr(path, st);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}


int cvmfs_stat_attr(LibContext *ctx,
                    const char *path,
                    struct cvmfs_attr *attr) {
  string lpath;
  int rc;
  rc = expand_ppath(ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  rc = ctx->GetExtAttr(path, attr);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}


int cvmfs_listdir(LibContext *ctx,
                  const char *path,
                  char ***buf,
                  size_t *buflen) {
  string lpath;
  int rc;
  rc = expand_path(0, ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  size_t listsize = 0;
  rc = ctx->ListDirectory(path, buf, &listsize, buflen, true);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}

int cvmfs_listdir_contents(LibContext *ctx,
                           const char *path,
                           char ***buf,
                           size_t *listlen,
                           size_t *buflen) {
  string lpath;
  int rc;
  rc = expand_path(0, ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  rc = ctx->ListDirectory(path, buf, listlen, buflen, false);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}

int cvmfs_listdir_stat(LibContext *ctx,
                       const char *path,
                       struct cvmfs_stat_t **buf,
                       size_t *listlen,
                       size_t *buflen) {
  string lpath;
  int rc;
  rc = expand_path(0, ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  rc = ctx->ListDirectoryStat(path, buf, listlen, buflen);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}


int cvmfs_stat_nc(LibContext *ctx,
                  const char *path,
                  struct cvmfs_nc_attr *nc_attr) {
  string lpath;
  int rc;
  rc = expand_path(0, ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  rc = ctx->GetNestedCatalogAttr(path, nc_attr);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}


int cvmfs_list_nc(LibContext *ctx,
                  const char *path,
                  char ***buf,
                  size_t *buflen) {
  string lpath;
  int rc;
  rc = expand_path(0, ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  rc = ctx->ListNestedCatalogs(path, buf, buflen);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}


void cvmfs_list_free(char **buf) {
  // Quick return if base pointer is NULL
  if (!buf)
    return;
  size_t pos = 0;
  // Iterate over each non-null entry and free
  // This assumes no null entries, which don't currently exist
  while (buf[pos]) {
    free(buf[pos]);
    pos++;
  }
  free(buf);
}

void cvmfs_enable_threaded(LibContext *ctx) { ctx->EnableMultiThreaded(); }

cvmfs_errors cvmfs_attach_repo_v2(const char *fqrn,
                                  SimpleOptionsParser *opts,
                                  LibContext **ctx) {
  assert(ctx != NULL);
  opts->SwitchTemplateManager(new DefaultOptionsTemplateManager(fqrn));
  *ctx = LibContext::Create(fqrn, opts);
  assert(*ctx != NULL);
  loader::Failures result = (*ctx)->mount_point()->boot_status();
  if (result != loader::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Attaching %s failed: %s (%d)", fqrn,
             (*ctx)->mount_point()->boot_error().c_str(), result);
    delete *ctx;
    *ctx = NULL;
  }
  return static_cast<cvmfs_errors>(result);
}


void cvmfs_adopt_options(cvmfs_context *ctx, SimpleOptionsParser *opts) {
  ctx->set_options_mgr(opts);
}


void cvmfs_detach_repo(LibContext *ctx) { delete ctx; }


cvmfs_errors cvmfs_init_v2(SimpleOptionsParser *opts) {
  int result = LibGlobals::Initialize(opts);
  if (result != loader::kFailOk) {
    LogCvmfs(kLogCvmfs, kLogStderr, "Initialization failed: %s (%d)",
             LibGlobals::GetInstance()->file_system()->boot_error().c_str(),
             result);
    LibGlobals::CleanupInstance();
  }
  return static_cast<cvmfs_errors>(result);
}


void cvmfs_fini() { LibGlobals::CleanupInstance(); }


static void (*ext_log_fn)(const char *msg) = NULL;


static void libcvmfs_log_fn(const LogSource /*source*/,
                            const int /*mask*/,
                            const char *msg) {
  if (ext_log_fn) {
    (*ext_log_fn)(msg);
  }
}


void cvmfs_set_log_fn(void (*log_fn)(const char *msg)) {
  ext_log_fn = log_fn;
  if (log_fn == NULL) {
    SetAltLogFunc(NULL);
  } else {
    SetAltLogFunc(libcvmfs_log_fn);
  }
}


char *cvmfs_statistics_format(cvmfs_context *ctx) {
  assert(ctx != NULL);
  std::string stats = ctx->mount_point()->statistics()->PrintList(
      perf::Statistics::kPrintHeader);
  return strdup(stats.c_str());
}


int cvmfs_remount(LibContext *ctx) {
  assert(ctx != NULL);
  return ctx->Remount();
}


uint64_t cvmfs_get_revision(LibContext *ctx) {
  assert(ctx != NULL);
  return ctx->GetRevision();
}
