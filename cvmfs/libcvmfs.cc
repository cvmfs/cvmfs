/**
 * This file is part of the CernVM File System.
 *
 * libcvmfs provides an API for the CernVM-FS client.  This is an
 * alternative to FUSE for reading a remote CernVM-FS repository.
 */

#define _FILE_OFFSET_BITS 64
#define __STDC_FORMAT_MACROS

#include "cvmfs_config.h"
#include "libcvmfs.h"

#include <errno.h>
#include <inttypes.h>
#include <stddef.h>
#include <sys/stat.h>

#include <cassert>
#include <cstdlib>
#include <string>

#include "libcvmfs_int.h"
#include "logging.h"
#include "smalloc.h"
#include "statistics.h"
#include "util/posix.h"

using namespace std;  // NOLINT

/**
 * Expand symlinks in all levels of a path.  Also, expand ".." and ".".  This
 * also has the side-effect of ensuring that cvmfs_getattr() is called on all
 * parent paths, which is needed to ensure proper loading of nested catalogs
 * before the child is accessed.
 */
static int expand_path(
  const int depth,
  LibContext *ctx,
  char const *path,
  string *expanded_path)
{
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
               "%s", path);
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

  if (buf.length() == 0 || buf[buf.length()-1] != '/') {
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

  char *ln_buf = reinterpret_cast<char *>(alloca(st.st_size+2));
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
    if (strncmp(ln_buf, ctx->mount_point()->fqrn().c_str(), len) == 0 &&
        (ln_buf[len] == '/' || ln_buf[len] == '\0'))
    {
      buf = ln_buf+len;
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
                        string *expanded_path)
{
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
  LibContext *ctx,
  int fd,
  void *buf,
  size_t size,
  off_t off)
{
  ssize_t nbytes = ctx->Pread(fd, buf, size, off);
  if (nbytes < 0) {
    errno = -nbytes;
    return -1;
  }
  return nbytes;
}


int cvmfs_close(LibContext *ctx, int fd)
{
  int rc = ctx->Close(fd);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}


int cvmfs_readlink(
  LibContext *ctx,
  const char *path,
  char *buf,
  size_t size
) {
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


int cvmfs_listdir(
  LibContext *ctx,
  const char *path,
  char ***buf,
  size_t *buflen
) {
  string lpath;
  int rc;
  rc = expand_path(0, ctx, path, &lpath);
  if (rc < 0) {
    return -1;
  }
  path = lpath.c_str();

  rc = ctx->ListDirectory(path, buf, buflen);
  if (rc < 0) {
    errno = -rc;
    return -1;
  }
  return 0;
}


cvmfs_errors cvmfs_attach_repo_v2(
  const char *fqrn,
  OptionsManager *opts,
  LibContext **ctx)
{
  assert(ctx != NULL);
  *ctx = LibContext::Create(fqrn, opts);
  assert(*ctx != NULL);
  return static_cast<cvmfs_errors>((*ctx)->mount_point()->boot_status());
}


void cvmfs_detach_repo(LibContext *ctx) {
  delete ctx;
}


cvmfs_errors cvmfs_init_v2(OptionsManager *opts) {
  int result = LibGlobals::Initialize(opts);
  return static_cast<cvmfs_errors>(result);
}


void cvmfs_fini() {
  LibGlobals::CleanupInstance();
}


static void (*ext_log_fn)(const char *msg) = NULL;


static void libcvmfs_log_fn(
  const LogSource /*source*/,
  const int /*mask*/,
  const char *msg
) {
  if (ext_log_fn) {
    (*ext_log_fn)(msg);
  }
}


void cvmfs_set_log_fn(void (*log_fn)(const char *msg))
{
  ext_log_fn = log_fn;
  if (log_fn == NULL) {
    SetAltLogFunc(NULL);
  } else {
    SetAltLogFunc(libcvmfs_log_fn);
  }
}


int cvmfs_remount(LibContext *ctx) {
  catalog::LoadError retval = ctx->RemountStart();
  if (retval == catalog::kLoadNew || retval == catalog::kLoadUp2Date) {
    return 0;
  }
  return -1;
}
