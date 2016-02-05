/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_LIBCVMFS_H_
#define CVMFS_LIBCVMFS_H_

/*
 * NOTE: when adding or removing public symbols, you must also update
 * the list in libcvmfs_public_syms.txt.
 */
#define LIBCVMFS_VERSION 2
#define LIBCVMFS_VERSION_MAJOR LIBCVMFS_VERSION
#define LIBCVMFS_VERSION_MINOR 3
// Revision Changelog
// 13: revision introduced
// 14: fix expand_path for absolute paths, add mountpoint to cvmfs_context
// 15: remove counting of open file descriptors
// 16: remove unnecessary free
// 17: apply new classes around the cache manager
// 18: add cvmfs_pread and support for chunked files
// 19: CernVM-FS 2.2.0
// 20: fix reading of chunked files
#define LIBCVMFS_REVISION 20

#include <sys/stat.h>
#include <unistd.h>

#define LIBCVMFS_FAIL_OK         0
/**
 * Could not increase the number of open files limit
 */
#define LIBCVMFS_FAIL_NOFILES   -1
/**
 * Could not create the cache directory
 */
#define LIBCVMFS_FAIL_MKCACHE   -2
/**
 * Could not change into the cache directory
 */
#define LIBCVMFS_FAIL_OPENCACHE -3
/**
 * Could not acquire lock file (flock)
 */
#define LIBCVMFS_FAIL_LOCKFILE  -4
/**
 * Could not create cache directory skeleton
 */
#define LIBCVMFS_FAIL_INITCACHE -5
/**
 * Could not initialize quota manager
 */
#define LIBCVMFS_FAIL_INITQUOTA -6
/**
 * Unknown option
 */
#define LIBCVMFS_FAIL_BADOPT    -7

#ifdef __cplusplus
extern "C" {
#endif

class cvmfs_context;

/**
 * Initialize global CVMFS library structures
 * Note: this _must_ be called before anything else in the library.
 *
 * @param[in] options, option1,option2,...
 * \return 0 on success
 */
int cvmfs_init(char const *options);

/**
 * Shut down the CVMFS library and release all resources.
 */
void cvmfs_fini();

/**
 * Initialize a CVMFS remote repository.
 *
 * See libcvmfs usage() for possible options.
 *
 * @param[in] options, option1,option2,...
 * \return 0 on success
 */
cvmfs_context* cvmfs_attach_repo(char const *options);

/**
 * Uninitialize a CVMFS remote repository and release all in memory resources
 * for it.
 */
void cvmfs_detach_repo(cvmfs_context *ctx);

/* Load a new catalog if there is one
 * \return 0 on success
 */
int cvmfs_remount(cvmfs_context *ctx);

/* Send syslog and debug messages to log_fn instead.  This may (and
 * probably should) be called before cvmfs_init().  Setting this to
 * NULL restores the default logging behavior.
 */
void cvmfs_set_log_fn( void (*log_fn)(const char *msg) );

/* Open a file in the CVMFS cache.
 *
 * @param[in] path, path to open (e.g. /dir/file, not /cvmfs/repo/dir/file)
 * \return read-only file descriptor, -1 on failure (sets errno)
 */
int cvmfs_open(cvmfs_context *ctx, const char *path);

/**
 * Reads from a file descriptor returned by cvmfs_open.  File descriptors that
 * have bit 30 set indicate chunked files.
 */
ssize_t cvmfs_pread(cvmfs_context *ctx,
                    int fd, void *buf, size_t size, off_t off);

/* Closes a file previously opened with cvmfs_open().
 *
 * @param[in] fd, file descriptor to close
 * \return 0 on success, -1 on failure (sets errno)
 */
int cvmfs_close(cvmfs_context *ctx, int fd);

/*
 * Reads a symlink from the catalog.  Environment variables in the
 * symlink that are of the form $(varname) are expanded.  If the
 * provided buffer is not long enough for the null-terminated value of
 * the symlink, -1 is returned and errno is set to ERANGE.
 *
 * @param[in] path, path of symlink (e.g. /dir/file, not /cvmfs/repo/dir/file)
 * @param[out] buf, buffer in which to write the null-terminated value of the symlink
 * @param[in] size, size of buffer
 * \return 0 on success, -1 on failure (sets errno)
 */
int cvmfs_readlink(cvmfs_context *ctx,
  const char *path,
  char *buf,
  size_t size);

/* Get information about a file.  If the file is a symlink,
 * return info about the file it points to, not the symlink itself.
 *
 * @param[in] path, path of file (e.g. /dir/file, not /cvmfs/repo/dir/file)
 * @param[out] st, stat buffer in which to write the result
 * \return 0 on success, -1 on failure (sets errno)
 */
int cvmfs_stat(cvmfs_context *ctx, const char *path, struct stat *st);

/* Get information about a file.  If the file is a symlink,
 * return info about the link, not the file it points to.
 *
 * @param[in] path, path of file (e.g. /dir/file, not /cvmfs/repo/dir/file)
 * @param[out] st, stat buffer in which to write the result
 * \return 0 on success, -1 on failure (sets errno)
 */
int cvmfs_lstat(cvmfs_context *ctx, const char *path, struct stat *st);

/* Get list of directory contents.  The directory contents
 * includes "." and "..".
 *
 * On return, the array will contain a NULL-terminated list of
 * strings.  The caller must free the strings and the array containing
 * them.  The array (*buf) may be NULL when this function is called.
 *
 * @param[in] path, path of directory (e.g. /dir, not /cvmfs/repo/dir)
 * @param[out] buf, pointer to dynamically allocated NULL-terminated array of
 *             strings
 * @param[in] buflen, pointer to variable containing size of array
 * \return 0 on success, -1 on failure (sets errno)
 */
int cvmfs_listdir(
  cvmfs_context *ctx,
  const char *path,
  char ***buf,
  size_t *buflen);

#ifdef __cplusplus
}
#endif

#endif  // CVMFS_LIBCVMFS_H_
