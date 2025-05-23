/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_SHRINKWRAP_FS_TRAVERSAL_INTERFACE_H_
#define CVMFS_SHRINKWRAP_FS_TRAVERSAL_INTERFACE_H_

#include "libcvmfs.h"

#define COPY_BUFFER_SIZE 4096

struct fs_traversal_context {
  uint64_t version;
  uint64_t size;

  char *repo;
  char *base;
  char *data;
  char *config;
  char *lib_version;

  void *ctx;
};

enum fs_open_type {
  fs_open_read,
  fs_open_write,
  fs_open_append
};

struct fs_traversal {
  /**
   * Local copy of the context that should be set with the
   * returned value of initialize. This is allocated using
   * initialize and freed using finalize.
   */
  struct fs_traversal_context *context_;

  /**
   * Method which initializes a file system traversal context based on
   * repository and data storage information
   *
   * @param[in] repo Repository name
   * @param[in] base Base repository location
   * @param[in] data Data storage information
   * @param[in] config A configuration file name
   * @param[in] num_threads Number of threads that may be started
   * @param[out] A pointer to the context
   */
  struct fs_traversal_context *(*initialize)(const char *repo,
                                             const char *base,
                                             const char *data,
                                             const char *config,
                                             int num_threads);

  /**
   * Method which finalizes the file system traversal.
   * Must always be called after a context had been initialized and before the
   * procedure ends
   *
   * The context is freed during execution of this method
   *
   * @param[in] ctx The context to finalize and free
   */
  void (*finalize)(struct fs_traversal_context *ctx);

  /**
   * Method that takes the specifications provided and stores
   * them in a provenance directory.
   *
   * @param[in] src The src context being archived
   * @param[in] dest The dest context being archived
   */
  void (*archive_provenance)(struct fs_traversal_context *src,
                             struct fs_traversal_context *dest);

  /**
   * Method which returns a list of the given directory.
   * Should not include "." or "..".
   *
   * @param[in] ctx The file system traversal context
   * @param[in] dir The directory over which should be iterated
   * @param[out] buf The list of the paths to the elements in the directory
   * @param[out] len Length of the output list
   */
  void (*list_dir)(struct fs_traversal_context *ctx,
                   const char *dir,
                   char ***buf,
                   size_t *len);

  /**
   * Method which returns a stat struct given a file path.
   * If the file doesn't exist, NULL is returned
   *
   * @note(steuber): The content checksum only needs to be calculated if this
   * is a source file system!
   *
   * For correct behaviour the following fields must be set
   * (and may not be NULL):
   * - version
   * - size
   * - st_mode
   * - st_uid
   * - st_gid
   * - cvm_name
   *
   *
   * @param[in] ctx The file system traversal context
   * @param[in] path The path of the object to stat
   * @param[out] stat The stat struct to write the information into
   * @param[in] get_hash Whether the file hash in cvm_checksum
   *  should be calculated (true) or not (false)
   * @returns 0 on success
   */
  int (*get_stat)(struct fs_traversal_context *ctx, const char *path,
                  struct cvmfs_attr *stat, bool get_hash);

  /**
   * Checks whether for a given stat the file at the stat's path name is
   * actually linked to the file described by the stat's checksum
   *
   * If this method returns false it usually means the file is out of sync
   * and needs to be copied and linked
   *
   * Error if:
   * - Visible path (stat->cvm_name) doesn't exist (ENOENT)
   *
   *
   * @param[in] ctx The file system traversal context
   * @param[in] stat The stat which should be used for the consistency check
   * @returns true if consistent, false if not
   *    is_hash_consistent returns false and a non-zero errno on error
   */
  bool (*is_hash_consistent)(struct fs_traversal_context *ctx,
                             const struct cvmfs_attr *stat);

  /**
   * Sets the meta information for the directory entry.
   * This assumes that other information is the same, and
   * only updates the metadata. Used only on directories.
   *
   * @param[in] ctx The file system traversal context
   * @param[in] path The path of the object to be updated
   * @param[in] stat The stat structure that determines the new values
   * @returns 0 on success, -1 otherwise
   */
  int (*set_meta)(struct fs_traversal_context *ctx, const char *path,
                  const struct cvmfs_attr *stat);

  /**
   * Method which returns an identifier (usually a path)
   * to a file identified by a stat struct
   *
   * The memory of the char is allocated on the heap and needs to be freed by
   * the caller
   *
   * @param[in] ctx The file system traversal context
   * @param[in] stat The stat struct describing the file
   * @returns A path that can be used to identify the file
   */
  char *(*get_identifier)(struct fs_traversal_context *ctx,
                          const struct cvmfs_attr *stat);


  /**
   * Method which checks whether the file described by the given
   * path exists in the destination file system
   *
   * @param[in] ctx The file system traversal context
   * @param[in] identifier The identifier of the file
   * (obtained by get_identifier)
   * @returns True if file was found, false if not
   */
  bool (*has_file)(struct fs_traversal_context *ctx, const char *identifier);

  /**
   * Method which creates a hardlink from the given path to the file given
   * by its identifier
   *
   * For this call to succeed the file given by its identifier already needs to
   * exist in the destination filesystem
   *
   * If the path already exists the old entry will silently be removed
   * and a new link will be created
   *
   * Error if:
   * - File described by identifier does not exist
   * - Directory does not exist
   *
   * @param[in] ctx The file system traversal context
   * @param[in] path The path where the file should be linked
   * @param[in] identifier The identifier of the file
   * (usually obtained through get_identifier)
   */
  int (*do_link)(struct fs_traversal_context *ctx,
                 const char *path,
                 const char *identifier);

  /**
   * Method removes the hardlink at the given path
   *
   * Error if:
   * - Link does not exist
   * - Link is directory
   * - Unlink not successful
   *
   * @param[in] ctx The file system traversal context
   * @param[in] path The path which should be removed
   */
  int (*do_unlink)(struct fs_traversal_context *ctx, const char *path);

  /**
   * Method which will create the given directory
   *
   * If the path already exists the old entry will silently be removed
   * and a new directory will be created
   *
   * Error if:
   * - Parent directory not defined
   *
   * @param[in] ctx The file system traversal context
   * @param[in] path The path to the directory that should be created
   * @param[in] stat The stat containing the meta data for directory creation
   */
  int (*do_mkdir)(struct fs_traversal_context *ctx,
                  const char *path,
                  const struct cvmfs_attr *stat);

  /**
   * Method which removes the given directory
   *
   * Error if:
   * - Directory doesn't exist
   * - Directory is not empty
   *
   * @param[in] ctx The file system traversal context
   * @param[in] path The path to the directory that should be removed
   */
  int (*do_rmdir)(struct fs_traversal_context *ctx, const char *path);

  /**
   * Atomically creates the file representing
   * the given identifier
   *
   * Error:
   * - If file exists (errno set to EEXIST)
   * - Error during creation and meta data saving operations
   *
   * @param[in] ctx The file system traversal context
   * @param[in] stat The stat containing the meta data for file creation
   */
  int (*touch)(struct fs_traversal_context *ctx, const struct cvmfs_attr *stat);

  /**
   * Retrieves a handle struct which allows the manipulation of the file
   * defined by the given identifier
   *
   * @param[in] ctx The file system traversal context
   * @param[in] identifier The identifier of the file to manipulate
   * (usually obtained through get_identifier)
   * @returns A context that can be used for reading from and writing to the
   * specified file (needs to be freed by ffree method)
   */
  void *(*get_handle)(struct fs_traversal_context *ctx, const char *identifier);


  /**
   * Method which creates a symlink at src which points to dest
   *
   * If src already exists the old entry will silently be removed
   * and a new symlink will be created
   *
   * Error:
   * - src directory doesn't exist
   * - symlink creation fails
   * - on empty destination
   *
   * @param[in] ctx The file system traversal context
   * @param[in] src The position at which the symlink should be saved
   * (parent directory must exist)
   * @param[in] dest The position the symlink should point to
   * @param[in] stat_info The stat containing the meta data for symlink creation
   * @note(steuber): Currently permissions and user. xattributes are not
   * established due to posix limitations.
   */
  int (*do_symlink)(struct fs_traversal_context *ctx,
                    const char *src,
                    const char *dest,
                    const struct cvmfs_attr *stat_info);

  /**
   * Method which executes garbage collection on the file systems hidden data
   * directories.
   *
   * This method is usually quite costly in terms of time and should only be
   * invoked with care!
   * If multiple repositories share the same hidden data repository it makes
   * sense to invoke it only once all repositories are up to date.
   *
   * @param[in] ctx The file system traversal context
   * @returns 0 on success
   */
  int (*garbage_collector)(struct fs_traversal_context *ctx);

  /**
   * Method which opens a file described by the given file context.
   * Needs to be called before read and write operations
   *
   * @param[in,out] file_ctx The file context used for opening
   * @param[in] op_mode The mode for opening the file
   * @returns 0 on success
   */
  int (*do_fopen)(void *file_ctx, fs_open_type op_mode);

  /**
   * Method which closes a file described by the given file context.
   * Needs to be called after open.
   * After this operation open needs to be called again before further
   * read and write operations
   *
   * @param[in,out] file_ctx The file context used
   * @returns 0 on success
   */
  int (*do_fclose)(void *file_ctx);

  /**
   * Method which reads len chars into buff from the file described by the given
   * context
   *
   * @param[in] file_ctx The file context used
   * @param[out] buff The buffer to write to
   * @param[in] len The length of the buffer buff
   * @param[out] read_len The number of chars read
   * $returns 0 on success
   */
  int (*do_fread)(void *file_ctx, char *buff, size_t len, size_t *read_len);

  /**
   * Method which writes len chars from buff into the file described by the
   * given context
   *
   * @param[in] file_ctx The file context used
   * @param[in] buff The buffer to read from
   * @param[in] len The length of the buffer buff
   * @returns 0 on success
   */
  int (*do_fwrite)(void *file_ctx, const char *buff, size_t len);

  /**
   * Method which frees a given file context
   */
  void (*do_ffree)(void *file_ctx);
};

#endif  // CVMFS_SHRINKWRAP_FS_TRAVERSAL_INTERFACE_H_
