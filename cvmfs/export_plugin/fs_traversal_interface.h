/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_
#define CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_

#include "libcvmfs.h"

struct fs_traversal_context {
  uint64_t version;
  uint64_t size;

  const char *repo;
  const char *data;

  void * ctx;
};

enum fs_open_type {
  fs_open_read = 1,
  fs_open_write = 2,
  fs_open_append = 4
};

struct fs_traversal {
  /**
   * Method which initializes a file system traversal context based on
   * repository and data storage information
   * 
   * @param[in] repo Repository information
   * @param[in] data Data storage information
   * @param[out] A pointer to the context
   */
  struct fs_traversal_context *(*initialize)(
    const char *repo,
    const char *data);

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
   * Method which returns a list over the given directory
   * 
   * This should not include hidden directories (like the data directory)
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
   * @param[in] ctx The file system traversal context
   * @param[in] path The path of the object to stat
   * @param[out] stat The stat struct to write the information into
   * @returns 0 on success
   */
  int (*get_stat)(struct fs_traversal_context *ctx,
                const char *path, struct cvmfs_stat *stat);

  /**
   * Method which returns an identifier (usually a path)
   * to a file identified by a stat struct
   * 
   * @param[in] ctx The file system traversal context
   * @param[in] stat The stat struct describing the file
   * @returns A path that can be used to identify the file
   * The memory of the char is allocated on the heap and needs to be freed
   */
  const char *(*get_identifier)(struct fs_traversal_context *ctx,
                const struct cvmfs_stat *stat);


  /**
   * Method which checks whether the file described by the given
   * path exists in the destination file system
   * 
   * @param[in] ctx The file system traversal context
   * @param[in] identifier The identifier of the file
   * (obtained by get_identifier)
   * @returns True if file was found, false if not
   */
  bool (*has_file)(struct fs_traversal_context *ctx,
                const char *identifier);

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
  int (*do_unlink)(struct fs_traversal_context *ctx,
                const char *path);

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
                const struct cvmfs_stat *stat);

  /**
   * Method which removes the given directory
   * 
   * Error if:
   * - Directory doesn't exist
   * - Error during removal of directory or child
   * - On ENAMETOOLONG if the full path is too long during removal
   * 
   * @param[in] ctx The file system traversal context
   * @param[in] path The path to the directory that should be removed
   */
  int (*do_rmdir)(struct fs_traversal_context *ctx,
                const char *path);

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
  int (*touch)(struct fs_traversal_context *ctx,
                const struct cvmfs_stat *stat);

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
  void *(*get_handle)(struct fs_traversal_context *ctx,
                const char *identifier);


  /**
   * Method which creates a symlink at src which points to dest
   * 
   * If src already exists the old entry will silently be removed
   * and a new symlink will be created
   * 
   * Error:
   * - src directory doesn't exist
   * - symlink creation fails
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
                const struct cvmfs_stat *stat_info);

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

#endif  // CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_
