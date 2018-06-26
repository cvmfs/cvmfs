/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_
#define CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_

#include "hash.h"
#include "pointer.h"
#include "shortstring.h"

enum fs_type {
  FS_CVMFS;
  FS_POSIX;
  FS_SQUASH;
};

struct fs_traversal_context {
  uint64_t version;
  uint64_t size;
  fs_type type;

  char *repo;
  char *data;

  void * ctx;
};

struct fs_traversal {

  struct fs_traversal_context *ctx;

  struct fs_traversal *(*initialize(fs_type type, const char *repo, const char *data));
  void (*finalize)();

  /**
   * Method which returns an iterator over all files, symlinks and directories
   * in the root destination directory
   * 
   * @params[out] iterator Where the iterator should be saved
   */
  void (*listdir)(const char *dir, char ***buf, size_t len);

  /**
   * Method which checks whether the file described by the given content hash
   * exists in the destination file system
   * 
   * This should always be realised by a file system lookup since the all files
   * should be hardlinked once by their content hash.
   * 
   * @param[in] hash The content hash of the file that should searchedc
   * @returns True if file was found, false if not
   * 
   */
  bool (*has_hash)(const shash::Any hash);

  /**
   * Method which creates a hardlink from the given path to the file identified
   * by its content hash.
   * 
   * For this call to succeed the file addressed by the content hash already
   * needs to exist in the destination file system
   * 
   * Error if:
   * - Hash file does not exist
   * - path already exists
   * 
   * @param[in] path The path at which the file should be hardlinked.
   * @param[in] hash The content hash of the file to hardlink
   */
  int (*link)(const char *path, void *hash);

  /**
   * Method removes the hardlink at the given path
   * 
   * Error if:
   * - unlink not successful
   * GetRootIterator
   * @param[in] path The path which should be removed
   */
  int (*unlink)(const char *path);

  /**
   * Method which will create the given directory
   * 
   * @param[in] path The path to the directory that should be created
   */
  int (*mkdir)(const char *path);

  /**
   * Method which removes the given directory
   * 
   * @param[in] path The path to the directory that should be removed
   */
  int (*rmdir)(const char *path);

  /**
   * Method which creates a copy of the given file on the destination file
   * system.
   * 
   * @param[in] fd A file descriptor to use for reading the file
   * @param[in] hash A hash by which the file can be identified (content hash)
   */
  int (*copy)(const char *path, void *hash);  // TODO(steuber): shash FILE -> c?

  /**
   * Method which creates a symlink at src which points to dest
   * 
   * @param[in] The position at which the symlink should be saved
   * @param[in] The position the symlink should point to
   */
  int (*symlink)(const char *src, const char *dest);

  /**
   * Method which executes a garbage collection on the destination file system.
   * This will remove all no longer linked content adressed files
   */
  int (*garbage_collection)();
};

#endif  // CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_
