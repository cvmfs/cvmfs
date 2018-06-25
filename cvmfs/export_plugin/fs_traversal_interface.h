/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_
#define CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_

#include "hash.h"
#include "pointer.h"
#include "shortstring.h"

/**
 * The different possible types of objects
 */ 
enum FsObjectType {
  FS_FILE = 1,
  FS_SYMLINK,
  FS_DIRECTORY
};

struct FsDestinationTraversal {
  FsObjectImplementor fsObject;
  FsIterator iterator;
  DestinationFs destination;
};

/**
 * Interface to be implemented for file systems so FsObjects work with them
 */
struct FsObjectImplementor {
  void *(*InitiateFsObjectStruct)();
  void  (*DestroyFsObjectStruct)();

  FsObjectType (GetType)(void *fs_object);

  /**
   * Method which returns the identifier (name) of the object
   * 
   * @returns The identifier of the object
   */
  std::string (*GetIdentifier)(void *fs_object);

  /**
   * Method which returns the path of the object relative to the root of the
   * desired destination.
   * 
   * @returns The path of the object
   */
  std::string (*GetPath)(void *fs_object);

  /**
   * Method which returns the content hash of the file described by this object
   * 
   * @returns The content hash of the file
   */
  shash::Any (*GetHash)(void *fs_object);

  /**
   * Method which returns the path of the symlink described by this object
   * 
   * @returns THe path of the symlink
   */
  std::string (*GetDestination)(void *fs_object);

  /**
   * Method which returns an iterator over the subdirectory described by this
   * object
   * 
   * @params[out] iterator Where the iterator should be saved
   */
  void (*GetIterator)(void *iterator);
};

/**
 * Iterator which allows iteration over the contents of a directory
 */
struct FsIterator {
  void *(*InitiateFsIteratorContext)();
  void  (*DestroyFsIteratorContext)();

  /**
   * Method which retrieves the current iterator element.
   * If no elements are left NULL is returned.
   * The first invocation after object construction returns the first element.
   * Contrary to other iterators this call is idempotent!
   * 
   * @returns A pointer to the current File System object of the iterator
   */
  void (GetCurrent)(void *iteratorCtx, void *fsObject);

  /**
   * Method which checks if there are any elements left
   * 
   * @returns True if there is an element, false if not
   */
  bool (HasCurrent)(void *iteratorCtx);

  /**
   * Method which sets the current element (retrievable by GetCurrent())
   * to the next one.
   * 
   * Silently fails if no more elements are available GetGurrent will then
   * return NULL
   */
  void (Step)(void *iterator);
};

struct DestinationFs {
  void *(*InitiateDestFsContext(const char *dest));
  void (*DestroyDestFsContext)(void *ctx);

  /**
   * Method which returns an iterator over all files, symlinks and directories
   * in the root destination directory
   * 
   * @params[out] iterator Where the iterator should be saved
   */
  void (*GetRootIterator)(void *ctx, void *iterator);

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
  bool (*HasFile)(void *ctx, const shash::Any hash);

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
  int (*Link)(void *ctx, const char *path, shash::Any hash);

  /**
   * Method removes the hardlink at the given path
   * 
   * Error if:
   * - unlink not successful
   * GetRootIterator
   * @param[in] path The path which should be removed
   */
  int (*Unlink)(void *ctx, const char *path);

  /**
   * Method which will create the given directory
   * 
   * @param[in] path The path to the directory that should be created
   */
  int (*CreateDirectory)(void *ctx, const char *path);

  /**
   * Method which removes the given directory
   * 
   * @param[in] path The path to the directory that should be removed
   */
  int (*RemoveDirectory)(void *ctx, const char *path);

  /**
   * Method which creates a copy of the given file on the destination file
   * system.
   * 
   * @param[in] fd A file descriptor to use for reading the file
   * @param[in] hash A hash by which the file can be identified (content hash)
   */
  int (*CopyFile)(void *ctx, FILE *fd, shash::Any hash);  // TODO(steuber): shash FILE -> c?

  /**
   * Method which creates a symlink at src which points to dest
   * 
   * @param[in] The position at which the symlink should be saved
   * @param[in] The position the symlink should point to
   */
  int (*CreateSymlink)(void *ctx, const char *src, const char *dest);

  /**
   * Method which executes a garbage collection on the destination file system.
   * This will remove all no longer linked content adressed files
   */
  int (*GarbageCollection)(void *ctx);
};

#endif  // CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_
