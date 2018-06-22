/**
 * This file is part of the CernVM File System.
 */
#ifndef CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_
#define CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_

#include "hash.h"
#include "pointer.h"
#include "shortstring.h"

/**
 * Interface to be implemented for file systems so FsObjects work with them
 */
class FsObjectImplementor {
 public:
  /**
   * Method which returns the identifier (name) of the object
   * 
   * @returns The identifier of the object
   */
  virtual NameString DoGetIdentifier() = 0;

  /**
   * Method which returns the path of the object relative to the root of the
   * desired destination.
   * 
   * @returns The path of the object
   */
  virtual PathString DoGetPath() = 0;

  /**
   * Method which returns an iterator over the subdirectory described by this
   * object
   * 
   * @params[out] iterator Where the iterator should be saved
   */
  virtual void DoGetIterator(FsIterator *iterator) = 0;

  /**
   * Method which returns the content hash of the file described by this object
   * 
   * @returns The content hash of the file
   */
  virtual shash::Any DoGetHash() = 0;

  /**
   * Method which returns the path of the symlink described by this object
   * 
   * @returns THe path of the symlink
   */
  virtual PathString DoGetDestination() = 0;
};

/**
 * Object which represents an object in a file system.
 * This can either be a file, a symlink or a directory.
 */
class FsObject {
 public:
  ~FsObject() {
    delete fs_object_implementor_;
  }

  /**
   * The different possible types of objects
   */ 
  enum FsObjectType {
    FS_FILE = 1,
    FS_SYMLINK,
    FS_DIRECTORY
  };

  /**
   * Method which returns the identifier (name) of the object
   * 
   * @returns The identifier of the object
   */
  NameString GetIdentifier() {
    return fs_object_implementor_->DoGetIdentifier();
  }

  /**
   * Method which returns the path of the object relative to the root of the
   * desired destination.
   * 
   * @returns The path of the object
   */
  PathString GetPath() {
    return fs_object_implementor_->DoGetPath();
  }

  /**
   * Method which returns the type of the object.
   * 
   * @returns The type
   */
  virtual FsObjectType GetType() = 0;

 protected:
  explicit FsObject(FsObjectImplementor *fs_object_implementor_param) {
    fs_object_implementor_ = fs_object_implementor_param;
  }

  FsObjectImplementor *fs_object_implementor_;
};

class FsDir : FsObject {
 public:
  explicit FsDir(FsObjectImplementor *fs_object_implementor_param):
            FsObject(fs_object_implementor_param) { }
  /**
   * Method which returns an iterator over the subdirectory described by this
   * object
   * 
   * @params[out] iterator Where the iterator should be saved
   */
  void GetIterator(FsIterator *iterator) {
    fs_object_implementor_->DoGetIterator(iterator);
  }

  FsObjectType GetType() {
    return FS_DIRECTORY;
  }
};

class FsFile : FsObject {
 public:
  explicit FsFile(FsObjectImplementor *fs_object_implementor_param):
            FsObject(fs_object_implementor_param) { }

  /**
   * Method which returns the content hash of the file described by this object
   * 
   * @returns The content hash of the file
   */
  shash::Any GetHash() {
    return fs_object_implementor_->DoGetHash();
  }

  FsObjectType GetType() {
    return FS_FILE;
  }
};

class FsSymlink : FsObject {
 public:
  explicit FsSymlink(FsObjectImplementor *fs_object_implementor_param):
            FsObject(fs_object_implementor_param) { }
  /**
   * Method which returns the path of the symlink described by this object
   * 
   * @returns THe path of the symlink
   */
  PathString GetDestination() {
    return fs_object_implementor_->DoGetDestination();
  }

  FsObjectType GetType() {
    return FS_SYMLINK;
  }
};

/**
 * Iterator which allows iteration over the contents of a directory
 */
class FsIterator {
 public:
  /**
   * Method which retrieves the current iterator element.
   * If no elements are left NULL is returned.
   * The first invocation after object construction returns the first element.
   * Contrary to other iterators this call is idempotent!
   * 
   * @returns A pointer to the current File System object of the iterator
   */
  virtual FsObject *GetCurrent() = 0;

  /**
   * Method which checks if there are any elements left
   * 
   * @returns True if there is an element, false if not
   */
  virtual bool HasCurrent() = 0;

  /**
   * Method which sets the current element (retrievable by GetCurrent())
   * to the next one.
   * 
   * Silently fails if no more elements are available GetGurrent will then
   * return NULL
   */
  virtual void Step() = 0;
};

/**
 * Interface which represents functionality for a destination file system
 */
class DestinationFsInterface {
 public:
  /**
   * Method which returns an iterator over all files, symlinks and directories
   * in the root destination directory
   * 
   * @params[out] iterator Where the iterator should be saved
   */
  virtual void GetRootIterator(FsIterator* iter) = 0;

  /**
   * Method which checks whether the file described by the given content hash
   * exists in the destination file system
   * 
   * This should always be realised by a file system lookup since the all files
   * should be hardlinked once by their content hash.
   * 
   * @param[in] hash The content hash of the file that should searched
   * @returns True if file was found, false if not
   * 
   */
  virtual bool HasFile(const shash::Any &hash) = 0;

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
  virtual int Link(const PathString &path, const shash::Any &hash) = 0;

  /**
   * Method removes the hardlink at the given path
   * 
   * Error if:
   * - unlink not successful
   * GetRootIterator
   * @param[in] path The path which should be removed
   */
  virtual int Unlink(const PathString &path) = 0;

  /**
   * Method which will create the given directory
   * 
   * @param[in] path The path to the directory that should be created
   */
  virtual int CreateDirectory(const PathString &path) = 0;

  /**
   * Method which removes the given directory
   * 
   * @param[in] path The path to the directory that should be removed
   */
  virtual int RemoveDirectory(const PathString &path) = 0;

  /**
   * Method which creates a copy of the given file on the destination file
   * system.
   * 
   * @param[in] fd A file descriptor to use for reading the file
   * @param[in] hash A hash by which the file can be identified (content hash)
   */
  virtual int CopyFile(const FILE *fd, const shash::Any &hash) = 0;

  /**
   * Method which creates a symlink at src which points to dest
   * 
   * @param[in] The position at which the symlink should be saved
   * @param[in] The position the symlink should point to
   */
  virtual int CreateSymlink(const PathString &src, const PathString &dest) = 0;

  /**
   * Method which executes a garbage collection on the destination file system.
   * This will remove all no longer linked content adressed files
   */
  virtual int GarbageCollection() = 0;
};

#endif  // CVMFS_EXPORT_PLUGIN_FS_TRAVERSAL_INTERFACE_H_
