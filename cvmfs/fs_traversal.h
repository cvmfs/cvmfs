/**
 * This file is part of the CernVM File System.
 *
 * It provides a file system traversal framework to abstract the traversal
 * of directories.
 */

#ifndef CVMFS_FS_TRAVERSAL_H_
#define CVMFS_FS_TRAVERSAL_H_

#include <errno.h>

#include <cassert>
#include <cstdlib>

#include <set>
#include <string>

#include "logging.h"
#include "platform.h"
#include "util.h"

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif

/**
 * @brief A simple recursion engine to abstract the recursion of directories.
 * It provides several callback hooks to instrument and control the recursion.
 * Hooks will be called on the provided delegate object of type T
 *
 * Callbacks are called for every directory entry found by the recursion engine.
 * The recursion can be influenced by return values of these callbacks.
 */
template <class T>
class FileSystemTraversal {
 public:
  typedef void (T::*VoidCallback)(const std::string &relative_path,
                                  const std::string &dir_name);
  typedef bool (T::*BoolCallback)(const std::string &relative_path,
                                  const std::string &dir_name);


  VoidCallback fn_enter_dir;
  VoidCallback fn_leave_dir;
  VoidCallback fn_new_file;
  VoidCallback fn_new_symlink;
  VoidCallback fn_new_socket;
  VoidCallback fn_new_block_dev;
  VoidCallback fn_new_character_dev;
  VoidCallback fn_new_fifo;

  /**
   * Optional callback for all files during recursion to decide
   * whether to completely ignore the file.  If this callback returns
   * true then the file will not be processed (this is a replacement
   * for the ignored_files set, and it allows to ignore based on names
   * or something else). If the function is not specified, no files
   * will be ignored (except for "." and "..").
   */
  BoolCallback fn_ignore_file;

  /**
   * Callback if a directory was found.  Depending on the response of
   * the callback, the recursion will continue in the found directory/
   * If this callback is not specified, it will recurse by default.
   */
  BoolCallback fn_new_dir_prefix;

  /**
   * Callback for a found directory after it was already recursed
   * e.g. for deletion of directories: first delete content,
   * then the directory itself
   */
  VoidCallback fn_new_dir_postfix;


  /**
   * Create a new recursion engine
   * @param delegate The object that will receive the callbacks
   * @param relative_to_directory The DirEntries will be created relative
   *        to this directory
   * @param recurse Should the traversal engine recurse? (if not,
   *        it just traverses the given directory)
   */
  FileSystemTraversal(T *delegate,
                      const std::string &relative_to_directory,
                      const bool recurse) :
    fn_enter_dir(NULL),
    fn_leave_dir(NULL),
    fn_new_file(NULL),
    fn_new_symlink(NULL),
    fn_new_socket(NULL),
    fn_new_block_dev(NULL),
    fn_new_character_dev(NULL),
    fn_new_fifo(NULL),
    fn_ignore_file(NULL),
    fn_new_dir_prefix(NULL),
    fn_new_dir_postfix(NULL),
    delegate_(delegate),
    relative_to_directory_(relative_to_directory),
    recurse_(recurse)
  {
    Init();
  }

  /**
   * Start the recursion.
   * @param dir_path The directory to start the recursion at
   */
  void Recurse(const std::string &dir_path) const {
    assert(fn_enter_dir != NULL ||
           fn_leave_dir != NULL ||
           fn_new_file != NULL ||
           fn_new_symlink != NULL ||
           fn_new_dir_prefix != NULL ||
           fn_new_block_dev != NULL ||
           fn_new_character_dev != NULL ||
           fn_new_fifo != NULL ||
           fn_new_socket != NULL);

    assert(relative_to_directory_.length() == 0 ||
           dir_path.substr(0, relative_to_directory_.length()) ==
             relative_to_directory_);

    DoRecursion(dir_path, "");
  }

 private:
  // The delegate all hooks are called on
  T *delegate_;

  /** dir_path in callbacks will be relative to this directory */
  std::string relative_to_directory_;
  bool recurse_;


  void Init() {
  }

  void DoRecursion(const std::string &parent_path, const std::string &dir_name)
    const
  {
    DIR *dip;
    platform_dirent64 *dit;
    const std::string path = parent_path + ((!dir_name.empty()) ?
                                           ("/" + dir_name) : "");

    // Change into directory and notify the user
    LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "entering %s (%s -- %s)",
             path.c_str(), parent_path.c_str(), dir_name.c_str());
    dip = opendir(path.c_str());
    if (!dip) {
      LogCvmfs(kLogFsTraversal, kLogStderr, "Failed to open %s (%d).\n"
               "Please check directory permissions.",
               path.c_str(), errno);
      abort();
    }
    Notify(fn_enter_dir, parent_path, dir_name);

    // Walk through the open directory notifying the user about contents
    while ((dit = platform_readdir(dip)) != NULL) {
      // Check if file should be ignored
      if (std::string(dit->d_name) == "." || std::string(dit->d_name) == "..") {
        continue;
      } else if (fn_ignore_file != NULL) {
        if (Notify(fn_ignore_file, path, dit->d_name)) {
          LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "ignoring %s/%s",
                   path.c_str(), dit->d_name);
          continue;
        }
      } else {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg,
                 "not ignoring %s/%s (fn_ignore_file not set)",
                 path.c_str(), dit->d_name);
      }

      // Notify user about found directory entry
      platform_stat64 info;
      int retval = platform_lstat((path + "/" + dit->d_name).c_str(), &info);
      if (retval != 0) {
        LogCvmfs(kLogFsTraversal, kLogStderr, "failed to lstat '%s' errno: %d",
                 (path + "/" + dit->d_name).c_str(), errno);
        abort();
      }
      if (S_ISDIR(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing directory %s/%s",
                 path.c_str(), dit->d_name);
        if (Notify(fn_new_dir_prefix, path, dit->d_name) && recurse_) {
          DoRecursion(path, dit->d_name);
        }
        Notify(fn_new_dir_postfix, path, dit->d_name);
      } else if (S_ISREG(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing regular file %s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_file, path, dit->d_name);
      } else if (S_ISLNK(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing symlink %s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_symlink, path, dit->d_name);
      } else if (S_ISSOCK(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing socket %s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_socket, path, dit->d_name);
      } else if (S_ISBLK(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing block-device %s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_block_dev, path, dit->d_name);
      } else if (S_ISCHR(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing character-device "
                                                  "%s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_character_dev, path, dit->d_name);
      } else if (S_ISFIFO(info.st_mode)) {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "passing FIFO %s/%s",
                 path.c_str(), dit->d_name);
        Notify(fn_new_fifo, path, dit->d_name);
      } else {
        LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "unknown file type %s/%s",
                 path.c_str(), dit->d_name);
      }
    }

    // Close directory and notify user
    closedir(dip);
    LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "leaving %s", path.c_str());
    Notify(fn_leave_dir, parent_path, dir_name);
  }

  inline bool Notify(const BoolCallback callback,
                     const std::string &parent_path,
                     const std::string &entry_name) const
  {
    return (callback == NULL) ? true :
      (delegate_->*callback)(GetRelativePath(parent_path),
                             entry_name);
  }

  inline void Notify(const VoidCallback callback,
                     const std::string &parent_path,
                     const std::string &entry_name) const
  {
    if (callback != NULL) {
      (delegate_->*callback)(GetRelativePath(parent_path),
                             entry_name);
    }
  }

  std::string GetRelativePath(const std::string &absolute_path) const {
    const unsigned int rel_dir_len = relative_to_directory_.length();
    if (rel_dir_len >= absolute_path.length()) {
      return "";
    } else if (rel_dir_len > 1) {
      return absolute_path.substr(rel_dir_len + 1);
    } else if (rel_dir_len == 0) {
      return absolute_path;
    } else if (relative_to_directory_ == "/") {
      return absolute_path.substr(1);
    } else {
      return "";
    }
  }
};  // FileSystemTraversal

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_FS_TRAVERSAL_H_
