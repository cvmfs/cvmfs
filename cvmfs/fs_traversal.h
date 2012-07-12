/**
 * This file is part of the CernVM File System.
 *
 * It provides a file system traversal framework to abstract the traversal
 * of directories.
 */

#ifndef CVMFS_FS_TRAVERSAL_H_
#define CVMFS_FS_TRAVERSAL_H_

#include <cassert>

#include <string>
#include <set>

#include "platform.h"
#include "logging.h"
#include "util.h"

namespace publish {

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
	 * @param ignored_files A list of files which the delegate does not care about
   *                     (no callback calls or recursion for them)
	 * @param recurse Should the traversal engine recurse? (if not,
   *        it just traverses the given directory)
	 */
	FileSystemTraversal(T *delegate,
	                const std::string &relative_to_directory,
	                const bool recurse,
	                const std::set<std::string> &ignored_files)
  {
    delegate_ = delegate;
    relative_to_directory_ = relative_to_directory;
    recurse_ = recurse;
    ignored_files_ = ignored_files;
    fn_enter_dir = NULL;
    fn_leave_dir = NULL;
    fn_new_file = NULL;
    fn_new_symlink = NULL;
    fn_new_dir_prefix = NULL;
    fn_new_dir_postfix = NULL;
    Init();
  }

  /**
   * start the recursion
   * @param dir_path The directory to start the recursion at
   */
  void Recurse(const std::string &dir_path) const {
    assert(fn_enter_dir != NULL ||
           fn_leave_dir != NULL ||
           fn_new_file != NULL ||
           fn_new_symlink != NULL ||
           fn_new_dir_prefix != NULL);

    const std::string parent_path = GetParentPath(dir_path);
    const std::string dir_name = GetFileName(dir_path);

    assert(relative_to_directory_.length() == 0 ||
           dir_path.substr(0, relative_to_directory_.length()) ==
             relative_to_directory_);

    DoRecursion(parent_path, dir_name);
  }

 private:
	// The delegate all hooks are called on
	T *delegate_;

	/** dir_path in callbacks will be relative to this directory */
	std::string relative_to_directory_;
	bool recurse_;

	// If one of these files are found somewhere they are completely ignored
	std::set<std::string> ignored_files_;


  void Init() {
    ignored_files_.insert(".");
    ignored_files_.insert("..");
  }

  void DoRecursion(const std::string &parent_path, const std::string &dir_name)
    const
	{
    DIR *dip;
    platform_dirent64 *dit;
    const std::string path = parent_path + "/" + dir_name;

    // Change into directory and notify the user
    dip = opendir(path.c_str());
    assert(dip);
    LogCvmfs(kLogFsTraversal, kLogVerboseMsg, "entering %s", path.c_str());
    Notify(fn_enter_dir, parent_path, dir_name);

    // Walk through the open directory notifying the about contents
    while ((dit = platform_readdir(dip)) != NULL) {
      // Check if filename is included in the ignored files list
      if (ignored_files_.find(dit->d_name) != ignored_files_.end())
        continue;

      // Notify user about found directory entry
      platform_stat64 info;
      int retval = platform_lstat((path + "/" + dit->d_name).c_str(), &info);
      assert(retval == 0);
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

	std::string GetRelativePath(const std::string &absolute_path) const  {
    const unsigned int rel_dir_len = relative_to_directory_.length();
    if (rel_dir_len >= absolute_path.length()) { return ""; }
	  else if (rel_dir_len > 1) { return absolute_path.substr(rel_dir_len + 1); }
	  else if (rel_dir_len == 0){ return absolute_path; }
	  else if (relative_to_directory_ == "/") { return absolute_path.substr(1); }
    else return "";
  }
};  // FileSystemTraversal

}  // namespace publish

#endif  // CVMFS_FS_TRAVERSAL_H_
